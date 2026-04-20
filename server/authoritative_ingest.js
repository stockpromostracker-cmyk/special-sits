#!/usr/bin/env node
// Authoritative-source ingest pipeline (regulator-first rebuild).
//
// Runs all primary (regulator) and secondary (aggregator) sources, upserts
// each deal into the `deals` table keyed by external_key, then matches news
// from raw_items to the resulting deals as news_items.
//
// Order:
//   1. US regulators  (SEC EDGAR: 10-12B, S-1, 424B4, DEFM14A)
//   2. UK regulator   (LSE RNS → Investegate fallback)
//   3. Nordic         (MFN RSS with MAR classification)
//   4. Aggregators    (stockanalysis.com spin-offs + IPOs)
//   5. Enrichment     (market data refresh for new deals)
//   6. News linkage   (attach matching raw_items as news_items)
//
// On first authoritative run we optionally wipe pre-existing news-tier deals.

require('dotenv').config();
const { query, migrate, parseJson, serializeJson, USE_PG } = require('./db');
const sec          = require('./sources/sec');
const lse          = require('./sources/lse_rns');
const nordic       = require('./sources/nordic');
const amfBdif      = require('./sources/amf_bdif');
const swissTob     = require('./sources/swiss_tob');
const euNewsMA     = require('./sources/eu_news_ma');
const stockanalysis = require('./sources/stockanalysis');
const knownEvents  = require('./sources/known_events');
const { refreshDeal } = require('./market_data');

// ---- Deal suppression list ----------------------------------------------
// When an automated source (SEC, aggregators) would re-ingest a deal that
// we've already curated correctly in known_events.json (e.g. Magnum Ice
// Cream re-appearing as an IPO via its 424B4 resale filing), skip it here.
//
// Keyed on (primary_ticker, deal_type). Case-insensitive ticker match. If
// the key matches and the source is NOT 'company_press_release' (i.e. a
// known_events entry), the deal is dropped before upsert.
const SUPPRESS_AUTO_INGEST = [
  // Magnum is a Unilever demerger, not an IPO. The SEC 424B4 is a resale
  // prospectus for Unilever's ~19.9% retained stake, not a primary listing.
  { ticker: 'MICC', deal_type: 'ipo', reason: 'Magnum is a spin_off, not an IPO; curated in known_events' },
  // Titan America is a partial carve-out IPO (parent kept 87%), properly
  // classified in known_events. SEC may still index it as generic IPO.
  // We don't suppress it because the known_events entry is also deal_type=ipo.
];

function shouldSuppress(d) {
  if (!d.primary_ticker) return false;
  // Only suppress auto-sources; always allow curated company_press_release.
  if (d.primary_source === 'company_press_release') return false;
  // Strip exchange prefix for comparison (NYSE:MICC → MICC)
  const bareTicker = String(d.primary_ticker).split(':').pop().toUpperCase();
  for (const rule of SUPPRESS_AUTO_INGEST) {
    if (rule.ticker.toUpperCase() === bareTicker && rule.deal_type === d.deal_type) {
      console.log(`[auth-ingest] suppressing ${d.primary_ticker}/${d.deal_type}: ${rule.reason}`);
      return true;
    }
  }
  return false;
}

// ---- Upsert helper -------------------------------------------------------
// Deals are keyed on external_key. If a matching external_key already exists
// we update key fields; otherwise we insert. External keys are of the form
// "sec:<accession>", "lse_rns:<url>", "mfn:<url>", "sa_spin:...", "sa_ipo:..."

async function upsertDeal(d) {
  // Normalise & compute derived fields
  if (d.completed_date) {
    const diff = Math.round((Date.now() - new Date(d.completed_date)) / 86400000);
    d.days_since_event = diff >= 0 ? diff : null;
  }
  if (d.key_dates && typeof d.key_dates === 'object') {
    // Compute days_to_event from the nearest future date in key_dates
    const futures = Object.values(d.key_dates)
      .filter(Boolean)
      .map(v => new Date(v))
      .filter(v => !isNaN(v) && v > new Date())
      .sort((a, b) => a - b);
    if (futures.length) {
      d.days_to_event = Math.round((futures[0] - Date.now()) / 86400000);
    }
  }

  // Look up existing by external_key via a marker column. We use source_filing_url
  // as a secondary dedupe key (primary is external_key stored in source_ids JSON).
  const existing = await query(
    `SELECT id FROM deals WHERE source_filing_url = $1 LIMIT 1`,
    [d.source_filing_url || '']
  );

  const fields = {
    deal_type: d.deal_type || 'other',
    status: d.status || 'announced',
    region: d.region,
    country: d.country,
    primary_ticker: d.primary_ticker || null,
    headline: d.headline,
    summary: d.summary || null,
    acquirer_name: d.acquirer_name || null,
    acquirer_ticker: d.acquirer_ticker || null,
    target_name: d.target_name || null,
    target_ticker: d.target_ticker || null,
    parent_name: d.parent_name || null,
    parent_ticker: d.parent_ticker || null,
    spinco_name: d.spinco_name || null,
    spinco_ticker: d.spinco_ticker || null,
    announce_date: d.announce_date || null,
    expected_close_date: d.expected_close_date || null,
    record_date: d.record_date || null,
    ex_date: d.ex_date || null,
    thesis: d.thesis || null,
    ipo_price: d.ipo_price ?? null,
    announce_price: d.announce_price ?? null,
    current_price: d.current_price ?? null,
    // Authoritative layer
    event_type: d.event_type,
    data_source_tier: d.data_source_tier,
    primary_source: d.primary_source,
    source_filing_url: d.source_filing_url,
    source_cik: d.source_cik || null,
    confidence: d.confidence ?? null,
    filing_date: d.filing_date || null,
    completed_date: d.completed_date || null,
    days_to_event: d.days_to_event ?? null,
    days_since_event: d.days_since_event ?? null,
    key_dates: serializeJson(d.key_dates || null),
    // Merger arb layer
    offer_price: d.offer_price ?? null,
    consideration: d.consideration || null,
    consideration_type: d.consideration_type || null,
    consideration_cash: d.consideration_cash ?? null,
    consideration_stock_ratio: d.consideration_stock_ratio ?? null,
    acquirer_proxy_ticker: d.acquirer_proxy_ticker || null,
    announce_date_source: d.announce_date_source || null,
    is_spac: d.is_spac ? 1 : 0,
    deal_value_usd: d.deal_value_usd ?? null,
  };

  if (existing.length) {
    // Update — but only OVERWRITE if new source is higher-tier than existing
    const currentRow = await query(`SELECT data_source_tier, confidence FROM deals WHERE id = $1`, [existing[0].id]);
    const currentTierRank = tierRank(currentRow[0]?.data_source_tier);
    const newTierRank = tierRank(d.data_source_tier);
    // Always COALESCE-update (fill blanks), but only overwrite if new tier >= old
    const overwrite = newTierRank >= currentTierRank;
    const setClauses = Object.keys(fields).map((k, i) => {
      if (overwrite) return `${k} = COALESCE($${i + 1}, ${k})`;
      return `${k} = COALESCE(${k}, $${i + 1})`;
    });
    setClauses.push(`updated_at = ${USE_PG ? 'NOW()' : "datetime('now')"}`);
    const sql = `UPDATE deals SET ${setClauses.join(', ')} WHERE id = $${Object.keys(fields).length + 1}`;
    await query(sql, [...Object.values(fields), existing[0].id]);
    return { id: existing[0].id, action: 'updated' };
  } else {
    const cols = Object.keys(fields);
    const placeholders = cols.map((_, i) => `$${i + 1}`).join(', ');
    const sql = `INSERT INTO deals (${cols.join(', ')}) VALUES (${placeholders}) RETURNING id`;
    const res = await query(sql, Object.values(fields));
    return { id: res[0]?.id, action: 'inserted' };
  }
}

function tierRank(tier) {
  switch (tier) {
    case 'official':   return 3;
    case 'aggregator': return 2;
    case 'news':       return 1;
    default:           return 0;
  }
}

// ---- News linkage --------------------------------------------------------

async function linkNewsToDeals() {
  // Walk recent raw_items and for each try to match a deal by:
  //   1. primary_ticker exact match (case-insensitive)
  //   2. issuer_name fuzzy match (contains)
  // If we find a match, insert a row into news_items. Idempotent via
  // uq_news_items_raw_deal.
  const rawItems = await query(
    `SELECT id, source, url, headline, body, published_at
     FROM raw_items
     WHERE fetched_at > ${USE_PG ? "NOW() - INTERVAL '60 days'" : "datetime('now','-60 days')"}
     ORDER BY fetched_at DESC LIMIT 2000`
  );
  const deals = await query(`SELECT id, primary_ticker, target_name, spinco_name, parent_name FROM deals WHERE primary_ticker IS NOT NULL`);

  let attached = 0;
  for (const item of rawItems) {
    const hay = `${item.headline || ''} ${item.body || ''}`;
    for (const d of deals) {
      let matched = null;
      // Ticker match — must be a standalone token
      if (d.primary_ticker) {
        const bare = d.primary_ticker.replace(/^[A-Z]+:/, '');
        const re = new RegExp(`(?<![A-Z0-9])${bare.replace(/[.\-]/g, '\\$&')}(?![A-Z0-9])`);
        if (re.test(hay)) matched = { kind: 'ticker', ticker: d.primary_ticker };
      }
      // Name match — fairly long tokens only to avoid false positives
      if (!matched) {
        for (const name of [d.target_name, d.spinco_name, d.parent_name].filter(Boolean)) {
          if (name.length >= 6 && hay.toLowerCase().includes(name.toLowerCase())) {
            matched = { kind: 'issuer_name', ticker: name };
            break;
          }
        }
      }
      if (!matched) continue;

      try {
        await query(
          `INSERT INTO news_items (deal_id, raw_item_id, source, url, headline, summary, published_at, matched_ticker, match_kind)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           ON CONFLICT DO NOTHING`,
          [d.id, item.id, item.source, item.url, item.headline, (item.body || '').slice(0, 500),
           item.published_at, matched.ticker, matched.kind]
        );
        attached++;
      } catch (e) {
        if (!/duplicate|UNIQUE/i.test(e.message)) {
          console.warn('[news-link] insert failed', e.message);
        }
      }
    }
  }
  return { raw_scanned: rawItems.length, news_items_attached: attached };
}

// ---- Full run ------------------------------------------------------------

async function runAuthoritativeCycle({ wipeExisting = false, skipMarket = false } = {}) {
  await migrate();

  if (wipeExisting) {
    console.log('[auth-ingest] wiping existing deals…');
    // Delete news_items first to respect FK logic (we don't actually have FKs but be safe)
    try { await query('DELETE FROM news_items'); } catch (e) { /* table may not yet exist */ }
    await query('DELETE FROM deals');
    console.log('[auth-ingest] wiped.');
  }

  const results = {
    sec: null, lse: null, nordic: null,
    amf: null, swiss: null, eu_news: null,
    sa: null, known: null,
    upserts: { inserted: 0, updated: 0 },
    news: null,
    market: { refreshed: 0 },
  };

  // ---- 1. SEC ----
  console.log('[auth-ingest] fetching SEC…');
  try {
    results.sec = await sec.fetchAll();
    console.log(`[auth-ingest] SEC: ${results.sec.deals_with_ticker.length} with-ticker / ${results.sec.deals.length} total`);
    for (const d of results.sec.deals_with_ticker) {
      if (shouldSuppress(d)) continue;
      const r = await upsertDeal(d);
      results.upserts[r.action === 'inserted' ? 'inserted' : 'updated']++;
    }
  } catch (e) { console.error('[auth-ingest] SEC failed:', e.message); }

  // ---- 2. LSE ----
  console.log('[auth-ingest] fetching LSE/Investegate…');
  try {
    results.lse = await lse.fetchAll({ days: 180 });
    console.log(`[auth-ingest] LSE via ${results.lse.source}: ${results.lse.count} deals`);
    for (const d of results.lse.deals) {
      if (!d.primary_ticker) continue;
      if (shouldSuppress(d)) continue;
      const r = await upsertDeal(d);
      results.upserts[r.action === 'inserted' ? 'inserted' : 'updated']++;
    }
  } catch (e) { console.error('[auth-ingest] LSE failed:', e.message); }

  // ---- 3. Nordic ----
  console.log('[auth-ingest] fetching Nordic MFN…');
  try {
    results.nordic = await nordic.fetchAll();
    console.log(`[auth-ingest] Nordic: ${results.nordic.count} of ${results.nordic.items_scanned} scanned`);
    for (const d of results.nordic.deals) {
      if (shouldSuppress(d)) continue;
      const r = await upsertDeal(d);
      results.upserts[r.action === 'inserted' ? 'inserted' : 'updated']++;
    }
  } catch (e) { console.error('[auth-ingest] Nordic failed:', e.message); }

  // ---- 3b. AMF BDIF (France — regulator JSON API) ----
  console.log('[auth-ingest] fetching AMF BDIF…');
  try {
    results.amf = await amfBdif.fetchAll();
    console.log(`[auth-ingest] AMF: ${results.amf.count} of ${results.amf.items_scanned} scanned`);
    for (const d of results.amf.deals) {
      if (shouldSuppress(d)) continue;
      const r = await upsertDeal(d);
      results.upserts[r.action === 'inserted' ? 'inserted' : 'updated']++;
    }
  } catch (e) { console.error('[auth-ingest] AMF failed:', e.message); }

  // ---- 3c. Swiss Takeover Board (CH — regulator HTML) ----
  console.log('[auth-ingest] fetching Swiss TOB…');
  try {
    results.swiss = await swissTob.fetchAll();
    console.log(`[auth-ingest] Swiss TOB: ${results.swiss.count} of ${results.swiss.items_scanned} scanned`);
    for (const d of results.swiss.deals) {
      if (shouldSuppress(d)) continue;
      const r = await upsertDeal(d);
      results.upserts[r.action === 'inserted' ? 'inserted' : 'updated']++;
    }
  } catch (e) { console.error('[auth-ingest] Swiss TOB failed:', e.message); }

  // ---- 3d. EU News M&A (DE/NL/IT/ES/BE/AT — Google News RSS fallback) ----
  console.log('[auth-ingest] fetching EU News M&A…');
  try {
    results.eu_news = await euNewsMA.fetchAll();
    console.log(`[auth-ingest] EU News: ${results.eu_news.count} of ${results.eu_news.items_scanned} scanned, ${results.eu_news.errors} errors`);
    for (const d of results.eu_news.deals) {
      if (shouldSuppress(d)) continue;
      const r = await upsertDeal(d);
      results.upserts[r.action === 'inserted' ? 'inserted' : 'updated']++;
    }
  } catch (e) { console.error('[auth-ingest] EU News failed:', e.message); }

  // ---- 4. StockAnalysis aggregator ----
  console.log('[auth-ingest] fetching StockAnalysis aggregator…');
  try {
    results.sa = await stockanalysis.fetchAll();
    console.log(`[auth-ingest] StockAnalysis: ${results.sa.spinoffs} spinoffs + ${results.sa.ipos} IPOs`);
    for (const d of results.sa.deals) {
      if (shouldSuppress(d)) continue;
      const r = await upsertDeal(d);
      results.upserts[r.action === 'inserted' ? 'inserted' : 'updated']++;
    }
  } catch (e) { console.error('[auth-ingest] StockAnalysis failed:', e.message); }

  // ---- 4b. Known-events seed (curated famous spin-offs outside auto windows) ----
  console.log('[auth-ingest] loading known-events seed…');
  try {
    results.known = await knownEvents.fetchAll();
    console.log(`[auth-ingest] Known events: ${results.known.count} curated deals`);
    for (const d of results.known.deals) {
      const r = await upsertDeal(d);
      results.upserts[r.action === 'inserted' ? 'inserted' : 'updated']++;
    }
  } catch (e) { console.error('[auth-ingest] known events failed:', e.message); }

  // ---- 5. Market-data refresh (price + mcap) ----
  // Resilient: throttled inside fetchQuote, chunked, errors never propagate.
  if (!skipMarket) {
    console.log('[auth-ingest] refreshing market data for new deals…');
    const newDeals = await query(
      `SELECT * FROM deals
       WHERE primary_ticker IS NOT NULL
         AND (market_refreshed_at IS NULL
              OR market_refreshed_at < ${USE_PG ? "NOW() - INTERVAL '3 days'" : "datetime('now','-3 days')"})
       ORDER BY id DESC LIMIT 150`
    );
    let errors = 0;
    const CHUNK = 25;
    for (let i = 0; i < newDeals.length; i += CHUNK) {
      const chunk = newDeals.slice(i, i + CHUNK);
      for (const d of chunk) {
        try { await refreshDeal(d); results.market.refreshed++; }
        catch (e) { errors++; /* non-fatal */ }
      }
      await new Promise(r => setTimeout(r, 500));
    }
    results.market.errors = errors;
    console.log(`[auth-ingest] market: refreshed ${results.market.refreshed}, errors ${errors}`);
  }

  // ---- 6. News linkage ----
  console.log('[auth-ingest] linking news items…');
  try {
    results.news = await linkNewsToDeals();
    console.log(`[auth-ingest] news: scanned ${results.news.raw_scanned}, attached ${results.news.news_items_attached}`);
  } catch (e) { console.error('[auth-ingest] news-link failed:', e.message); }

  return results;
}

if (require.main === module) {
  const wipe = process.argv.includes('--wipe');
  const skipMarket = process.argv.includes('--no-market');
  runAuthoritativeCycle({ wipeExisting: wipe, skipMarket })
    .then(r => { console.log('[auth-ingest] done', JSON.stringify(r, null, 2)); process.exit(0); })
    .catch(e => { console.error('[auth-ingest] FATAL', e); process.exit(1); });
}

module.exports = { runAuthoritativeCycle, upsertDeal, linkNewsToDeals };
