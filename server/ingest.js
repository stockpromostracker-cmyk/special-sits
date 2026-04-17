#!/usr/bin/env node
// Daily ingestion cycle:
//  1. Pull all feeds into raw_items
//  2. Classify every 'new' raw_item with Gemini
//  3. Promote classified special situations into the deals table (or update existing)

require('dotenv').config();
const { query, migrate, parseJson, serializeJson } = require('./db');
const { fetchAll } = require('./feeds');
const { classify } = require('./classifier');
const { prefilter, normalizeHeadline } = require('./prefilter');

const MAX_CLASSIFY_PER_RUN = parseInt(process.env.MAX_CLASSIFY_PER_RUN || '200', 10);

// Source priority — classify high-signal sources first so that under a tight
// per-run cap we still get the best deals. Lower number = higher priority.
const SOURCE_PRIORITY = {
  'email':              0,
  'sec_edgar':          1,
  'prnewswire_ma':      2,
  'businesswire_ma':    2,
  // Google News sub-sources
  'google_news:ma_global':    3,
  'google_news:ma_cash':      3,
  'google_news:uk_rule27':    3,
  'google_news:eu_ma':        3,
  'google_news:take_private': 3,
  'google_news:spinoffs':     3,
  'google_news:ipo_filing':   4,
  'google_news:ipo_eu':       4,
  'google_news:tender':       4,
  'google_news:spac':         4,
  'google_news:activist':     5,
  'google_news:rights':       5,
  'google_news:nordic':       5,
};
function priorityOf(source) {
  if (SOURCE_PRIORITY[source] != null) return SOURCE_PRIORITY[source];
  if (source?.startsWith('google_news:')) return 5;
  return 9;
}

async function classifyPending() {
  // Pull a generous candidate pool — we'll prefilter/dedupe in memory, then
  // classify only what survives, up to MAX_CLASSIFY_PER_RUN.
  const poolSize = MAX_CLASSIFY_PER_RUN * 4;
  const rows = await query(
    `SELECT id, source, headline, body FROM raw_items WHERE status = $1 ORDER BY id DESC LIMIT $2`,
    ['new', poolSize]
  );
  console.log(`[classify] ${rows.length} pending in pool`);

  // Sort by source priority so high-signal items are classified first.
  rows.sort((a, b) => priorityOf(a.source) - priorityOf(b.source) || b.id - a.id);

  // Track normalized-headline hashes already seen to dedupe cross-source duplicates.
  // Seed from recent classified hits so we don't re-classify same story next day.
  const seen = new Set();
  const recentHits = await query(
    `SELECT headline FROM raw_items WHERE status IN ($1, $2) ORDER BY id DESC LIMIT 2000`,
    ['classified_hit', 'skipped_duplicate']
  );
  for (const r of recentHits) seen.add(normalizeHeadline(r.headline));

  let classified = 0, promoted = 0, skipped_prefilter = 0, skipped_dup = 0;

  for (const row of rows) {
    if (classified >= MAX_CLASSIFY_PER_RUN) break;

    // 1) Prefilter: keyword gate + SEC item-code gate
    const pre = prefilter({ source: row.source, headline: row.headline, body: row.body });
    if (!pre.pass) {
      await query(
        `UPDATE raw_items SET status = $1, classification = $2 WHERE id = $3`,
        ['skipped_prefilter', serializeJson({ is_special_situation: false, reason: pre.reason }), row.id]
      );
      skipped_prefilter++;
      continue;
    }

    // 2) Headline dedupe
    const norm = normalizeHeadline(row.headline);
    if (norm && seen.has(norm)) {
      await query(
        `UPDATE raw_items SET status = $1, classification = $2 WHERE id = $3`,
        ['skipped_duplicate', serializeJson({ is_special_situation: false, reason: 'duplicate headline' }), row.id]
      );
      skipped_dup++;
      continue;
    }
    if (norm) seen.add(norm);

    // 3) Actually classify via Gemini
    try {
      const result = await classify({
        headline: row.headline,
        body: row.body,
        source: row.source,
      });

      await query(
        `UPDATE raw_items SET status = $1, classification = $2 WHERE id = $3`,
        [result.is_special_situation ? 'classified_hit' : 'classified_miss',
         serializeJson(result), row.id]
      );
      classified++;

      if (result.is_special_situation) {
        await upsertDeal(result, row.id);
        promoted++;
      }
    } catch (e) {
      console.error('[classify item]', row.id, e.message);
      await query(`UPDATE raw_items SET status = $1 WHERE id = $2`, ['error', row.id]);
    }
  }
  console.log(`[classify] done: classified=${classified} promoted=${promoted} skipped_prefilter=${skipped_prefilter} skipped_dup=${skipped_dup}`);
  return { classified, promoted, skipped_prefilter, skipped_dup };
}

async function upsertDeal(deal, sourceRawId) {
  // Try to find an existing deal for the same primary ticker pair to merge sources.
  const key = deal.target_ticker || deal.spinco_ticker || deal.acquirer_ticker;
  let existing = null;
  if (key) {
    const rows = await query(
      `SELECT * FROM deals WHERE target_ticker = $1 OR spinco_ticker = $1 OR acquirer_ticker = $1 LIMIT 1`,
      [key]
    );
    existing = rows[0];
  }

  if (existing) {
    const prev = parseJson(existing.source_ids) || [];
    if (!prev.includes(sourceRawId)) prev.push(sourceRawId);
    await query(
      `UPDATE deals SET
         status = COALESCE($1, status),
         headline = $2,
         summary = $3,
         thesis = COALESCE($4, thesis),
         risks = COALESCE($5, risks),
         source_ids = $6,
         updated_at = ${process.env.DATABASE_URL ? 'NOW()' : "datetime('now')"}
       WHERE id = $7`,
      [deal.status, deal.headline, deal.summary, deal.thesis, deal.risks,
       serializeJson(prev), existing.id]
    );
    return existing.id;
  }

  const rows = await query(
    `INSERT INTO deals (
      deal_type, status, region, headline, summary, thesis, risks,
      acquirer_name, acquirer_ticker, target_name, target_ticker,
      parent_name, parent_ticker, spinco_name, spinco_ticker,
      deal_value_usd, consideration, offer_price,
      announce_date, expected_close_date, record_date, ex_date,
      source_ids
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
    RETURNING id`,
    [
      deal.deal_type, deal.status || 'announced', deal.region, deal.headline,
      deal.summary, deal.thesis, deal.risks,
      deal.acquirer_name, deal.acquirer_ticker, deal.target_name, deal.target_ticker,
      deal.parent_name, deal.parent_ticker, deal.spinco_name, deal.spinco_ticker,
      deal.deal_value_usd, deal.consideration, deal.offer_price,
      deal.announce_date, deal.expected_close_date, deal.record_date, deal.ex_date,
      serializeJson([sourceRawId]),
    ]
  );
  return rows[0]?.id;
}

async function runCycle() {
  await migrate();
  console.log('[ingest] fetching feeds…');
  const { fetched, inserted } = await fetchAll();
  console.log(`[ingest] fetched ${fetched} items, ${inserted} new`);

  const cls = await classifyPending();
  console.log(`[ingest] classified ${cls.classified}, promoted ${cls.promoted}, skipped_prefilter ${cls.skipped_prefilter}, skipped_dup ${cls.skipped_dup}`);
  return { fetched, inserted, ...cls };
}

if (require.main === module) {
  runCycle()
    .then(res => { console.log('[ingest] done', res); process.exit(0); })
    .catch(e => { console.error('[ingest] fatal', e); process.exit(1); });
}

module.exports = { runCycle, classifyPending, upsertDeal };
