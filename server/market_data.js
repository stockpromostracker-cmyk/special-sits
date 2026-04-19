// Market data fetcher using yahoo-finance2. Free, no API key required,
// covers global exchanges via the same .SUFFIX convention we use.
//
// Two entry points:
//   fetchQuote(yahooSymbol)   \u2014 one-shot fetch, returns { price, marketCap, currency, sector, industry, shortName }
//   refreshDeal(deal)         \u2014 populate primary_ticker + market_cap + current_price (+ announce_price on first fetch)

const { pickPrimaryTicker, parseTicker } = require('./tickers');
const { query } = require('./db');

// ---- Yahoo call throttle --------------------------------------------------
// Yahoo rate-limits aggressively when we blast 200+ symbols in a tight loop.
// A 300-450ms per-call gap keeps us well under the ~2 req/sec threshold
// they typically tolerate for unauthenticated clients.
let _lastYfCall = 0;
async function throttle(minMs = 300) {
  const wait = minMs - (Date.now() - _lastYfCall);
  if (wait > 0) await new Promise(r => setTimeout(r, wait));
  _lastYfCall = Date.now();
}

let yf = null;
function getYf() {
  if (yf) return yf;
  // yahoo-finance2 v3+ exposes a class \u2014 instantiate once.
  const YahooFinance = require('yahoo-finance2').default;
  yf = new YahooFinance({ suppressNotices: ['yahooSurvey', 'ripHistorical'] });
  return yf;
}

// Very rough USD conversion table \u2014 good enough for deal-size bucketing.
// Refreshed periodically from yahoo; falls back to these constants.
const FX_TO_USD_DEFAULT = {
  USD: 1,
  GBP: 1.27,
  GBX: 0.0127, // UK pence
  EUR: 1.07,
  CHF: 1.12,
  SEK: 0.094,
  DKK: 0.143,
  NOK: 0.092,
  ISK: 0.0072,
  PLN: 0.25,
};

// Yahoo returns a few currency codes in non-standard casings (notably 'GBp' for
// UK pence, occasionally 'ILa' for Israeli agorot, 'ZAc' for SA cents). This
// normalises to the upper-case ISO-4217-ish key we use in FX_CACHE.
function normalizeCurrency(raw) {
  if (!raw) return 'USD';
  const s = String(raw);
  // Yahoo returns 'GBp' (lowercase p) for UK pence. 'GBX' is the alternative
  // ticker-style code some feeds use. Treat both as pence.
  if (s === 'GBp' || s.toUpperCase() === 'GBX') return 'GBX';
  // ZAc (South African cents), ILa (Israeli agorot) — very rare minor units.
  if (s === 'ZAc') return 'ZAR';  // accept as rand; we don't scale (no deals)
  if (s === 'ILa') return 'ILS';
  return s.toUpperCase();
}

let FX_CACHE = { ...FX_TO_USD_DEFAULT, _refreshedAt: 0 };

async function refreshFx() {
  // Refresh only once an hour
  if (Date.now() - FX_CACHE._refreshedAt < 3600_000) return FX_CACHE;
  const pairs = { GBP: 'GBPUSD=X', EUR: 'EURUSD=X', CHF: 'CHFUSD=X',
                  SEK: 'SEKUSD=X', DKK: 'DKKUSD=X', NOK: 'NOKUSD=X',
                  ISK: 'ISKUSD=X', PLN: 'PLNUSD=X' };
  try {
    const symbols = Object.values(pairs);
    const results = await getYf().quote(symbols);
    const arr = Array.isArray(results) ? results : [results];
    const next = { ...FX_TO_USD_DEFAULT };
    for (const [ccy, sym] of Object.entries(pairs)) {
      const hit = arr.find(r => r && r.symbol === sym);
      if (hit && hit.regularMarketPrice) next[ccy] = hit.regularMarketPrice;
    }
    next.GBX = next.GBP / 100;
    next._refreshedAt = Date.now();
    FX_CACHE = next;
  } catch (e) {
    console.warn('[market_data] FX refresh failed:', e.message);
  }
  return FX_CACHE;
}

function toUsd(amount, currency) {
  if (amount == null) return null;
  const key = normalizeCurrency(currency);
  const rate = FX_CACHE[key];
  if (rate == null) {
    // Log once per unknown currency so we notice gaps in the FX table.
    if (!toUsd._warned) toUsd._warned = new Set();
    if (!toUsd._warned.has(key)) {
      console.warn(`[market_data] no FX rate for currency "${currency}" (normalized "${key}") \u2014 returning null`);
      toUsd._warned.add(key);
    }
    return null;
  }
  return Number(amount) * rate;
}

// Core quote fetch. Returns normalized shape or null on failure.
async function fetchQuote(yahooSymbol) {
  if (!yahooSymbol) return null;
  try {
    await refreshFx();
    await throttle();
    const q = await getYf().quote(yahooSymbol);
    if (!q) return null;

    const currency = q.currency || 'USD';
    const price = q.regularMarketPrice ?? q.postMarketPrice ?? q.preMarketPrice ?? null;
    const marketCap = q.marketCap ?? null;

    // Try to get sector/industry from quoteSummary (richer endpoint).
    let sector = null, industry = null;
    try {
      const qs = await getYf().quoteSummary(yahooSymbol, { modules: ['assetProfile', 'summaryProfile'] });
      sector = qs?.assetProfile?.sector || qs?.summaryProfile?.sector || null;
      industry = qs?.assetProfile?.industry || qs?.summaryProfile?.industry || null;
    } catch (_) {
      // Not all symbols have assetProfile (e.g. newly-listed SPACs); ignore.
    }

    return {
      symbol: yahooSymbol,
      price,
      priceUsd: toUsd(price, currency),
      marketCap,
      marketCapUsd: toUsd(marketCap, currency),
      currency,
      sector,
      industry,
      shortName: q.shortName || q.longName || null,
    };
  } catch (e) {
    console.warn(`[market_data] quote failed for ${yahooSymbol}:`, e.message);
    return null;
  }
}

// Historical close nearest a target date. Used for announce_price.
async function fetchHistoricalPrice(yahooSymbol, dateStr) {
  if (!yahooSymbol || !dateStr) return null;
  try {
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return null;
    const start = new Date(date);
    start.setDate(start.getDate() - 5);
    const end = new Date(date);
    end.setDate(end.getDate() + 5);

    await throttle();
    const hist = await getYf().chart(yahooSymbol, {
      period1: start, period2: end, interval: '1d',
    });
    const quotes = hist?.quotes || [];
    if (!quotes.length) return null;

    // Pick the trading day closest to the target date.
    const target = date.getTime();
    let best = null, bestDiff = Infinity;
    for (const q of quotes) {
      if (q.close == null) continue;
      const d = Math.abs(new Date(q.date).getTime() - target);
      if (d < bestDiff) { bestDiff = d; best = q; }
    }
    return best ? { date: best.date, close: best.close } : null;
  } catch (e) {
    console.warn(`[market_data] historical failed for ${yahooSymbol} @ ${dateStr}:`, e.message);
    return null;
  }
}

// Given a deal row, populate market-data columns. Idempotent \u2014 safe to re-run.
// Only overwrites non-derived fields; primary_ticker is re-derived each call.
async function refreshDeal(deal) {
  const parsed = pickPrimaryTicker(deal);
  if (!parsed) {
    return { updated: false, reason: 'no parsable ticker' };
  }

  const quote = await fetchQuote(parsed.yahooSymbol);
  const updates = {
    primary_ticker: parsed.label,
    yahoo_symbol:   parsed.yahooSymbol,
    country:        parsed.country,
  };
  if (quote) {
    // CRITICAL: only fall back to native price if FX conversion actually
    // failed. For unknown currencies priceUsd is null and falling back would
    // mix units (e.g. UK pence into a USD column). Previously we used
    // `priceUsd ?? price` which silently stored pence as USD for GBp listings.
    updates.current_price  = quote.priceUsd != null ? quote.priceUsd : null;
    updates.market_cap_usd = quote.marketCapUsd ?? null;
    updates.currency       = quote.currency;
    if (quote.sector && !deal.sector)     updates.sector   = quote.sector;
    if (quote.industry && !deal.industry) updates.industry = quote.industry;

    // Convert LLM-extracted offer_price from native currency to USD the
    // first time we see it (or every refresh if we haven't recorded an
    // offer_currency yet \u2014 keeps behaviour idempotent once converted).
    // The LLM sees raw headlines like "recommended offer of 61p per share"
    // and returns `61`; the quote's native currency is the best proxy for
    // the offer's currency since the target lists there.
    if (deal.offer_price != null && deal.offer_price_converted !== 1) {
      const offerUsd = toUsd(deal.offer_price, quote.currency || 'USD');
      if (offerUsd != null && offerUsd !== Number(deal.offer_price)) {
        updates.offer_price = offerUsd;
        updates.offer_price_converted = 1;
      } else if (offerUsd != null && offerUsd === Number(deal.offer_price)) {
        // Already USD \u2014 just mark as converted so we don't re-process.
        updates.offer_price_converted = 1;
      }
    }
  }

  // Populate announce_price once, if we have an announce_date.
  if (!deal.announce_price && deal.announce_date) {
    const hist = await fetchHistoricalPrice(parsed.yahooSymbol, deal.announce_date);
    if (hist?.close != null) {
      // Require successful FX conversion; if unknown currency, leave null
      // rather than mixing units. The `?? hist.close` fallback used to stamp
      // UK pence into the USD column \u2014 see FX fix April 2026.
      updates.announce_price = toUsd(hist.close, quote?.currency || 'USD');
    }
  }

  // ---- Spin-off specific: also fetch parent + spinco prices and returns ----
  // The generic primary_ticker may be either the parent or the spinco; here we
  // resolve both explicitly when the deal exposes them as separate fields.
  if (deal.deal_type === 'spin_off' || (deal.event_type || '').startsWith('spin_off')) {
    const exDate = deal.ex_date || deal.completed_date ||
                   (deal.key_dates && typeof deal.key_dates === 'object' && (deal.key_dates.ex_date || deal.key_dates.first_trade_date || deal.key_dates.completed_date));

    const parentParsed = deal.parent_ticker ? parseTicker(deal.parent_ticker) : null;
    if (parentParsed?.yahooSymbol) {
      const pq = await fetchQuote(parentParsed.yahooSymbol);
      if (pq?.priceUsd != null) {
        updates.parent_current_price = pq.priceUsd;
        if (exDate && !deal.parent_baseline_price) {
          const ph = await fetchHistoricalPrice(parentParsed.yahooSymbol, exDate);
          if (ph?.close != null) {
            updates.parent_baseline_price = toUsd(ph.close, pq.currency || 'USD');
          }
        }
        const base = updates.parent_baseline_price ?? deal.parent_baseline_price;
        if (base && updates.parent_current_price) {
          updates.parent_return_pct = ((updates.parent_current_price - base) / base) * 100;
        }
      }
    }

    const spincoParsed = deal.spinco_ticker ? parseTicker(deal.spinco_ticker) : null;
    if (spincoParsed?.yahooSymbol) {
      const sq = await fetchQuote(spincoParsed.yahooSymbol);
      if (sq?.priceUsd != null) {
        updates.spinco_current_price = sq.priceUsd;
        if (exDate && !deal.spinco_baseline_price) {
          const sh = await fetchHistoricalPrice(spincoParsed.yahooSymbol, exDate);
          if (sh?.close != null) {
            updates.spinco_baseline_price = toUsd(sh.close, sq.currency || 'USD');
          }
        }
        const base = updates.spinco_baseline_price ?? deal.spinco_baseline_price;
        if (base && updates.spinco_current_price) {
          updates.spinco_return_pct = ((updates.spinco_current_price - base) / base) * 100;
        }
      }
    }
  }

  // ---- Merger arb specific: unaffected price + spread-to-deal ----
  // Unaffected price = close on the trading day BEFORE announce_date —
  // i.e. before the market knew about the deal. This is the true reference
  // for measuring bid premium.
  //
  // Spread-to-deal = (offer_price - current_price) / current_price. Positive
  // spread means current trades BELOW offer — arb opportunity; negative means
  // market expects a bump.
  const isMerger = deal.deal_type === 'merger_arb' ||
                   (deal.event_type || '').startsWith('merger');
  if (isMerger) {
    // Unaffected price: fetch once, when announce_date known and not yet set
    if (deal.announce_date && !deal.unaffected_price) {
      // Go back 1 calendar day — fetchHistoricalPrice already finds nearest
      // trading day, so Monday announce → we'll get Friday close.
      const unaffDate = new Date(deal.announce_date);
      unaffDate.setDate(unaffDate.getDate() - 1);
      const unaffStr = unaffDate.toISOString().slice(0, 10);
      const hist = await fetchHistoricalPrice(parsed.yahooSymbol, unaffStr);
      if (hist?.close != null) {
        updates.unaffected_price = toUsd(hist.close, quote?.currency || 'USD');
      }
    }
    // Spread-to-deal: needs both offer_price and current_price
    // Use the post-conversion USD values if present in updates (for offer_price
    // we may have just converted from native currency).
    const offer = updates.offer_price ?? deal.offer_price;
    const curr  = updates.current_price ?? deal.current_price;
    if (offer && curr && curr > 0) {
      const spread = ((offer - curr) / curr) * 100;
      // Sanity guard: an arb spread >300% or <-80% is almost certainly bad data
      // (wrong ticker, stale quote, inverted fields). Don't poison the DB with
      // the nonsense number \u2014 leave spread null so the UI hides it.
      if (Math.abs(spread) <= 300) {
        updates.spread_to_deal_pct = spread;
      } else {
        updates.spread_to_deal_pct = null;
        console.warn(`[market_data] deal ${deal.id} ${deal.primary_ticker}: spread ${spread.toFixed(1)}% out of range (offer=${offer}, curr=${curr}) \u2014 nulled`);
      }
    }
  }

  // Build SQL SET clause.
  const set = [];
  const params = [];
  for (const [k, v] of Object.entries(updates)) {
    params.push(v);
    set.push(`${k} = $${params.length}`);
  }
  const tsCol = process.env.DATABASE_URL ? 'NOW()' : "datetime('now')";
  set.push(`market_refreshed_at = ${tsCol}`);
  params.push(deal.id);
  await query(`UPDATE deals SET ${set.join(', ')} WHERE id = $${params.length}`, params);

  return { updated: true, primary_ticker: parsed.label, market_cap_usd: updates.market_cap_usd };
}

// Batch refresh. Called from /api/admin/refresh-prices and the daily cron.
// Resilient: every deal is wrapped in try/catch, Yahoo errors never bubble
// up to the HTTP layer, and we throttle between calls to stay under Yahoo's
// rate limit. `limit` defaults to 500 but is sliced into 50-deal chunks with
// a small pause between chunks to avoid memory spikes.
async function refreshAllDeals({ activeOnly = true, limit = 500 } = {}) {
  const where = activeOnly
    ? `WHERE status IN ('announced','pending','rumored','completed')`
    : '';
  const deals = await query(
    `SELECT * FROM deals ${where} ORDER BY COALESCE(announce_date, ${process.env.DATABASE_URL ? 'first_seen_at::text' : 'first_seen_at'}) DESC LIMIT $1`,
    [limit]
  );
  let ok = 0, skipped = 0, errors = 0;
  const CHUNK = 50;
  for (let i = 0; i < deals.length; i += CHUNK) {
    const chunk = deals.slice(i, i + CHUNK);
    for (const d of chunk) {
      try {
        const r = await refreshDeal(d);
        if (r.updated) ok++; else skipped++;
      } catch (e) {
        errors++;
        console.warn(`[market_data] refreshDeal ${d.id} failed:`, e.message);
      }
    }
    // Breathing room between chunks
    await new Promise(r => setTimeout(r, 500));
  }
  return { total: deals.length, refreshed: ok, skipped, errors };
}

module.exports = {
  fetchQuote,
  fetchHistoricalPrice,
  refreshDeal,
  refreshAllDeals,
  refreshFx,
  toUsd,
};
