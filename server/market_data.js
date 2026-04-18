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
  const rate = FX_CACHE[currency];
  if (rate == null) return null;
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
    updates.current_price  = quote.priceUsd ?? quote.price ?? null;
    updates.market_cap_usd = quote.marketCapUsd ?? null;
    updates.currency       = quote.currency;
    if (quote.sector && !deal.sector)     updates.sector   = quote.sector;
    if (quote.industry && !deal.industry) updates.industry = quote.industry;
  }

  // Populate announce_price once, if we have an announce_date.
  if (!deal.announce_price && deal.announce_date) {
    const hist = await fetchHistoricalPrice(parsed.yahooSymbol, deal.announce_date);
    if (hist?.close != null) {
      updates.announce_price = toUsd(hist.close, quote?.currency || 'USD') ?? hist.close;
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
            updates.parent_baseline_price = toUsd(ph.close, pq.currency || 'USD') ?? ph.close;
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
            updates.spinco_baseline_price = toUsd(sh.close, sq.currency || 'USD') ?? sh.close;
          }
        }
        const base = updates.spinco_baseline_price ?? deal.spinco_baseline_price;
        if (base && updates.spinco_current_price) {
          updates.spinco_return_pct = ((updates.spinco_current_price - base) / base) * 100;
        }
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
    `SELECT * FROM deals ${where} ORDER BY COALESCE(announce_date, first_seen_at) DESC LIMIT $1`,
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
