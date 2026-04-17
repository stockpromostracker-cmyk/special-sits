// Market data fetcher using yahoo-finance2. Free, no API key required,
// covers global exchanges via the same .SUFFIX convention we use.
//
// Two entry points:
//   fetchQuote(yahooSymbol)   \u2014 one-shot fetch, returns { price, marketCap, currency, sector, industry, shortName }
//   refreshDeal(deal)         \u2014 populate primary_ticker + market_cap + current_price (+ announce_price on first fetch)

const { pickPrimaryTicker } = require('./tickers');
const { query } = require('./db');

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
async function refreshAllDeals({ activeOnly = true, limit = 500 } = {}) {
  const where = activeOnly
    ? `WHERE status IN ('announced','pending','rumored')`
    : '';
  const deals = await query(
    `SELECT * FROM deals ${where} ORDER BY COALESCE(announce_date, first_seen_at) DESC LIMIT $1`,
    [limit]
  );
  let ok = 0, skipped = 0;
  for (const d of deals) {
    const r = await refreshDeal(d).catch(e => ({ updated: false, reason: e.message }));
    if (r.updated) ok++; else skipped++;
  }
  return { total: deals.length, refreshed: ok, skipped };
}

module.exports = {
  fetchQuote,
  fetchHistoricalPrice,
  refreshDeal,
  refreshAllDeals,
  refreshFx,
  toUsd,
};
