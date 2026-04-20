// Rollup insider_transactions into per-deal aggregates stored on the deals row.
// Runs after ingestion and (again) after the daily market-data refresh so the
// trading_below_insider_price flag uses the latest current_price.

const { query } = require('./db');
const { KNOWN_ACTIVISTS } = require('./insider_feeds');

// Compute aggregates for a single deal and persist them onto the deals row.
async function rollupDeal(deal) {
  const ticker = deal.primary_ticker || deal.target_ticker || deal.parent_ticker
              || deal.spinco_ticker || deal.acquirer_ticker;
  if (!ticker) return { skipped: true, reason: 'no ticker' };

  // 1) Trailing 180-day insider trades \u2014 open-market P/S only
  const cutoff = new Date(Date.now() - 180 * 86400 * 1000).toISOString().slice(0, 10);

  // Match by issuer_ticker (preferred) OR fall back to issuer_name ILIKE target/parent name.
  // We store tickers in insider_transactions as "US:AAPL" for SEC, so normalise the compare
  // against the bare symbol as well.
  const bareSymbol = String(ticker).split(':').pop();
  const nameCandidates = [
    deal.target_name, deal.parent_name, deal.acquirer_name, deal.spinco_name,
  ].filter(Boolean).map(n => n.toLowerCase());

  const trades = await findTransactions({ ticker, bareSymbol, nameCandidates, cutoff });

  const buys  = trades.filter(t => t.is_buy === 1 || t.is_buy === true);
  const sells = trades.filter(t => t.is_buy === 0 || t.is_buy === false);
  const stakes = trades.filter(t => t.is_buy == null);  // 13D/G disclosures

  const buyUsd  = sum(buys.map(t => Number(t.value_usd || 0)));
  const sellUsd = sum(sells.map(t => Number(t.value_usd || 0)));
  const netUsd  = buyUsd - sellUsd;

  const distinctBuyers = new Set(buys.map(t => (t.insider_name || '').toLowerCase()).filter(Boolean));
  const clusterBuying = distinctBuyers.size >= 3 ? 1 : 0;

  // Volume-weighted avg buy price (USD)
  let avgPrice = null;
  const pricedBuys = buys.filter(t => t.price_usd && t.shares);
  if (pricedBuys.length) {
    const numer = sum(pricedBuys.map(t => Number(t.price_usd) * Math.abs(Number(t.shares))));
    const denom = sum(pricedBuys.map(t => Math.abs(Number(t.shares))));
    avgPrice = denom > 0 ? numer / denom : null;
  }

  const currentPrice = deal.current_price != null ? Number(deal.current_price) : null;
  const tradingBelow = (avgPrice != null && currentPrice != null && currentPrice < avgPrice) ? 1 : 0;

  // Activist flag \u2014 either already extracted by the LLM, or derived from 13D rows
  // whose insider_name matches a KNOWN_ACTIVISTS entry.
  let activistOnRegister = deal.activist_on_register || 0;
  if (!activistOnRegister) {
    for (const s of stakes) {
      const name = s.insider_name || '';
      if (KNOWN_ACTIVISTS.some(a => new RegExp(a.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i').test(name))) {
        activistOnRegister = 1;
        break;
      }
    }
  }

  const tsFn = process.env.DATABASE_URL ? 'NOW()' : "datetime('now')";
  await query(
    `UPDATE deals SET
       insider_buy_count_6m = $1,
       insider_buy_usd_6m = $2,
       insider_sell_usd_6m = $3,
       insider_net_usd_6m = $4,
       cluster_buying = $5,
       avg_insider_buy_price = $6,
       trading_below_insider_price = $7,
       activist_on_register = COALESCE($8, activist_on_register),
       insider_refreshed_at = ${tsFn}
     WHERE id = $9`,
    [
      distinctBuyers.size,
      buyUsd || null,
      sellUsd || null,
      netUsd || null,
      clusterBuying,
      avgPrice,
      tradingBelow,
      activistOnRegister,
      deal.id,
    ]
  );

  return {
    deal_id: deal.id,
    ticker,
    trades: trades.length,
    buys: buys.length,
    sells: sells.length,
    stakes: stakes.length,
    distinct_buyers: distinctBuyers.size,
    cluster_buying: clusterBuying,
    avg_insider_buy_price: avgPrice,
    trading_below_insider_price: tradingBelow,
    activist_on_register: activistOnRegister,
  };
}

async function rollupAll({ activeOnly = true, limit = 500 } = {}) {
  // Include completed deals up to 365 days old — insider activity around a
  // recently-completed spin-off (e.g. Coffee Stain in Dec 2025) is highly
  // material. Stale completed deals (>1y) are skipped to keep this fast.
  const where = activeOnly
    ? `WHERE status IN ('rumored','announced','pending')
          OR (status = 'completed' AND (completed_date IS NULL
              OR completed_date >= ${process.env.DATABASE_URL ? "NOW() - INTERVAL '365 days'" : "date('now', '-365 days')"}))`
    : '';
  const rows = await query(`SELECT * FROM deals ${where} ORDER BY id DESC LIMIT $1`, [limit]);
  let ok = 0, skipped = 0;
  for (const d of rows) {
    try {
      const r = await rollupDeal(d);
      if (r.skipped) skipped++; else ok++;
    } catch (e) {
      console.error('[rollupAll]', d.id, e.message);
    }
  }
  console.log(`[incentives] rollup \u2014 ok=${ok}, skipped=${skipped} (of ${rows.length})`);
  return { ok, skipped, total: rows.length };
}

// List insider transactions attached to a deal (used by drawer UI).
async function listTransactionsForDeal(deal) {
  const ticker = deal.primary_ticker || deal.target_ticker || deal.parent_ticker
              || deal.spinco_ticker || deal.acquirer_ticker;
  if (!ticker) return [];
  const bareSymbol = String(ticker).split(':').pop();
  const cutoff = new Date(Date.now() - 180 * 86400 * 1000).toISOString().slice(0, 10);
  const nameCandidates = [
    deal.target_name, deal.parent_name, deal.acquirer_name, deal.spinco_name,
  ].filter(Boolean).map(n => n.toLowerCase());
  const rows = await findTransactions({
    ticker, bareSymbol, nameCandidates, cutoff, orderAndLimit: true,
  });
  return rows;
}

// Portable search across Postgres and SQLite. Name-match uses LIKE against each
// candidate string individually (SQLite has no ANY, and Postgres ANY fails on
// empty arrays).
//
// We also fuzzy-match issuer_name with:
//   • case-insensitive prefix match (e.g. "Coffee Stain Group AB" LIKE "coffee stain group%")
//   • stripping legal suffixes (AB / plc / NV / SA / Inc / Ltd) before comparison
function stripLegalSuffix(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/[.,]/g, ' ')
    .replace(/\b(ab|plc|nv|n\.v\.|s\.e\.|se|sa|oyj|asa|as|a\/s|gmbh|inc|ltd|corp|corporation|holdings|group|company|co)\b/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

async function findTransactions({ ticker, bareSymbol, nameCandidates, cutoff, orderAndLimit }) {
  const where = [
    `(issuer_ticker = $1 OR issuer_ticker = $2`,
  ];
  const params = [ticker, `US:${bareSymbol}`];
  const nameClauses = [];
  const seen = new Set();
  for (const n of nameCandidates.slice(0, 4)) {
    // Exact match
    if (!seen.has(n)) {
      seen.add(n);
      params.push(n);
      nameClauses.push(`LOWER(issuer_name) = $${params.length}`);
    }
    // Prefix match — catches "Coffee Stain" matching "Coffee Stain Group AB"
    // and vice-versa. Use the first two meaningful words as the prefix.
    const stripped = stripLegalSuffix(n);
    const prefix = stripped.split(/\s+/).slice(0, 2).join(' ');
    if (prefix && prefix.length >= 4 && !seen.has(prefix)) {
      seen.add(prefix);
      params.push(`${prefix}%`);
      nameClauses.push(`LOWER(issuer_name) LIKE $${params.length}`);
    }
  }
  if (nameClauses.length) {
    where[0] += ` OR ${nameClauses.join(' OR ')}`;
  }
  where[0] += ')';
  params.push(cutoff);
  where.push(`transaction_date >= $${params.length}`);

  const tail = orderAndLimit ? 'ORDER BY transaction_date DESC, id DESC LIMIT 50' : '';
  return query(
    `SELECT * FROM insider_transactions WHERE ${where.join(' AND ')} ${tail}`,
    params
  );
}

function sum(arr) { return arr.reduce((a, b) => a + (Number(b) || 0), 0); }

module.exports = { rollupDeal, rollupAll, listTransactionsForDeal };
