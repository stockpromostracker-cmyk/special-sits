// Compensation & ownership — surface REAL data, not just link-outs.
//
// Two angles:
//
//   1. OWNERSHIP  — aggregate all known holdings for a deal from the
//      insider_transactions table (US Form 4/3, UK RNS, NL AFM, SE FI).
//      Ranks insiders by last-reported shares (highest = largest holder),
//      with recent buy/sell activity flags.
//
//   2. COMPENSATION — for US issuers with a CIK, pull CEO/CFO total comp
//      from SEC XBRL (Pay-vs-Performance taxonomy). Non-US issuers fall
//      back to a link-out to the national regulator's register.
//
// Both are best-effort: empty arrays are valid results.

const { query } = require('./db');

const UA = 'SpecialSits Research cfrjacobsson@gmail.com';

// --------------------------------------------------------------------------
// 1. Ownership — aggregate from insider_transactions
// --------------------------------------------------------------------------
//
// Form 4 includes a running postTransactionAmounts value that represents
// the total holding AFTER the filed transaction. We don't currently store
// that column, so we approximate ranking by: most-recent transaction size
// per insider, summed over the trailing 3 years. That captures the biggest
// disclosed movers even if it isn't a perfect beneficial-ownership snapshot.
//
// For non-US (AFM, FI) we only have transaction amounts — same approximation.

async function ownershipForDeal(deal) {
  const ticker = deal.primary_ticker || deal.target_ticker || deal.parent_ticker
             || deal.spinco_ticker || deal.acquirer_ticker;
  if (!ticker) return { holders: [], notes: ['No ticker'] };

  const bareSymbol = String(ticker).split(':').pop();
  const nameCandidates = [
    deal.target_name, deal.parent_name, deal.acquirer_name, deal.spinco_name,
  ].filter(Boolean).map(n => n.toLowerCase());

  // Trailing 3 years so we capture Form 3 (initial ownership) and subsequent activity.
  const cutoff = new Date(Date.now() - 3 * 365 * 86400 * 1000).toISOString().slice(0, 10);

  // Name normalizer — strip corporate suffixes + lowercase. This lets us
  // match "Signify N.V." ↔ "Signify" ↔ "Signify NV".
  const normKey = (s) => String(s || '').toLowerCase()
    .replace(/[,.]/g, ' ')
    .replace(/\b(inc|corp|corporation|company|co|ltd|limited|plc|ag|sa|nv|n\.v|s\.a|se|spa|ab|asa|gmbh|llc|lp|oyj|abp|holdings?|group)\b/g, ' ')
    .replace(/\s+/g, ' ').trim();
  const normNames = nameCandidates.map(normKey).filter(Boolean);

  const where = [`(issuer_ticker = $1 OR issuer_ticker = $2`];
  const params = [ticker, `US:${bareSymbol}`];
  // Exact name match (original)
  for (const n of nameCandidates.slice(0, 4)) {
    params.push(n);
    where[0] += ` OR LOWER(issuer_name) = $${params.length}`;
  }
  // Prefix-match against normalized name — anchors at start and compares a
  // meaningful substring so "Signify" matches "Signify N.V." without
  // matching unrelated companies. We use LIKE on LOWER(issuer_name).
  for (const n of normNames.slice(0, 4)) {
    if (n.length < 4) continue; // too short, risk of false positives
    params.push(`${n}%`);
    where[0] += ` OR LOWER(issuer_name) LIKE $${params.length}`;
    params.push(`% ${n} %`);
    where[0] += ` OR ' ' || LOWER(issuer_name) || ' ' LIKE $${params.length}`;
  }
  where[0] += ')';
  params.push(cutoff);
  where.push(`transaction_date >= $${params.length}`);

  const rows = await query(
    `SELECT * FROM insider_transactions WHERE ${where.join(' AND ')}
     ORDER BY transaction_date DESC, id DESC`, params);

  if (!rows.length) return { holders: [], notes: ['No transaction history'] };

  // Group by insider_name. For each holder compute:
  //   - net_shares (sum of shares, signed; positive=accumulated)
  //   - last_title (most recent title we saw)
  //   - buy_count / sell_count (trailing 180d for recency flag)
  //   - latest_date
  //   - total_buy_usd / total_sell_usd (trailing 3y)
  const cutoff180 = new Date(Date.now() - 180 * 86400 * 1000).toISOString().slice(0, 10);
  const byInsider = new Map();
  for (const r of rows) {
    const key = (r.insider_name || '').trim();
    if (!key) continue;
    if (!byInsider.has(key)) {
      byInsider.set(key, {
        insider_name: key,
        insider_title: r.insider_title || null,
        is_director: !!r.is_director,
        is_officer: !!r.is_officer,
        is_ten_percent_owner: !!r.is_ten_percent_owner,
        net_shares: 0,
        last_shares: null,          // most recent transaction shares (signed)
        last_date: null,
        buy_count_180d: 0,
        sell_count_180d: 0,
        buy_usd_180d: 0,
        sell_usd_180d: 0,
        source: r.source,
      });
    }
    const h = byInsider.get(key);
    // Signed shares: is_buy=1 → positive, is_buy=0 → negative, null → unsigned
    const signedShares = r.is_buy === 1 ? Math.abs(Number(r.shares || 0))
                       : r.is_buy === 0 ? -Math.abs(Number(r.shares || 0))
                       : Number(r.shares || 0);
    h.net_shares += signedShares;
    if (!h.last_date || (r.transaction_date && r.transaction_date > h.last_date)) {
      h.last_date = r.transaction_date;
      h.last_shares = signedShares;
      if (r.insider_title && !h.insider_title) h.insider_title = r.insider_title;
    }
    if (r.transaction_date && r.transaction_date >= cutoff180) {
      if (r.is_buy === 1) { h.buy_count_180d++;  h.buy_usd_180d  += Number(r.value_usd || 0); }
      if (r.is_buy === 0) { h.sell_count_180d++; h.sell_usd_180d += Number(r.value_usd || 0); }
    }
  }

  // Rank: net_shares desc, then last_date desc.
  const holders = [...byInsider.values()].sort((a, b) => {
    const na = Math.abs(a.net_shares), nb = Math.abs(b.net_shares);
    if (nb !== na) return nb - na;
    return (b.last_date || '').localeCompare(a.last_date || '');
  }).slice(0, 15);

  return { holders, notes: [], total_transactions: rows.length };
}

// --------------------------------------------------------------------------
// 2. Executive compensation — SEC Pay-vs-Performance (US only)
// --------------------------------------------------------------------------
//
// Companies subject to Reg S-K Item 402(v) (2023+) file a Pay-vs-Performance
// XBRL table in their DEF 14A. Key concepts:
//   ecd:PeoTotalCompAmt         — CEO (Principal Executive Officer) total comp
//   ecd:PeoActuallyPaidCompAmt  — CAP metric (stock mark-to-market)
//   ecd:NonPeoNeoAvgTotalCompAmt — average for other named execs
//
// SEC's companyconcept API returns all historical values for a concept.
// We pull the two most recent fiscal years.
//
// Non-US issuers: return empty + a regulator link-out (handled by caller).

async function fetchUsCompensation(cik) {
  if (!cik) return null;
  const cikPadded = String(cik).padStart(10, '0');
  const concepts = [
    { concept: 'PeoTotalCompAmt',         label: 'CEO total compensation' },
    { concept: 'PeoActuallyPaidCompAmt',  label: 'CEO compensation actually paid' },
    { concept: 'NonPeoNeoAvgTotalCompAmt',label: 'Other NEO avg total comp' },
    { concept: 'NonPeoNeoAvgCompActuallyPaidAmt', label: 'Other NEO comp actually paid' },
  ];
  const out = [];
  for (const c of concepts) {
    const url = `https://data.sec.gov/api/xbrl/companyconcept/CIK${cikPadded}/ecd/${c.concept}.json`;
    try {
      const r = await fetch(url, { headers: { 'User-Agent': UA, 'Accept': 'application/json' } });
      if (!r.ok) continue;
      const j = await r.json();
      const usd = j.units?.USD || [];
      // Newest two fiscal years, dedup by fy.
      const sorted = usd.slice().sort((a, b) => (b.fy || 0) - (a.fy || 0));
      const seen = new Set();
      const years = [];
      for (const v of sorted) {
        if (!v.fy || seen.has(v.fy)) continue;
        seen.add(v.fy);
        years.push({ fy: v.fy, val: v.val, form: v.form });
        if (years.length >= 2) break;
      }
      if (years.length) out.push({ concept: c.concept, label: c.label, years });
    } catch (_) { /* ignore */ }
  }
  return out.length ? out : null;
}

async function compensationForDeal(deal) {
  // Only US issuers have CIKs; EU regulators don't publish comp in a parseable API.
  if (deal.source_cik) {
    const us = await fetchUsCompensation(deal.source_cik);
    if (us) return { source: 'sec_xbrl', items: us };
  }
  return null;
}

// --------------------------------------------------------------------------
// 3. Beneficial holders — who ACTUALLY owns the stock (not movers)
// --------------------------------------------------------------------------
//
// Pulls the most-recent snapshot per (holder) from beneficial_holders for
// any ticker/ISIN matched to this deal. Different sources (sec_13f,
// nordic_major, uk_tr1, afm_nl_subst) can coexist — the UI shows them
// grouped and the concentration math prefers whichever source gave a
// pct-of-capital measurement.
async function beneficialHoldersForDeal(deal) {
  const candidateTickers = [
    deal.primary_ticker, deal.target_ticker, deal.spinco_ticker, deal.parent_ticker,
  ].filter(Boolean);
  if (!candidateTickers.length) return { holders: [], sources: [], notes: ['no ticker'] };

  // Also try bare symbol (without EXCHANGE: prefix) since sec_13f stores that
  const bareSymbols = candidateTickers.map(t => String(t).split(':').pop());
  const tickerSet = new Set([...candidateTickers, ...bareSymbols]);

  // Name keys for fuzzy-match against issuer_name
  const nameKeys = [deal.target_name, deal.spinco_name, deal.parent_name]
    .filter(Boolean)
    .map(n => String(n).toLowerCase()
      .replace(/\b(inc|corp|corporation|company|co|ltd|limited|plc|ag|sa|nv|n\.v|s\.a|se|spa|ab|asa|gmbh|llc|lp|oyj|abp|holdings?|group)\b/g, ' ')
      .replace(/\s+/g, ' ').trim())
    .filter(s => s.length >= 4);

  const params = [];
  const clauses = [];
  for (const t of tickerSet) {
    params.push(t);
    clauses.push(`issuer_ticker = $${params.length}`);
  }
  for (const n of nameKeys) {
    params.push(`${n}%`);
    clauses.push(`LOWER(issuer_name) LIKE $${params.length}`);
  }
  if (!clauses.length) return { holders: [], sources: [], notes: ['no match'] };

  const rows = await query(
    `SELECT source, issuer_name, issuer_ticker, holder_name, holder_type,
            as_of_date, shares, position_pct, value_usd, filing_url, is_13d, raw_holder_type
     FROM beneficial_holders WHERE ${clauses.join(' OR ')}
     ORDER BY as_of_date DESC, id DESC`,
    params
  );

  if (!rows.length) return { holders: [], sources: [], notes: ['no disclosed holders'] };

  // Keep most-recent snapshot per (holder_name, source). Different sources
  // may show the same fund — we keep both rows so the UI can show provenance.
  const seen = new Map();
  for (const r of rows) {
    const key = `${r.source}|${(r.holder_name || '').toLowerCase()}`;
    if (!seen.has(key)) seen.set(key, r);
  }
  const latest = [...seen.values()];

  // Rank: position_pct desc (primary), then value_usd desc, then shares desc.
  latest.sort((a, b) => {
    const pa = Number(a.position_pct || 0), pb = Number(b.position_pct || 0);
    if (pb !== pa) return pb - pa;
    const va = Number(a.value_usd || 0), vb = Number(b.value_usd || 0);
    if (vb !== va) return vb - va;
    return Number(b.shares || 0) - Number(a.shares || 0);
  });

  const sources = [...new Set(latest.map(r => r.source))];
  return { holders: latest.slice(0, 25), sources, notes: [], total: latest.length };
}

// --------------------------------------------------------------------------
// 4. Ownership concentration summary
// --------------------------------------------------------------------------
// From the beneficial-holders snapshot, compute:
//   - top5_pct, top10_pct (sum of pct across top N holders)
//   - insider_pct (sum where holder_type = insider)
//   - institutional_pct (sum where holder_type in institutional/index_fund)
//   - implied_free_float_pct = 100 - (sum of all disclosed insider/strategic)
function concentrationFromHolders(holders) {
  if (!holders || !holders.length) return null;
  const withPct = holders.filter(h => Number(h.position_pct) > 0);
  if (!withPct.length) return null;
  const sum = (arr) => arr.reduce((a, h) => a + Number(h.position_pct || 0), 0);
  const top5  = sum([...withPct].sort((a,b) => Number(b.position_pct)-Number(a.position_pct)).slice(0, 5));
  const top10 = sum([...withPct].sort((a,b) => Number(b.position_pct)-Number(a.position_pct)).slice(0, 10));
  const insider       = sum(withPct.filter(h => h.holder_type === 'insider'));
  const institutional = sum(withPct.filter(h => h.holder_type === 'institutional' || h.holder_type === 'index_fund'));
  const strategic     = sum(withPct.filter(h => h.holder_type === 'strategic' || h.holder_type === 'state' || h.holder_type === 'foundation'));
  const disclosed_total = sum(withPct);
  return {
    top5_pct: round(top5),
    top10_pct: round(top10),
    insider_pct: round(insider),
    institutional_pct: round(institutional),
    strategic_pct: round(strategic),
    disclosed_total_pct: round(disclosed_total),
    holder_count: withPct.length,
  };
}
function round(n) { return Math.round(Number(n) * 100) / 100; }

// --------------------------------------------------------------------------
// 5. Disclosed short positions (regulator-disclosed >=0.5% EU/UK)
// --------------------------------------------------------------------------
async function disclosedShortsForDeal(deal) {
  const bareSym = String(deal.primary_ticker || deal.target_ticker || deal.spinco_ticker || '').split(':').pop();
  if (!bareSym) return [];
  const names = [deal.target_name, deal.spinco_name, deal.parent_name].filter(Boolean);
  const params = [bareSym];
  const clauses = [`issuer_ticker LIKE '%' || $1`];
  for (const n of names) {
    params.push(`${n.toLowerCase()}%`);
    clauses.push(`LOWER(issuer_name) LIKE $${params.length}`);
  }
  const rows = await query(
    `SELECT source, holder_name, issuer_name, position_pct, as_of_date
       FROM short_positions
      WHERE kind = 'disclosed_position' AND (${clauses.join(' OR ')})
      ORDER BY as_of_date DESC, id DESC
      LIMIT 200`,
    params
  );
  // Keep most recent per holder
  const byHolder = new Map();
  for (const r of rows) {
    const k = (r.holder_name || '').toLowerCase();
    if (!byHolder.has(k)) byHolder.set(k, r);
  }
  const latest = [...byHolder.values()]
    .filter(r => Number(r.position_pct) > 0)
    .sort((a, b) => Number(b.position_pct) - Number(a.position_pct))
    .slice(0, 15);
  return latest;
}

// --------------------------------------------------------------------------
// 6. Activist / >5% beneficial owners (13D/13G)
// --------------------------------------------------------------------------
async function activistFilersForDeal(deal) {
  const bareSym = String(deal.primary_ticker || deal.target_ticker || deal.spinco_ticker || '').split(':').pop();
  if (!bareSym) return [];
  // 13D/13G are stored in insider_transactions with special source; we also
  // mark them via is_13d flag on beneficial_holders. Union both.
  const rows = await query(
    `SELECT * FROM beneficial_holders
      WHERE is_13d = 1 AND (issuer_ticker LIKE '%' || $1 OR LOWER(issuer_name) LIKE $2)
      ORDER BY as_of_date DESC LIMIT 20`,
    [bareSym, `%${(deal.target_name || deal.spinco_name || '').toLowerCase().split(' ')[0]}%`]
  );
  return rows || [];
}

async function ownershipAndCompForDeal(deal) {
  const [ownership, beneficial, compensation, disclosed_shorts, activists] = await Promise.all([
    ownershipForDeal(deal).catch(() => ({ holders: [], notes: ['lookup failed'] })),
    beneficialHoldersForDeal(deal).catch(() => ({ holders: [], sources: [], notes: ['lookup failed'] })),
    compensationForDeal(deal).catch(() => null),
    disclosedShortsForDeal(deal).catch(() => []),
    activistFilersForDeal(deal).catch(() => []),
  ]);
  const concentration = concentrationFromHolders(beneficial.holders);
  return { ownership, beneficial, concentration, compensation, disclosed_shorts, activists };
}

module.exports = { ownershipForDeal, compensationForDeal, ownershipAndCompForDeal };
