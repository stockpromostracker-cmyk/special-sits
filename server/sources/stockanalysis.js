// StockAnalysis.com aggregator source — clean HTML tables of completed US
// spin-offs and recent IPOs. Used as SECONDARY (enrichment):
//   - Cross-checks completed spin-offs the SEC 8-K pass may have missed
//   - Provides IPO price + current price for return calc
//   - Fills in ex_date / completed_date cleanly
//
// data_source_tier for these is 'aggregator' and confidence=0.7. If a
// matching SEC filing is found during rollup the tier upgrades to 'official'.

const UA = 'Mozilla/5.0 (compatible; SpecialSits/1.0)';

async function fetchHtml(url) {
  const res = await fetch(url, { headers: { 'User-Agent': UA, 'Accept': 'text/html' } });
  if (!res.ok) throw new Error(`${url} → ${res.status}`);
  return await res.text();
}

// Extract rows from a stockanalysis.com data-table. They render server-side
// as <table>…<tr><td>cell</td></tr></table> so regex is robust enough.
function parseTable(html, expectedCols) {
  // Find the first large <table> block after the <h1>
  const tableMatch = html.match(/<table[^>]*>([\s\S]*?)<\/table>/i);
  if (!tableMatch) return [];
  const body = tableMatch[1];
  const rows = [];
  const trRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  let m;
  while ((m = trRe.exec(body)) !== null) {
    const tdRe = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
    const cells = [];
    let c;
    while ((c = tdRe.exec(m[1])) !== null) {
      cells.push(stripHtml(c[1]).trim());
    }
    if (cells.length >= expectedCols) rows.push(cells);
  }
  // First row is the header
  return rows.slice(1);
}

function stripHtml(s) {
  return String(s || '')
    .replace(/<br\s*\/?>/gi, ' ')
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/\s+/g, ' ')
    .trim();
}

// Parse a header-style date like "Feb 26, 2026" → "2026-02-26"
function parseDate(s) {
  if (!s) return null;
  const clean = String(s).trim();
  const d = new Date(clean);
  if (isNaN(d)) return null;
  return d.toISOString().slice(0, 10);
}

// ---- Public API ----------------------------------------------------------

async function fetchSpinoffs() {
  const html = await fetchHtml('https://stockanalysis.com/actions/spinoffs/');
  const rows = parseTable(html, 5);
  const out = [];
  for (const [date, parent, newSym, parentCo, newCo] of rows) {
    if (!date || !newSym) continue;
    const completed = parseDate(date);
    if (!completed) continue;
    const today = new Date();
    const compDate = new Date(completed);
    const daysSince = Math.round((today - compDate) / 86400000);
    out.push({
      event_type: 'spin_off_completed',
      data_source_tier: 'aggregator',
      primary_source: 'stockanalysis_spin',
      source_filing_url: 'https://stockanalysis.com/actions/spinoffs/',
      confidence: 0.7,
      deal_type: 'spin_off',
      status: 'completed',
      region: 'US',
      country: 'US',
      headline: `${newCo} (${newSym}) spun off from ${parentCo} (${parent})`,
      summary: `${newCo} began trading as an independent public company on ${completed}, spun off from ${parentCo}.`,
      parent_name: parentCo,
      parent_ticker: parent,
      spinco_name: newCo,
      spinco_ticker: newSym,
      primary_ticker: newSym,
      completed_date: completed,
      announce_date: completed,
      ex_date: completed,
      key_dates: { ex_date: completed, completed_date: completed },
      days_since_event: daysSince >= 0 ? daysSince : null,
      external_key: `sa_spin:${parent}:${newSym}:${completed}`,
    });
  }
  return out;
}

// SPAC shell detection: issuer name or symbol pattern indicates blank-check company.
// These clog the IPO feed with ~22 "no current price" rows because Yahoo/stockanalysis
// don't always carry SPAC warrants/units.
function looksLikeSpac(companyName, symbol) {
  const n = String(companyName || '').toLowerCase();
  const s = String(symbol || '').toUpperCase();
  if (/\bacquisition\s+(corp|company|limited|plc|inc)\b/.test(n)) return true;
  if (/\bcapital\s+acquisition\b/.test(n)) return true;
  if (/\bblank[-\s]?check\b/.test(n)) return true;
  if (/\bSPAC\b/i.test(n)) return true;
  // Common SPAC symbol patterns: ACAC, ACACU, ACACW (units/warrants)
  if (/^[A-Z]{2,5}[UW]$/.test(s) && /\bacquisition\b/.test(n)) return true;
  return false;
}

async function fetchIpos() {
  const html = await fetchHtml('https://stockanalysis.com/ipos/');
  const rows = parseTable(html, 5);
  const out = [];
  for (const [date, symbol, company, ipoPriceRaw, currentRaw, returnRaw] of rows) {
    if (!date || !symbol) continue;
    const ipoDate = parseDate(date);
    if (!ipoDate) continue;
    const ipoPrice = parseFloat(String(ipoPriceRaw || '').replace(/[^\d.-]/g, ''));
    const current = parseFloat(String(currentRaw || '').replace(/[^\d.-]/g, ''));
    const today = new Date();
    const ipoDt = new Date(ipoDate);
    const daysSince = Math.round((today - ipoDt) / 86400000);
    // Only keep IPOs from the last 180d (beyond that they're not "recent")
    if (daysSince > 180 || daysSince < 0) continue;
    const isSpac = looksLikeSpac(company, symbol);
    out.push({
      event_type: 'ipo_recent',
      data_source_tier: 'aggregator',
      primary_source: 'stockanalysis_ipo',
      source_filing_url: `https://stockanalysis.com/stocks/${String(symbol).toLowerCase()}/`,
      confidence: 0.7,
      deal_type: isSpac ? 'spac' : 'ipo',
      is_spac: isSpac || undefined,
      status: 'completed',
      region: 'US',
      country: 'US',
      headline: `${company} (${symbol}) IPO'd at $${isFinite(ipoPrice) ? ipoPrice.toFixed(2) : '—'}`,
      summary: `${company} completed its initial public offering on ${ipoDate}.`,
      target_name: company,
      target_ticker: symbol,
      primary_ticker: symbol,
      completed_date: ipoDate,
      announce_date: ipoDate,
      ipo_price: isFinite(ipoPrice) ? ipoPrice : null,
      announce_price: isFinite(ipoPrice) ? ipoPrice : null,
      current_price: isFinite(current) ? current : null,
      key_dates: { first_trade_date: ipoDate, completed_date: ipoDate },
      days_since_event: daysSince,
      external_key: `sa_ipo:${symbol}:${ipoDate}`,
    });
  }
  return out;
}

async function fetchAll() {
  const tasks = await Promise.allSettled([fetchSpinoffs(), fetchIpos()]);
  const [s, i] = tasks;
  const result = { spinoffs: 0, ipos: 0, deals: [] };
  if (s.status === 'fulfilled') { result.spinoffs = s.value.length; result.deals.push(...s.value); }
  else console.error('[stockanalysis] spinoffs failed:', s.reason?.message);
  if (i.status === 'fulfilled') { result.ipos = i.value.length; result.deals.push(...i.value); }
  else console.error('[stockanalysis] ipos failed:', i.reason?.message);
  return result;
}

module.exports = { fetchAll, fetchSpinoffs, fetchIpos, parseTable, stripHtml, parseDate };
