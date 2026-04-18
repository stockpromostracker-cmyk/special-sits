// SEC EDGAR authoritative source — the ground truth for US events.
// Uses the public efts.sec.gov full-text search API and the submissions.json
// endpoint. No API key required; just a polite User-Agent.
//
// Coverage:
//   - Form 10-12B / 10-12B/A → pending US spin-offs (registration statement)
//   - Form 8-K item 2.01     → completed M&A / spin-off distribution (detected heuristically)
//   - Form S-1 / S-1/A       → IPO pipeline (registration statement)
//   - Form 424B4             → IPO priced (final prospectus)
//   - Form DEFM14A           → merger proxy (shareholder vote)
//   - Form SC 14D9           → tender offer recommendation
//
// Every filing becomes a candidate deal with data_source_tier='official',
// confidence=1.0, a source_filing_url, and known structured fields.

const UA = 'SpecialSits Research contact@special-sits.local';

async function efts(forms, { startdt, enddt, q = '' } = {}) {
  const u = new URL('https://efts.sec.gov/LATEST/search-index');
  if (q) u.searchParams.set('q', q);
  u.searchParams.set('forms', Array.isArray(forms) ? forms.join(',') : forms);
  if (startdt && enddt) {
    u.searchParams.set('dateRange', 'custom');
    u.searchParams.set('startdt', startdt);
    u.searchParams.set('enddt', enddt);
  }
  const res = await fetch(u.toString(), { headers: { 'User-Agent': UA, 'Accept': 'application/json' } });
  if (!res.ok) throw new Error(`EDGAR ${forms} ${res.status}`);
  const j = await res.json();
  return j.hits?.hits?.map(h => h._source).filter(Boolean) || [];
}

// Parse EDGAR display_names field → { company, ticker, cik }
function parseDisplayName(display) {
  // Formats seen:
  //   "FedEx Freight Holding Company, Inc.  (FDXF)  (CIK 0002082247)"
  //   "1 Finax AI Technologies, Inc  (CIK 0002122965)"           ← no ticker
  const s = String(display || '').trim();
  const cik = (s.match(/CIK\s+(\d{4,10})/) || [])[1] || null;
  const tickerMatch = s.match(/\(\s*([A-Z0-9][A-Z0-9.\-]{0,9})\s*\)\s*\(CIK/);
  const ticker = tickerMatch ? tickerMatch[1] : null;
  const company = s.replace(/\s*\([A-Z0-9.\-]+\)\s*\(CIK[^)]+\)\s*$/, '')
                   .replace(/\s*\(CIK[^)]+\)\s*$/, '')
                   .trim();
  return { company, ticker, cik };
}

function accessionToUrl(accession, cik) {
  // accession like "0001104659-26-041977"; URL format drops the dashes
  const raw = String(accession || '').replace(/-/g, '');
  const cikPlain = String(cik || '').replace(/^0+/, '') || '0';
  return `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cikPlain}&type=&dateb=&owner=include&count=40`
      || `https://www.sec.gov/Archives/edgar/data/${cikPlain}/${raw}/`;
}

function filingIndexUrl(accession, cik) {
  const raw = String(accession || '').replace(/-/g, '');
  const cikPlain = String(cik || '').replace(/^0+/, '') || '0';
  return `https://www.sec.gov/Archives/edgar/data/${cikPlain}/${raw}/`;
}

function normaliseHit(h) {
  const display = Array.isArray(h.display_names) ? h.display_names[0] : h.display_names;
  const { company, ticker, cik } = parseDisplayName(display);
  return {
    form: h.form,
    file_date: h.file_date,
    accession: h.adsh,
    company,
    ticker,
    cik,
    filing_url: filingIndexUrl(h.adsh, cik),
  };
}

// Map an EDGAR filing into a "candidate deal" row for our upsert pipeline.
function makeDeal({ form, file_date, accession, company, ticker, cik, filing_url }, eventType, primarySource) {
  return {
    event_type: eventType,
    data_source_tier: 'official',
    primary_source: primarySource,
    source_filing_url: filing_url,
    source_cik: cik,
    confidence: 1.0,
    filing_date: file_date,
    key_dates: { filing_date: file_date },

    // Fill the legacy columns so existing UI still works
    deal_type: eventType.startsWith('spin_off') ? 'spin_off'
             : eventType.startsWith('ipo') ? 'ipo'
             : eventType.startsWith('merger') ? 'merger_arb'
             : 'other',
    status: eventType.endsWith('_completed') || eventType === 'ipo_recent' ? 'completed' : 'announced',
    region: 'US',
    country: 'US',
    headline: eventTypeHeadline(eventType, company, ticker, form),
    summary: `Filed ${form} with the SEC on ${file_date}.`,

    // Identity fields
    // For 10-12B the filer IS the spin-off entity (SpinCo), so populate spinco_*.
    // For S-1/424B4 the filer is the newly-issuing company → populate target_* as the IPO entity.
    // For DEFM14A the filer is the target of the merger.
    ...identityFields(eventType, company, ticker),

    // Stable external id for idempotency:
    external_key: `sec:${accession}`,
    announce_date: file_date,
  };
}

function eventTypeHeadline(eventType, company, ticker, form) {
  const t = ticker ? ` (${ticker})` : '';
  switch (eventType) {
    case 'spin_off_pending':   return `${company}${t} — spin-off registration (${form})`;
    case 'spin_off_completed': return `${company}${t} — spin-off distribution (8-K)`;
    case 'ipo_pending':        return `${company}${t} — IPO registration (${form})`;
    case 'ipo_recent':         return `${company}${t} — IPO priced (${form})`;
    case 'merger_pending':     return `${company}${t} — merger proxy (${form})`;
    default:                   return `${company}${t} — ${form}`;
  }
}

function identityFields(eventType, company, ticker) {
  if (eventType.startsWith('spin_off')) {
    return { spinco_name: company, spinco_ticker: ticker, primary_ticker: ticker };
  }
  if (eventType.startsWith('ipo')) {
    return { target_name: company, target_ticker: ticker, primary_ticker: ticker };
  }
  if (eventType.startsWith('merger')) {
    return { target_name: company, target_ticker: ticker, primary_ticker: ticker };
  }
  return { target_name: company, target_ticker: ticker, primary_ticker: ticker };
}

// ---- Public API ----------------------------------------------------------

// Default lookback: 24 months for registration statements (they can sit for
// 12-18 months before the transaction completes), 12 months for priced/closed
// events. Pass `since` as YYYY-MM-DD to override.
async function fetchSpinoffPipeline({ since } = {}) {
  const startdt = since || daysAgo(730);  // 24 months — e.g. Amrize 10-12B was mid-2024
  const enddt = today();
  const hits = await efts(['10-12B', '10-12B/A'], { startdt, enddt });
  return hits.map(normaliseHit).map(h => makeDeal(h, 'spin_off_pending', h.form === '10-12B/A' ? 'sec_10_12b_a' : 'sec_10_12b'));
}

// Completed spin-offs: 8-K with Item 2.01 ("Completion of Acquisition or
// Disposition of Assets") that specifically mentions "spin-off" or
// "distribution". Narrower than fetching all 8-Ks.
async function fetchSpinoffCompleted({ since } = {}) {
  const startdt = since || daysAgo(730);
  const enddt = today();
  const hits = await efts(['8-K'], { startdt, enddt, q: '"spin-off" "Item 2.01"' });
  return hits.map(normaliseHit).map(h => {
    const d = makeDeal(h, 'spin_off_completed', 'sec_8k_201');
    d.completed_date = h.file_date;
    d.key_dates = { ...(d.key_dates || {}), completed_date: h.file_date };
    return d;
  });
}

async function fetchIpoPipeline({ since } = {}) {
  const startdt = since || daysAgo(365);
  const enddt = today();
  const hits = await efts(['S-1', 'S-1/A', 'F-1', 'F-1/A'], { startdt, enddt });
  return hits.map(normaliseHit).map(h => makeDeal(h, 'ipo_pending', 'sec_s1'));
}

async function fetchIpoPriced({ since } = {}) {
  const startdt = since || daysAgo(365);
  const enddt = today();
  // 424B4 is filed on/just after IPO pricing. Narrowest IPO signal.
  const hits = await efts(['424B4', '424B1'], { startdt, enddt });
  return hits.map(normaliseHit).map(h => makeDeal(h, 'ipo_recent', 'sec_424b4'));
}

async function fetchMergerProxies({ since } = {}) {
  const startdt = since || daysAgo(365);
  const enddt = today();
  const hits = await efts(['DEFM14A', 'PREM14A', 'SC 14D9'], { startdt, enddt });
  return hits.map(normaliseHit).map(h => {
    const d = makeDeal(h, 'merger_pending', 'sec_defm14a');
    d.summary = `Merger filing ${h.form} on EDGAR ${h.file_date}.`;
    return d;
  });
}

async function fetchAll({ since } = {}) {
  const tasks = await Promise.allSettled([
    fetchSpinoffPipeline({ since }),
    fetchSpinoffCompleted({ since }),
    fetchIpoPipeline({ since }),
    fetchIpoPriced({ since }),
    fetchMergerProxies({ since }),
  ]);
  const results = { spinoff_pending: 0, spinoff_completed: 0, ipo_pending: 0, ipo_priced: 0, merger_pending: 0, deals: [] };
  const [spin, spinC, ipoP, ipoR, m] = tasks;
  if (spin.status === 'fulfilled') { results.spinoff_pending = spin.value.length; results.deals.push(...spin.value); }
  else console.error('[sec] spinoff pipeline failed:', spin.reason?.message);
  if (spinC.status === 'fulfilled') { results.spinoff_completed = spinC.value.length; results.deals.push(...spinC.value); }
  else console.error('[sec] spinoff completed failed:', spinC.reason?.message);
  if (ipoP.status === 'fulfilled') { results.ipo_pending = ipoP.value.length; results.deals.push(...ipoP.value); }
  else console.error('[sec] ipo pipeline failed:', ipoP.reason?.message);
  if (ipoR.status === 'fulfilled') { results.ipo_priced = ipoR.value.length; results.deals.push(...ipoR.value); }
  else console.error('[sec] ipo priced failed:', ipoR.reason?.message);
  if (m.status === 'fulfilled') { results.merger_pending = m.value.length; results.deals.push(...m.value); }
  else console.error('[sec] merger proxies failed:', m.reason?.message);

  // De-dupe by external_key (same filing appearing in multiple passes shouldn't happen,
  // but defensive)
  const seen = new Set();
  results.deals = results.deals.filter(d => {
    if (seen.has(d.external_key)) return false;
    seen.add(d.external_key); return true;
  });
  // Drop deals with no ticker: EDGAR display_names occasionally omits it, but we need
  // a public ticker to be useful. Keep CIK so a later run can retry resolution.
  results.deals_with_ticker = results.deals.filter(d => d.primary_ticker);
  results.deals_without_ticker = results.deals.length - results.deals_with_ticker.length;
  return results;
}

function today()     { return new Date().toISOString().slice(0, 10); }
function daysAgo(n)  { const d = new Date(); d.setDate(d.getDate() - n); return d.toISOString().slice(0, 10); }

module.exports = {
  fetchAll,
  fetchSpinoffPipeline,
  fetchSpinoffCompleted,
  fetchIpoPipeline,
  fetchIpoPriced,
  fetchMergerProxies,
  parseDisplayName,   // exported for tests
};
