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

async function fetchMergerProxies({ since, enrich = true } = {}) {
  const startdt = since || daysAgo(365);
  const enddt = today();
  const hits = await efts(['DEFM14A', 'PREM14A', 'SC 14D9'], { startdt, enddt });
  const deals = hits.map(normaliseHit).map(h => {
    const d = makeDeal(h, 'merger_pending', 'sec_defm14a');
    d.summary = `Merger filing ${h.form} on EDGAR ${h.file_date}.`;
    d.filing_date = h.file_date;
    d.announce_date_source = 'filing_date';
    // Preserve the raw filing accession so enrichment can fetch the body
    d._accession = h.accession;
    d._form = h.form;
    return d;
  });

  // Enrichment: walk back to real announce date and extract offer terms.
  // Opt-out via enrich=false for fast / test runs.
  if (enrich) {
    // Concurrency: EDGAR's fair-use policy is ~10 req/sec. We stay well under
    // by limiting to 3 concurrent enrichment tasks — each does 1-2 fetches.
    await limitedParallel(deals, 3, async (d) => {
      try { await enrichMergerDeal(d); }
      catch (e) { /* swallow per-deal enrichment errors; keep base deal */ }
    });
  }
  // Drop internal-only fields before returning to caller.
  deals.forEach(d => { delete d._accession; delete d._form; });
  return deals;
}

// ---- Merger enrichment ---------------------------------------------------

// For a merger deal filed as DEFM14A/PREM14A, walk backwards through the
// filer's SEC submissions to find the true deal-announcement date. Deal
// announcements almost always come via either:
//   1. 8-K Item 1.01 "Entry into a Material Definitive Agreement" (the
//      merger agreement is an exhibit). This is THE canonical announce date.
//   2. DEFA14A "Additional Proxy Soliciting Materials" — typically a press
//      release filed the same day as or shortly after the 8-K.
// We take the earliest such filing in a 180-day window before the proxy.
async function findTrueAnnounceDate(cik, beforeDate) {
  if (!cik) return null;
  const cikPad = String(cik).padStart(10, '0');
  const url = `https://data.sec.gov/submissions/CIK${cikPad}.json`;
  const res = await fetch(url, { headers: { 'User-Agent': UA, 'Accept': 'application/json' } });
  if (!res.ok) return null;
  const j = await res.json();
  const recent = j.filings?.recent;
  if (!recent) return null;
  const forms = recent.form || [];
  const dates = recent.filingDate || [];
  const items = recent.items || []; // 8-K items like "1.01"
  const cutoffLo = new Date(beforeDate); cutoffLo.setDate(cutoffLo.getDate() - 180);
  const cutoffHi = new Date(beforeDate); cutoffHi.setDate(cutoffHi.getDate() - 1); // at least 1 day before
  const cutoffLoS = cutoffLo.toISOString().slice(0,10);
  const cutoffHiS = cutoffHi.toISOString().slice(0,10);

  const candidates = [];
  for (let i = 0; i < forms.length; i++) {
    const f = forms[i]; const dt = dates[i]; const it = items[i] || '';
    if (!f || !dt) continue;
    if (dt < cutoffLoS || dt > cutoffHiS) continue;
    if (f === '8-K' && it.includes('1.01')) candidates.push({ date: dt, source: 'sec_8k_101' });
    else if (f === 'DEFA14A') candidates.push({ date: dt, source: 'sec_defa14a' });
  }
  if (!candidates.length) return null;
  // Earliest wins — typically the 8-K beats DEFA14A by 0-1 days.
  candidates.sort((a, b) => a.date.localeCompare(b.date));
  return candidates[0];
}

// Heuristic extractor for per-share merger consideration from the primary
// proxy document. EDGAR filing index lists all attachments; we pick the main
// .htm (largest HTML file) and regex a size-capped slice of the body.
//
// This is deliberately conservative: we'd rather return NULL than a wrong
// number. Missed extractions show "—" in the UI; wrong ones would mislead arb.
async function extractOfferTerms(accession, cik) {
  if (!accession || !cik) return null;
  const raw = String(accession).replace(/-/g, '');
  const cikPlain = String(cik).replace(/^0+/, '') || '0';
  const indexJson = `https://www.sec.gov/Archives/edgar/data/${cikPlain}/${raw}/index.json`;

  let mainDoc = null;
  try {
    const r = await fetch(indexJson, { headers: { 'User-Agent': UA, 'Accept': 'application/json' } });
    if (!r.ok) return null;
    const j = await r.json();
    const items = j.directory?.item || [];
    // Pick the largest .htm/.html that is NOT an exhibit (ex-XX, ex_) —
    // the main proxy is typically the first large HTM listed.
    const htmls = items.filter(it => /\.htm?$/i.test(it.name) && !/^ex[-_]/i.test(it.name) && !/^(R\d+|Financial)/.test(it.name));
    htmls.sort((a, b) => Number(b.size || 0) - Number(a.size || 0));
    mainDoc = htmls[0]?.name;
  } catch { return null; }
  if (!mainDoc) return null;

  const docUrl = `https://www.sec.gov/Archives/edgar/data/${cikPlain}/${raw}/${mainDoc}`;
  let html;
  try {
    const r = await fetch(docUrl, { headers: { 'User-Agent': UA, 'Accept': 'text/html' } });
    if (!r.ok) return null;
    // Merger consideration is almost always discussed in the summary/
    // "The Merger" section near the top. Cap at 800KB to avoid whole-proxy loads.
    const buf = await r.arrayBuffer();
    html = Buffer.from(buf.slice(0, 800_000)).toString('utf8');
  } catch { return null; }

  // Strip tags, decode common entities (incl. non-breaking space &#160;),
  // collapse whitespace for easier regex matching.
  const text = html
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;|&#160;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&#8220;|&#8221;|&ldquo;|&rdquo;/g, '"')
    .replace(/&#8217;|&#8216;|&rsquo;|&lsquo;/g, "'")
    .replace(/\s+/g, ' ');

  // Focus on the "merger consideration" vicinity (first 60KB of stripped text).
  // Patterns observed in real DEFM14As:
  //   "$X.XX in cash per share"
  //   "$X.XX per share of Company Common Stock"
  //   "right to receive $X.XX in cash, without interest"
  //   "0.14625 shares of K-C common stock plus $3.50 in cash (the merger consideration)"
  const window = text.slice(0, 60_000);

  // Cash-per-share patterns. Use /g flag + matchAll so we can iterate all matches
  // and skip par-value noise (first match is often "$0.01 per share" par-value boilerplate).
  // Order from strictest (highest confidence) → loosest.
  const cashPatterns = [
    /(?:right|entitled)\s+to\s+receive\s+\$(\d{1,4}(?:\.\d{1,4})?)\s+in\s+cash(?:,?\s+without\s+interest)?(?:\s+net\s+of)?/gi,
    /\$(\d{1,4}(?:\.\d{1,4})?)\s+(?:in\s+cash\s+)?per\s+share(?:\s+of\s+(?:common|ordinary|class))?/gi,
    /(?:a\s+cash\s+payment\s+of|cash\s+consideration\s+of|per-?share\s+merger\s+consideration\s+of)\s+\$(\d{1,4}(?:\.\d{1,4})?)/gi,
    // bidirectional: merger consideration near $X in cash (either side)
    /merger\s+consideration[^.]{0,80}?\$(\d{1,4}(?:\.\d{1,4})?)\s+in\s+cash/gi,
    /\$(\d{1,4}(?:\.\d{1,4})?)\s+in\s+cash[^.]{0,80}?merger\s+consideration/gi,
    // "plus $X.XX in cash" in merger-consideration context
    /(?:plus|and)\s+\$(\d{1,4}(?:\.\d{1,4})?)\s+in\s+cash/gi,
  ];

  const isValidCashMatch = (m, win) => {
    const v = parseFloat(m[1]);
    if (!(v >= 0.5 && v < 10_000)) return false;
    // Reject if "par value" appears in 40 chars before (par-value boilerplate like "$0.01 par value per share")
    const pre = win.slice(Math.max(0, m.index - 40), m.index);
    if (/par\s+value/i.test(pre)) return false;
    // Reject obvious non-per-share dollar amounts (termination fees, aggregate values)
    const post = win.slice(m.index, Math.min(win.length, m.index + 80));
    if (/(?:termination\s+fee|billion|million\s*,|aggregate)/i.test(post)) return false;
    return true;
  };

  let offerCash = null;
  outer: for (const p of cashPatterns) {
    for (const m of window.matchAll(p)) {
      if (isValidCashMatch(m, window)) { offerCash = parseFloat(m[1]); break outer; }
    }
  }

  // Stock exchange ratio pattern:
  //   "X.XX shares of Parent Common Stock for each share"
  //   "exchange ratio of X.XX"
  let stockRatio = null;
  const stockPatterns = [
    /(\d+(?:\.\d{1,6})?)\s+shares?\s+of\s+(?:Parent|Acquirer|[A-Z][\w ]{2,40})\s+common\s+stock\s+(?:for\s+each|per)/i,
    /exchange\s+ratio\s+of\s+(\d+(?:\.\d{1,6})?)/i,
  ];
  for (const p of stockPatterns) {
    const m = window.match(p);
    if (m) { const v = parseFloat(m[1]); if (v > 0 && v < 1000) { stockRatio = v; break; } }
  }

  let type = 'unknown';
  if (offerCash && stockRatio) type = 'mixed';
  else if (offerCash) type = 'cash';
  else if (stockRatio) type = 'stock';

  // --- Expected close date -------------------------------------------
  // "expected to close in the [first/second/third/fourth] quarter of 2025"
  // "expected to be completed in [Q1/Q2/Q3/Q4] 2025"
  // "targeted to close by [month] [day], [year]" / "on or about [date]"
  let expectedClose = null;
  const qMap = { first: '03-31', second: '06-30', third: '09-30', fourth: '12-31',
                 q1: '03-31', q2: '06-30', q3: '09-30', q4: '12-31' };
  const quarterYear = window.match(/expected\s+to\s+(?:close|be\s+completed|be\s+consummated)[^.]{0,120}?(?:in\s+the\s+)?(first|second|third|fourth|Q1|Q2|Q3|Q4)\s+(?:fiscal\s+)?quarter\s+of\s+(20\d{2})/i)
                     || window.match(/(?:close|be\s+completed|be\s+consummated)[^.]{0,80}?(Q1|Q2|Q3|Q4)\s+(20\d{2})/i);
  if (quarterYear) {
    const q = qMap[quarterYear[1].toLowerCase()];
    if (q) expectedClose = `${quarterYear[2]}-${q}`;
  }
  if (!expectedClose) {
    // "targeted to close on or about March 31, 2025"
    const monthDay = window.match(/(?:expected|targeted|anticipated|projected)\s+to\s+(?:close|be\s+completed|be\s+consummated)[^.]{0,80}?(?:on\s+or\s+about\s+)?((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+20\d{2})/i);
    if (monthDay) {
      const dt = new Date(monthDay[1]);
      if (!isNaN(dt)) expectedClose = dt.toISOString().slice(0, 10);
    }
  }
  if (!expectedClose) {
    // "second half of 2025" → 2025-12-31 (end of period)
    const half = window.match(/expected\s+to\s+(?:close|be\s+completed)[^.]{0,80}?(first|second)\s+half\s+of\s+(20\d{2})/i);
    if (half) expectedClose = half[1].toLowerCase() === 'first' ? `${half[2]}-06-30` : `${half[2]}-12-31`;
  }

  // --- Deal value (aggregate transaction size) -----------------------
  // "transaction is valued at approximately $4.5 billion"
  // "total equity value of approximately $3.2 billion"
  // "aggregate consideration of approximately $1.8 billion"
  // "enterprise value of approximately $2.1 billion"
  let dealValueUsd = null;
  const dvPatterns = [
    /(?:transaction|deal|merger)\s+(?:is\s+)?valued\s+at\s+approximately\s+\$(\d+(?:\.\d+)?)\s+(billion|million)/i,
    /(?:total|aggregate)\s+(?:equity|consideration|transaction|purchase)\s+(?:value|price|consideration)\s+of\s+(?:approximately\s+)?\$(\d+(?:\.\d+)?)\s+(billion|million)/i,
    /enterprise\s+value\s+of\s+(?:approximately\s+)?\$(\d+(?:\.\d+)?)\s+(billion|million)/i,
    /all-cash\s+transaction\s+(?:valued|worth)\s+at\s+approximately\s+\$(\d+(?:\.\d+)?)\s+(billion|million)/i,
  ];
  for (const p of dvPatterns) {
    const m = window.match(p);
    if (m) {
      const v = parseFloat(m[1]);
      const mul = /billion/i.test(m[2]) ? 1e9 : 1e6;
      if (isFinite(v) && v > 0 && v < 10000) { dealValueUsd = v * mul; break; }
    }
  }

  // --- Acquirer name ------------------------------------------------
  // "MERGER AGREEMENT BY AND AMONG ... [AcquirerCo], Inc., ... [Target], Inc."
  // or "will be acquired by [AcquirerCo]"
  // Very naive — only capture when a clear pattern exists.
  let acquirerName = null;
  const acqMatch = window.match(/will\s+be\s+acquired\s+by\s+([A-Z][\w,.\s&-]{2,80}?)(?:\s+\(|\s+in\s+a|\s+for\s+approximately|\s+for\s+\$|\.)/);
  if (acqMatch) acquirerName = acqMatch[1].trim().replace(/\s+/g, ' ').replace(/,$/, '');

  if (type === 'unknown' && !expectedClose && !dealValueUsd && !acquirerName) return null;

  return {
    consideration_type: type === 'unknown' ? null : type,
    consideration_cash: offerCash,
    consideration_stock_ratio: stockRatio,
    // For pure-cash deals we can set offer_price directly. Mixed/stock need
    // an acquirer reference price which we'll resolve in market_data.js.
    offer_price: type === 'cash' ? offerCash : null,
    expected_close_date: expectedClose,
    deal_value_usd: dealValueUsd,
    acquirer_name: acquirerName,
  };
}

async function enrichMergerDeal(d) {
  const cik = d.source_cik;
  const prem = d.filing_date || d.announce_date;
  if (!cik || !prem) return;

  const [announce, terms] = await Promise.all([
    findTrueAnnounceDate(cik, prem).catch(() => null),
    extractOfferTerms(d._accession, cik).catch(() => null),
  ]);

  if (announce) {
    d.announce_date = announce.date;
    d.announce_date_source = announce.source;
    d.key_dates = { ...(d.key_dates || {}), announce_date: announce.date, filing_date: prem };
  }
  if (terms) {
    if (terms.consideration_type) d.consideration_type = terms.consideration_type;
    if (terms.consideration_cash != null) d.consideration_cash = terms.consideration_cash;
    if (terms.consideration_stock_ratio != null) d.consideration_stock_ratio = terms.consideration_stock_ratio;
    if (terms.offer_price != null) d.offer_price = terms.offer_price;
    if (terms.expected_close_date) {
      d.expected_close_date = terms.expected_close_date;
      d.key_dates = { ...(d.key_dates || {}), expected_close_date: terms.expected_close_date };
    }
    if (terms.deal_value_usd) d.deal_value_usd = terms.deal_value_usd;
    if (terms.acquirer_name && !d.acquirer_name) d.acquirer_name = terms.acquirer_name;
    // Human-readable consideration string for the drawer
    if (terms.consideration_type === 'cash' && terms.consideration_cash) d.consideration = `$${terms.consideration_cash.toFixed(2)} cash per share`;
    else if (terms.consideration_type === 'stock' && terms.consideration_stock_ratio) d.consideration = `${terms.consideration_stock_ratio} shares per share`;
    else if (terms.consideration_type === 'mixed' && terms.consideration_cash && terms.consideration_stock_ratio) d.consideration = `$${terms.consideration_cash.toFixed(2)} cash + ${terms.consideration_stock_ratio} shares per share`;
  }
}

// Simple concurrency limiter: runs `fn(item)` over all items with at most
// `n` concurrent calls in flight. Preserves original order; does not throw.
async function limitedParallel(items, n, fn) {
  const results = new Array(items.length);
  let next = 0;
  async function worker() {
    while (next < items.length) {
      const i = next++;
      try { results[i] = await fn(items[i], i); }
      catch (e) { results[i] = null; }
    }
  }
  await Promise.all(Array.from({ length: Math.min(n, items.length) }, () => worker()));
  return results;
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
  extractOfferTerms,  // exported for backfill admin endpoint
  enrichMergerDeal,
};
