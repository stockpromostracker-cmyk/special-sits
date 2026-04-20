// Beneficial-holder ingesters — who actually OWNS the stock.
//
// Sources:
//   sec_13f       — US Form 13F-HR (quarterly institutional holdings)
//   uk_tr1        — UK RNS major-holder (TR-1) notifications
//   nordic_major  — Nasdaq Nordic / OMX company-IR major shareholder tables
//   afm_nl_subst  — Netherlands AFM substantial-holdings register
//   sec_13d_13g   — US activist / >5% beneficial owners (promoted from insider feeds)
//
// All feeds normalize into `beneficial_holders` table.
// Each ingester is best-effort: errors return empty array.

const Parser = require('rss-parser');
const { query } = require('./db');

const UA = 'SpecialSits Research cfrjacobsson@gmail.com';
const BROWSER_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

const parser = new Parser({
  timeout: 25000,
  headers: { 'user-agent': UA, 'accept': 'application/atom+xml, application/rss+xml, application/xml;q=0.9, */*;q=0.8' },
});

// --------------------------------------------------------------------------
// Normalized holder-type inference
// --------------------------------------------------------------------------
// Tag a holder as institutional / index / insider / state / foundation etc.
// Used in the UI for quick visual grouping and in concentration math
// (insider % vs. institutional % vs. free float).
const INDEX_FUND_PATTERNS = [
  /\bishares\b/i, /\bspdr\b/i, /\bvanguard\b/i, /\bstate street\b/i,
  /\bblackrock\b/i, /\bamundi\b/i, /\bxtrackers\b/i, /\blyxor\b/i,
  /\binvesco\s+etf\b/i, /\bnorthern\s+trust\b/i, /\bdws\b/i,
];
const STATE_PATTERNS = [
  /\bfolketrygdfondet\b/i, /\bnorges\s+bank\b/i, /\bafp\b/i,
  /\bgovernment\s+of\b/i, /\bministry\s+of\b/i, /\bsovereign\s+wealth\b/i,
  /\bkingdom\s+of\b/i, /\brepublic\s+of\b/i, /\bkpa\b/i, /\bstate\s+of\b/i,
];
const FOUNDATION_PATTERNS = [
  /\bstiftelse\b/i, /\bfoundation\b/i, /\bfundacion\b/i, /\bfondation\b/i,
  /\btrust\b/i, /\bstiftung\b/i,
];

function inferHolderType(name, raw) {
  const n = String(name || '');
  const r = String(raw || '').toLowerCase();
  if (INDEX_FUND_PATTERNS.some(p => p.test(n))) return 'index_fund';
  if (STATE_PATTERNS.some(p => p.test(n))) return 'state';
  if (FOUNDATION_PATTERNS.some(p => p.test(n))) return 'foundation';
  if (/\b(capital|asset|investment|fund|advisors|management|partners|llc|lp|limited|gmbh|ab|sa|nv|plc|pension|endowment)\b/i.test(n))
    return 'institutional';
  if (r.includes('insider') || r.includes('director') || r.includes('officer') || r.includes('executive')) return 'insider';
  if (/\b(chairman|ceo|cfo|founder)\b/i.test(n)) return 'insider';
  // Default: natural-person pattern = insider, corporate = institutional
  const words = n.trim().split(/\s+/).filter(Boolean);
  if (words.length <= 3 && words.every(w => /^[A-ZÅÄÖØÆÜa-zåäöøæü.'\-]+$/.test(w))) return 'insider';
  return 'institutional';
}

// --------------------------------------------------------------------------
// Upsert helper
// --------------------------------------------------------------------------
async function upsertHolder(h) {
  const holderType = h.holder_type || inferHolderType(h.holder_name, h.raw_holder_type);
  try {
    await query(
      `INSERT INTO beneficial_holders
        (source, issuer_name, issuer_ticker, isin, issuer_country,
         holder_name, holder_type, holder_cik, as_of_date,
         shares, position_pct, value_usd, filing_url, is_13d, raw_holder_type)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
       ON CONFLICT(source, issuer_ticker, isin, holder_name, as_of_date) DO UPDATE SET
         shares = EXCLUDED.shares,
         position_pct = EXCLUDED.position_pct,
         value_usd = EXCLUDED.value_usd,
         filing_url = COALESCE(EXCLUDED.filing_url, beneficial_holders.filing_url),
         holder_type = COALESCE(EXCLUDED.holder_type, beneficial_holders.holder_type),
         is_13d = CASE WHEN EXCLUDED.is_13d = 1 THEN 1 ELSE beneficial_holders.is_13d END,
         raw_holder_type = COALESCE(EXCLUDED.raw_holder_type, beneficial_holders.raw_holder_type)`,
      [h.source, h.issuer_name || null, h.issuer_ticker || null, h.isin || null,
       h.issuer_country || null, h.holder_name, holderType, h.holder_cik || null,
       h.as_of_date, h.shares || null, h.position_pct || null, h.value_usd || null,
       h.filing_url || null, h.is_13d ? 1 : 0, h.raw_holder_type || null]
    );
    return true;
  } catch (e) {
    if (!/duplicate|UNIQUE/i.test(e.message)) console.error('[holders:upsert]', e.message);
    return false;
  }
}

// --------------------------------------------------------------------------
// 1) SEC 13F-HR — US institutional holders (quarterly)
// --------------------------------------------------------------------------
//
// Workflow:
//   a) Given a target CIK, pull the company's "facts" via EDGAR submissions API
//      to get CUSIP/ISIN (not directly available — we instead pull the issuer's
//      shares outstanding and known CUSIP from the 10-K).
//   b) Query EDGAR full-text search for 13F-HR filings containing the CUSIP.
//      Faster alternative: use the `/api/xbrl/` endpoints.
//   c) Parse the information-table XML for each matching 13F.
//
// For our v1, we take a pragmatic shortcut: use the free `stockanalysis.com`
// or `fintel.io` aggregations when CUSIP lookup is cheap, else fall back to
// querying 13F form.primaryDocument.xml per filing. Since we're trying to be
// official-sources-first, we use EDGAR's full-text search.

async function fetchSec13fForIssuer({ cik, cusip, ticker, issuerName }) {
  if (!cusip) return [];
  // EDGAR full-text search — filings of type 13F-HR that mention the CUSIP.
  // This can return hundreds of results; we cap to the latest 20 filings.
  const url = `https://efts.sec.gov/LATEST/search-index?q=%22${cusip}%22&dateRange=custom&startdt=${fiveYearsAgo()}&enddt=${today()}&forms=13F-HR&hits=20`;
  let hits;
  try {
    const r = await fetch(url, { headers: { 'User-Agent': UA, 'Accept': 'application/json' } });
    if (!r.ok) return [];
    const j = await r.json();
    hits = (j.hits && j.hits.hits) || [];
  } catch (e) {
    console.error('[sec_13f:search]', e.message);
    return [];
  }
  // Group by filer CIK, take latest per filer — gives current holdings snapshot.
  const latestByFiler = new Map();
  for (const h of hits) {
    const filerCik = h._source?.ciks?.[0] || h._source?.cik;
    if (!filerCik) continue;
    const filed = h._source?.file_date || h._source?.period_ending;
    if (!filed) continue;
    const prev = latestByFiler.get(filerCik);
    if (!prev || filed > prev.filed) {
      latestByFiler.set(filerCik, {
        filer_cik: filerCik,
        filer_name: h._source?.display_names?.[0] || 'Unknown filer',
        filed,
        accession: h._source?.adsh,
      });
    }
  }

  // For each filer, fetch the information-table XML and extract the row for our CUSIP.
  const results = [];
  const filers = [...latestByFiler.values()];
  // Cap to top 60 filers (13Fs have hundreds; largest holdings tend to come first).
  for (const f of filers.slice(0, 60)) {
    try {
      const row = await fetch13fPosition(f, cusip);
      if (row) {
        results.push({
          source: 'sec_13f',
          issuer_name: issuerName,
          issuer_ticker: ticker,
          issuer_country: 'US',
          holder_name: cleanFilerName(f.filer_name),
          holder_cik: f.filer_cik,
          as_of_date: f.filed,
          shares: row.shares,
          value_usd: row.value_usd,
          filing_url: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${f.filer_cik}&type=13F&dateb=&owner=include&count=40`,
          raw_holder_type: 'institutional_13f',
        });
      }
    } catch (e) {
      // Swallow per-filer failures
    }
  }
  return results;
}

function cleanFilerName(name) {
  return String(name || '')
    .replace(/\s+\(CIK \d+\)/i, '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

async function fetch13fPosition(filer, cusip) {
  // Resolve the 13F-HR primary XML document.
  const accNoDashes = filer.accession.replace(/-/g, '');
  const indexUrl = `https://www.sec.gov/Archives/edgar/data/${Number(filer.filer_cik)}/${accNoDashes}/`;
  let listing;
  try {
    const r = await fetch(indexUrl + 'index.json', { headers: { 'User-Agent': UA, 'Accept': 'application/json' } });
    if (!r.ok) return null;
    listing = await r.json();
  } catch { return null; }
  const items = listing.directory?.item || [];
  // Look for an info-table XML — name often contains 'infotable' or ends in '.xml' and isn't the primary_doc
  const infoTable = items.find(it => /infotable/i.test(it.name) && /\.xml$/i.test(it.name))
                 || items.find(it => /\.xml$/i.test(it.name) && !/primary_doc/i.test(it.name));
  if (!infoTable) return null;
  let xml;
  try {
    const r = await fetch(indexUrl + infoTable.name, { headers: { 'User-Agent': UA } });
    if (!r.ok) return null;
    xml = await r.text();
  } catch { return null; }
  // Find the infoTable block matching our CUSIP. Regex parse — the schema is stable.
  const regex = new RegExp(
    `<infoTable[^>]*>[\\s\\S]*?<cusip>\\s*${cusip.replace(/[^A-Z0-9]/gi, '')}\\s*</cusip>[\\s\\S]*?</infoTable>`,
    'i'
  );
  const match = xml.match(regex);
  if (!match) return null;
  const block = match[0];
  const shares = Number((block.match(/<sshPrnamt>\s*([\d,]+)\s*<\/sshPrnamt>/i) || [])[1]?.replace(/,/g, '') || 0);
  const value = Number((block.match(/<value>\s*([\d,]+)\s*<\/value>/i) || [])[1]?.replace(/,/g, '') || 0);
  if (!shares) return null;
  return {
    shares,
    // 13F value field is in thousands pre-2022 Q4, in dollars post — normalize:
    // Post-2022 Q4 amendment changed <value> units from thousands to dollars.
    // We use the filing date to decide.
    value_usd: filer.filed >= '2023-01-01' ? value : value * 1000,
  };
}

function today()        { return new Date().toISOString().slice(0, 10); }
function fiveYearsAgo() { const d = new Date(); d.setFullYear(d.getFullYear() - 5); return d.toISOString().slice(0, 10); }

// --------------------------------------------------------------------------
// 2) UK RNS — TR-1 major holder notifications
// --------------------------------------------------------------------------
//
// RNS publishes "Holding(s) in Company" notifications whenever a fund crosses
// a 3% / 5% / 10% threshold. We harvest from the LSE news RSS filtered to TR-1.
// This gives us RECENT changes, not a static top-20. Aggregating into a
// current-holder snapshot requires keeping the most-recent position per
// (issuer, holder) — done at query time in the UI.
async function fetchUkTr1({ days = 365 } = {}) {
  // Google News title is too unstructured for reliable parsing. Only keep
  // rows where we can extract BOTH a holder name AND a percentage; otherwise
  // discard to avoid junk in beneficial_holders.
  const q = 'site:londonstockexchange.com "Holding(s) in Company" OR "TR-1"';
  const url = `https://news.google.com/rss/search?q=${encodeURIComponent(q)}&hl=en-GB&gl=GB&ceid=GB:en`;
  try {
    const feed = await parser.parseURL(url);
    const cutoff = new Date(Date.now() - days * 86400 * 1000);
    const items = (feed.items || []).filter(i => {
      const d = new Date(i.pubDate || i.isoDate || 0);
      return d >= cutoff;
    });
    const out = [];
    for (const it of items.slice(0, 40)) {
      const parsed = parseTr1Title(it.title || '');
      if (!parsed || !parsed.pct || !parsed.holder_name || !parsed.issuer_name) continue;
      // Reject rows where issuer_name is a generic RNS prefix.
      if (/^(Holding\(s\) in Company|TR-1|Rule|Form)/i.test(parsed.issuer_name)) continue;
      // Reject holder_name that looks like a timestamp.
      if (/^\d{1,2}:\d{2}/.test(parsed.holder_name)) continue;
      out.push({
        source: 'uk_tr1',
        issuer_name: parsed.issuer_name,
        issuer_country: 'GB',
        holder_name: parsed.holder_name,
        as_of_date: (it.pubDate || it.isoDate || '').slice(0, 10),
        position_pct: parsed.pct,
        filing_url: it.link,
        raw_holder_type: 'uk_tr1',
      });
    }
    return out;
  } catch (e) {
    console.error('[uk_tr1]', e.message);
    return [];
  }
}

// "COMPANY NAME - Holding(s) in Company - BlackRock, Inc. - 5.12%"
function parseTr1Title(title) {
  const t = String(title).replace(/\s+/g, ' ').trim();
  const pctMatch = t.match(/(\d{1,2}\.\d{1,4})\s*%/);
  const pct = pctMatch ? Number(pctMatch[1]) : null;
  // Common LSE format: "COMPANY - Holding(s) in Company"
  const segs = t.split(/\s+[-–]\s+/);
  if (segs.length < 2) return null;
  const issuer_name = segs[0].trim();
  // Holder name is often the 3rd segment or embedded in the body
  let holder_name = null;
  for (const s of segs.slice(1)) {
    if (/holding\(s\) in company|TR-1/i.test(s)) continue;
    if (s.length < 60 && s.length > 2) { holder_name = s.trim(); break; }
  }
  if (!issuer_name || !holder_name) return null;
  return { issuer_name, holder_name, pct };
}

// --------------------------------------------------------------------------
// 3) Netherlands AFM — substantial-holdings register
// --------------------------------------------------------------------------
//
// AFM publishes an open-data file of substantial-holdings notifications
// (>=3% thresholds in Dutch listed companies).
//
// JSON endpoint: https://www.afm.nl/en/sector/registers/meldingenregisters/substantiele-deelnemingen
// The actual data is served via an internal API and requires pagination.
//
// For v1 we scrape the HTML list + fall back to Google News searches
// of "AFM substantial holding" as an early-warning RSS.
async function fetchAfmSubstantial({ days = 365 } = {}) {
  const q = 'site:afm.nl "substantial holding" OR "substantiele deelneming"';
  const url = `https://news.google.com/rss/search?q=${encodeURIComponent(q)}&hl=nl&gl=NL&ceid=NL:nl`;
  try {
    const feed = await parser.parseURL(url);
    const cutoff = new Date(Date.now() - days * 86400 * 1000);
    const items = (feed.items || []).filter(i => {
      const d = new Date(i.pubDate || i.isoDate || 0);
      return d >= cutoff;
    });
    const out = [];
    for (const it of items.slice(0, 40)) {
      const parsed = parseAfmTitle(it.title || '');
      if (!parsed || !parsed.pct || !parsed.holder_name || !parsed.issuer_name) continue;
      // Filter junk: issuer or holder looking like a URL/timestamp/generic word
      if (/^(https?:|AFM|Registers?|\d{1,2}:)/i.test(parsed.issuer_name)) continue;
      if (/^(https?:|AFM|\d{1,2}:)/i.test(parsed.holder_name)) continue;
      out.push({
        source: 'afm_nl_subst',
        issuer_name: parsed.issuer_name,
        issuer_country: 'NL',
        holder_name: parsed.holder_name,
        as_of_date: (it.pubDate || it.isoDate || '').slice(0, 10),
        position_pct: parsed.pct,
        filing_url: it.link,
        raw_holder_type: 'afm_substantial',
      });
    }
    return out;
  } catch (e) {
    console.error('[afm_nl_subst]', e.message);
    return [];
  }
}

function parseAfmTitle(title) {
  const t = String(title).replace(/\s+/g, ' ').trim();
  const pctMatch = t.match(/(\d{1,2}[.,]\d{1,2})\s*%/);
  const pct = pctMatch ? Number(pctMatch[1].replace(',', '.')) : null;
  // AFM titles don't always split cleanly; we do a best-effort
  const m = t.match(/^(.+?)\s*[-–]\s*(.+?)(?:\s*[-–]\s*(.+?))?$/);
  if (!m) return null;
  return { issuer_name: (m[2] || '').trim(), holder_name: (m[1] || '').trim(), pct };
}

// --------------------------------------------------------------------------
// 4) Nordic major-shareholder tables (Swedish/Finnish/Danish/Norwegian)
// --------------------------------------------------------------------------
//
// Unlike the US, Nordic regulators don't publish a machine-readable
// substantial-holdings register. Instead, top-holder tables live on:
//   - Company IR pages (updated monthly, via Monitor by Modular Finance)
//   - Nasdaq Nordic market-making sheets
//   - Euroclear Sweden (behind a paywall)
//
// For v1 we use the Holdings.se public pages (which license the Modular
// Finance data) and Avanza's public shareholder pages for SE issuers.
// The ingester takes a ticker list and fetches each.
async function fetchNordicMajorHolders(tickers = []) {
  const out = [];
  for (const t of tickers) {
    // Expect STO:SYMBOL or HEL:SYMBOL or CPH:SYMBOL or OSL:SYMBOL
    const [exch, sym] = String(t).split(':');
    if (!exch || !sym) continue;
    // Avanza public shareholder page (Swedish only for now)
    if (exch === 'STO') {
      const rows = await fetchAvanzaShareholders(sym);
      out.push(...rows.map(r => ({ ...r, issuer_ticker: t })));
    }
  }
  return out;
}

async function fetchAvanzaShareholders(symbol) {
  // Avanza page pattern: https://www.avanza.se/aktier/om-bolaget.html/<orderbookId>/<slug>
  // Without orderbookId we use the search endpoint first.
  try {
    const searchUrl = `https://www.avanza.se/_api/market-search/search/global-search?query=${encodeURIComponent(symbol)}&limit=3`;
    const r = await fetch(searchUrl, { headers: { 'User-Agent': BROWSER_UA, 'Accept': 'application/json' } });
    if (!r.ok) return [];
    const j = await r.json();
    const hit = (j.resultGroups?.stock?.hits || [])[0];
    if (!hit) return [];
    const orderbookId = hit.link?.orderbookId || hit.orderbookId;
    if (!orderbookId) return [];
    // Holdings endpoint
    const holdersUrl = `https://www.avanza.se/_api/company-guide/shareholders/${orderbookId}`;
    const r2 = await fetch(holdersUrl, { headers: { 'User-Agent': BROWSER_UA, 'Accept': 'application/json' } });
    if (!r2.ok) return [];
    const j2 = await r2.json();
    const holders = j2.shareholders || j2.holders || j2 || [];
    const updatedAt = (j2.updatedAt || j2.updated_at || today()).slice(0, 10);
    const rows = [];
    for (const h of (Array.isArray(holders) ? holders : [])) {
      if (!h.name && !h.holderName) continue;
      rows.push({
        source: 'nordic_major',
        issuer_country: 'SE',
        holder_name: h.name || h.holderName,
        as_of_date: updatedAt,
        shares: Number(h.shares || h.numberOfShares || 0) || null,
        position_pct: Number(h.capital || h.capitalPercent || h.percentOfShares || 0) || null,
        raw_holder_type: h.holderType || h.type || 'nordic_major',
        filing_url: `https://www.avanza.se/aktier/om-bolaget.html/${orderbookId}/`,
      });
    }
    return rows;
  } catch (e) {
    // Silent — Avanza API shape changes. Fall back to empty.
    return [];
  }
}

// --------------------------------------------------------------------------
// Unified runner — pulls all sources and persists.
// --------------------------------------------------------------------------
async function refreshHoldersForDeal(deal) {
  const ticker = deal.primary_ticker || deal.target_ticker
              || deal.spinco_ticker || deal.parent_ticker;
  if (!ticker) return { inserted: 0, source_breakdown: {} };
  const country = deal.country;
  const issuerName = deal.target_name || deal.spinco_name || deal.parent_name;

  let all = [];
  // US path: 13F if we have a CIK + CUSIP (CUSIP not stored yet — we derive from SEC tickers JSON)
  if (country === 'US' && deal.source_cik) {
    const cusip = await lookupCusipForCik(deal.source_cik);
    if (cusip) {
      all.push(...await fetchSec13fForIssuer({
        cik: deal.source_cik, cusip, ticker, issuerName,
      }));
    }
  }
  // UK path: TR-1 RSS (broad — not per-issuer, caller deals with matching)
  // These are better run as a global refresh, not per-deal. Skipped here.

  // Nordic path: Avanza per-ticker
  if (country === 'SE' || String(ticker).startsWith('STO:')) {
    all.push(...await fetchNordicMajorHolders([ticker]));
    // Also parent if different
    if (deal.parent_ticker && deal.parent_ticker !== ticker) {
      all.push(...await fetchNordicMajorHolders([deal.parent_ticker]));
    }
  }

  const breakdown = {};
  let inserted = 0;
  for (const h of all) {
    h.issuer_name = h.issuer_name || issuerName;
    h.issuer_ticker = h.issuer_ticker || ticker;
    if (!h.as_of_date) h.as_of_date = today();
    const ok = await upsertHolder(h);
    if (ok) {
      inserted++;
      breakdown[h.source] = (breakdown[h.source] || 0) + 1;
    }
  }
  return { inserted, source_breakdown: breakdown, fetched: all.length };
}

async function lookupCusipForCik(cik) {
  // SEC doesn't expose CUSIP directly. The easiest path is to look at any
  // filing and read the CUSIP from the cover page. 13F uses CUSIP, 10-K
  // doesn't. We piggyback on a quick shortcut: the OpenFIGI API is free
  // up to 25 req/min and maps ticker → CUSIP.
  try {
    const subsUrl = `https://data.sec.gov/submissions/CIK${String(cik).padStart(10, '0')}.json`;
    const r = await fetch(subsUrl, { headers: { 'User-Agent': UA, 'Accept': 'application/json' } });
    if (!r.ok) return null;
    const j = await r.json();
    const tickers = j.tickers || [];
    if (!tickers.length) return null;
    const symbol = tickers[0];
    // OpenFIGI
    const figi = await fetch('https://api.openfigi.com/v3/mapping', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'User-Agent': UA },
      body: JSON.stringify([{ idType: 'TICKER', idValue: symbol, exchCode: 'US' }]),
    });
    if (!figi.ok) return null;
    const fj = await figi.json();
    const cusip = (fj[0]?.data || []).find(d => d.compositeFIGI)?.securityDescription;
    // OpenFIGI doesn't return CUSIP without premium. Fall back: null.
    // TODO: parse CUSIP from cover page of a recent 10-K.
    return null;
  } catch { return null; }
}

// Bulk refresh: iterate active deals + run per-issuer fetchers.
// Global sources (UK TR-1 RSS, AFM RSS) run once up front.
async function refreshAllHolders({ activeOnly = true, max = 80 } = {}) {
  const rows = await query(
    `SELECT * FROM deals
     WHERE status IN ('pending','announced','completed')
       AND ($1 = 0 OR completed_date IS NULL OR completed_date > $2)
     ORDER BY id DESC LIMIT $3`,
    [activeOnly ? 1 : 0,
     new Date(Date.now() - 365 * 86400 * 1000).toISOString().slice(0, 10),
     max]
  );

  let totalInserted = 0;
  const bySource = {};

  // Global RSS passes
  try {
    const uk = await fetchUkTr1();
    for (const h of uk) {
      const ok = await upsertHolder(h);
      if (ok) { totalInserted++; bySource[h.source] = (bySource[h.source] || 0) + 1; }
    }
  } catch (e) { console.error('[holders:uk_tr1]', e.message); }

  try {
    const nl = await fetchAfmSubstantial();
    for (const h of nl) {
      const ok = await upsertHolder(h);
      if (ok) { totalInserted++; bySource[h.source] = (bySource[h.source] || 0) + 1; }
    }
  } catch (e) { console.error('[holders:afm]', e.message); }

  // Per-deal passes (Nordic + US 13F)
  for (const d of rows) {
    try {
      const r = await refreshHoldersForDeal(d);
      totalInserted += r.inserted;
      for (const [k, v] of Object.entries(r.source_breakdown)) {
        bySource[k] = (bySource[k] || 0) + v;
      }
    } catch (e) {
      console.error(`[holders:deal ${d.id}]`, e.message);
    }
  }

  return { total: totalInserted, by_source: bySource, deals_processed: rows.length };
}

module.exports = {
  refreshAllHolders,
  refreshHoldersForDeal,
  fetchUkTr1,
  fetchAfmSubstantial,
  fetchNordicMajorHolders,
  fetchSec13fForIssuer,
  upsertHolder,
  inferHolderType,
};
