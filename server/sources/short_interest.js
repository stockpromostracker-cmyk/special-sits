// Short interest / short position ingestors.
//
// Three public regulator feeds, three different shapes:
//   - FINRA regShoDaily (US)  → daily short-sale VOLUME per symbol per
//                               reporting facility (NCTRF/NQTRF/NYTRF).
//                               Derived metric: short_ratio = short/total.
//   - AFM (Netherlands)       → DISCLOSED net short POSITIONS by holder
//                               ≥0.5% of issued capital (SSR).
//   - FCA (United Kingdom)    → Same SSR regime as NL, published as a daily
//                               XLSX aggregate of current positions.
//
// All rows normalize into the `short_positions` table in db.js. The `kind`
// column discriminates 'daily_volume' (US) from 'disclosed_position' (EU/UK).
//
// Each fetcher is best-effort: on error we log and return [] rather than
// blocking the full ingest cycle.
//
// NOTE on UNIQUE constraints: Postgres treats NULL as distinct in UNIQUE,
// so we coerce optional keys (isin / holder_name / reporting_facility) to
// empty string '' in the row objects so dedup works consistently across
// both SQLite and PG.

const XLSX = require('xlsx');

const UA = 'SpecialSits Research cfrjacobsson@gmail.com';
const BROWSER_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

// YYYY-MM-DD today in UTC.
function today() { return new Date().toISOString().slice(0, 10); }

// Most recent N business days (Mon–Fri) in UTC, newest first. We don't
// account for US/UK holidays — feeds just return no rows on those dates.
function lastBusinessDays(n) {
  const out = [];
  const d = new Date();
  while (out.length < n) {
    const day = d.getUTCDay();
    if (day !== 0 && day !== 6) out.push(d.toISOString().slice(0, 10));
    d.setUTCDate(d.getUTCDate() - 1);
  }
  return out;
}

function toNumber(v) {
  if (v === null || v === undefined || v === '') return null;
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  const s = String(v).replace(/\s/g, '').replace(',', '.');
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : null;
}

// --------------------------------------------------------------------------
// 1) FINRA regShoDaily  (US — daily short-sale volume per symbol)
// --------------------------------------------------------------------------
// POST https://api.finra.org/data/group/otcMarket/name/regShoDaily
// Body: { limit, compareFilters: [{ fieldName: 'tradeReportDate',
//                                   fieldValue: 'YYYY-MM-DD',
//                                   compareType: 'EQUAL' }] }
// Returns CSV with: tradeReportDate, symbol, shortParQuantity,
//   shortExemptParQuantity, totalParQuantity, marketCode, reportingFacilityCode.
// T-1 currency (today's date returns empty; yesterday is usually populated).
// --------------------------------------------------------------------------

async function fetchFinraRegShoForDate(date, limit = 20000) {
  const url = 'https://api.finra.org/data/group/otcMarket/name/regShoDaily';
  const body = {
    limit,
    compareFilters: [{
      fieldName: 'tradeReportDate',
      fieldValue: date,
      compareType: 'EQUAL',
    }],
  };
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'accept': 'text/plain,text/csv,*/*',
        'content-type': 'application/json',
        'user-agent': UA,
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      console.error(`[short:finra] ${date} HTTP ${res.status}`);
      return [];
    }
    const text = await res.text();
    return parseFinraCsv(text, date);
  } catch (e) {
    console.error('[short:finra]', date, e.message);
    return [];
  }
}

// CSV has no header on this endpoint — columns are positional.
//   0: tradeReportDate
//   1: symbol
//   2: shortParQuantity
//   3: shortExemptParQuantity
//   4: totalParQuantity
//   5: marketCode   (B=BATS OTC, Q=Nasdaq, N=NYSE)
//   6: reportingFacilityCode  (NCTRF / NQTRF / NYTRF)
function parseFinraCsv(text, date) {
  if (!text || text.length < 20) return [];
  const out = [];
  const lines = text.split(/\r?\n/);
  for (const line of lines) {
    if (!line || !line.trim()) continue;
    // Values are double-quoted, comma-separated.
    const parts = line.split(',').map(s => s.replace(/^"|"$/g, ''));
    if (parts.length < 7) continue;
    const [rptDate, symbol, shortQty, _exemptQty, totalQty, marketCode, facility] = parts;
    if (!symbol || symbol === 'symbol') continue; // header row if present
    const s = toNumber(shortQty);
    const t = toNumber(totalQty);
    if (!s || !t || t <= 0) continue;
    // Collapse EXCHANGE:SYMBOL. US exchange can't be inferred from marketCode
    // alone (B=ATS, Q=Nasdaq, N=NYSE TRF — all reporting venues, not listing
    // venues). We leave ticker as bare symbol; the join to `deals` compares
    // against `primary_ticker` which we also store bare.
    out.push({
      source: 'finra_regsho',
      kind: 'daily_volume',
      issuer_name: null,
      issuer_country: 'US',
      issuer_ticker: symbol.toUpperCase(),
      isin: '',
      holder_name: '',
      as_of_date: rptDate || date,
      short_volume: s,
      total_volume: t,
      short_ratio: +(s / t).toFixed(6),
      position_pct: null,
      reporting_facility: facility || '',
    });
  }
  return out;
}

// Wrapper: pull last N business days. Deduped at insert time by the UNIQUE
// key (source, kind, ticker, isin, holder, date, facility).
async function fetchFinraRegSho(days = 5) {
  const rows = [];
  for (const d of lastBusinessDays(days)) {
    const chunk = await fetchFinraRegShoForDate(d);
    rows.push(...chunk);
  }
  console.log(`[short:finra] ${rows.length} rows across ${days} days`);
  return rows;
}

// --------------------------------------------------------------------------
// 2) AFM (Netherlands)  —  disclosed net short positions ≥0.5%
// --------------------------------------------------------------------------
// Public register: https://www.afm.nl/en/sector/registers/meldingenregisters/netto-shortposities-actueel
// CSV export:      https://www.afm.nl/export.aspx?type=8a46a4ef-f196-4467-a7ab-1ae1cb58f0e7&format=csv
//
// Semicolon-delimited, UTF-8, header row:
//   "Positie houder";"Naam van de emittent";"ISIN";"Netto Shortpositie";"Positiedatum"
// --------------------------------------------------------------------------

function splitCsvLine(line, sep = ';') {
  const out = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQ) {
      if (c === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; }
        else { inQ = false; }
      } else cur += c;
    } else {
      if (c === '"') inQ = true;
      else if (c === sep) { out.push(cur); cur = ''; }
      else cur += c;
    }
  }
  out.push(cur);
  return out;
}

async function fetchAfmShort() {
  const url = 'https://www.afm.nl/export.aspx?type=8a46a4ef-f196-4467-a7ab-1ae1cb58f0e7&format=csv';
  try {
    const res = await fetch(url, { headers: { 'user-agent': BROWSER_UA, 'accept': 'text/csv,*/*' } });
    if (!res.ok) {
      console.error(`[short:afm] HTTP ${res.status}`);
      return [];
    }
    const text = await res.text();
    const lines = text.split(/\r?\n/).filter(l => l.trim().length > 0);
    if (lines.length < 2) return [];
    // Skip header.
    const out = [];
    for (let i = 1; i < lines.length; i++) {
      const cols = splitCsvLine(lines[i]);
      if (cols.length < 5) continue;
      const [holder, issuer, isin, pct, dateStr] = cols.map(s => s.trim());
      const p = toNumber(pct);
      if (!holder || !issuer || !p) continue;
      // Date format: "YYYY-MM-DD HH:MM:SS"
      const d = (dateStr || '').slice(0, 10);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) continue;
      out.push({
        source: 'afm_short',
        kind: 'disclosed_position',
        issuer_name: issuer,
        issuer_country: 'NL',
        issuer_ticker: '',
        isin: isin || '',
        holder_name: holder,
        as_of_date: d,
        short_volume: null,
        total_volume: null,
        short_ratio: null,
        position_pct: p,
        reporting_facility: '',
      });
    }
    console.log(`[short:afm] ${out.length} disclosed positions`);
    return out;
  } catch (e) {
    console.error('[short:afm]', e.message);
    return [];
  }
}

// --------------------------------------------------------------------------
// 3) FCA (United Kingdom)  —  disclosed net short positions ≥0.5%
// --------------------------------------------------------------------------
// Daily update XLSX: https://www.fca.org.uk/publication/data/short-positions-daily-update.xlsx
// The workbook has (at least) two sheets:
//   - "Current" (or similar) — snapshot of all positions ≥0.5% still live
//   - "Historic" (or similar) — delisting / reductions below threshold
// We ingest the current sheet. Columns observed historically:
//   Position Holder | Name of Share Issuer | ISIN | Net Short Position (%) |
//   Position Date
// --------------------------------------------------------------------------

async function fetchFcaShort() {
  const url = 'https://www.fca.org.uk/publication/data/short-positions-daily-update.xlsx';
  try {
    const res = await fetch(url, {
      headers: {
        'user-agent': BROWSER_UA,
        'accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*',
      },
    });
    if (!res.ok) {
      console.error(`[short:fca] HTTP ${res.status}`);
      return [];
    }
    const buf = Buffer.from(await res.arrayBuffer());
    const wb = XLSX.read(buf, { type: 'buffer' });
    // Prefer the sheet whose name starts with "Current"; fall back to first.
    const sheetName = wb.SheetNames.find(n => /current/i.test(n)) || wb.SheetNames[0];
    const sheet = wb.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(sheet, { defval: null });
    const out = [];
    for (const r of rows) {
      // Column names vary by release. Match flexibly.
      const holder = firstVal(r, /holder/i);
      const issuer = firstVal(r, /(issuer|share issuer|company)/i);
      const isin   = firstVal(r, /isin/i);
      const pct    = firstVal(r, /(net short|position.*%|% of)/i);
      const dateV  = firstVal(r, /(position date|as.of|date)/i);
      const p = toNumber(pct);
      if (!holder || !issuer || !p) continue;
      const d = excelToIsoDate(dateV);
      if (!d) continue;
      out.push({
        source: 'fca_short',
        kind: 'disclosed_position',
        issuer_name: String(issuer).trim(),
        issuer_country: 'GB',
        issuer_ticker: '',
        isin: (isin ? String(isin).trim() : ''),
        holder_name: String(holder).trim(),
        as_of_date: d,
        short_volume: null,
        total_volume: null,
        short_ratio: null,
        position_pct: p,
        reporting_facility: '',
      });
    }
    console.log(`[short:fca] ${out.length} disclosed positions (sheet: ${sheetName})`);
    return out;
  } catch (e) {
    console.error('[short:fca]', e.message);
    return [];
  }
}

// Pick first row value whose key matches `rx`. Excel column headers are
// unpredictable across regulator releases so we match case-insensitively on
// substrings rather than hard-coding names.
function firstVal(row, rx) {
  for (const k of Object.keys(row)) {
    if (rx.test(k)) return row[k];
  }
  return null;
}

// Excel may serialize dates as numeric serials (days-since-1900) or as
// parsed-JS Date objects if XLSX was told to convert. Handle both.
function excelToIsoDate(v) {
  if (v == null) return null;
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  if (typeof v === 'number') {
    // Excel serial: days since 1900-01-00, with 1900 leap bug.
    const ms = Math.round((v - 25569) * 86400 * 1000);
    const d = new Date(ms);
    if (isNaN(d.getTime())) return null;
    return d.toISOString().slice(0, 10);
  }
  const s = String(v).trim();
  // ISO already?
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  // DD/MM/YYYY (FCA convention)
  let m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (m) return `${m[3]}-${m[2].padStart(2, '0')}-${m[1].padStart(2, '0')}`;
  return null;
}

module.exports = {
  fetchFinraRegSho,
  fetchFinraRegShoForDate,
  fetchAfmShort,
  fetchFcaShort,
  // test helpers
  _parseFinraCsv: parseFinraCsv,
  _splitCsvLine: splitCsvLine,
  _excelToIsoDate: excelToIsoDate,
};
