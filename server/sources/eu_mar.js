// EU MAR Article 19 PDMR ingestors.
//
// Two confirmed-working direct feeds as of April 2026:
//   - Netherlands (AFM)       → CSV + XML register (open, no auth)
//   - Sweden (Finansinspektionen) → CSV export (UTF-16LE) via marknadssok.fi.se
//
// All rows normalize to the same `insider_transactions` schema as SEC Form 4.
// Each fetcher is best-effort and returns [] on failure; errors are logged
// rather than propagated so a broken feed can't block the rest of the cycle.
//
// Output row schema (see db.js → insider_transactions):
//   source, source_id, url, issuer_name, issuer_country, issuer_ticker,
//   insider_name, insider_title, is_director, is_officer,
//   is_ten_percent_owner, transaction_date, transaction_code, is_buy,
//   shares, price_local, value_local, currency, price_usd, value_usd

const UA = 'SpecialSits Research cfrjacobsson@gmail.com';
const { toUsd } = require('../market_data');

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

// Parse one CSV line with semicolon separator and "quoted ; values ; inside".
// AFM and FI both use semicolon + double-quote. This is a hand-rolled parser
// rather than a dependency because the grammar is simple and consistent.
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
      } else {
        cur += c;
      }
    } else {
      if (c === '"') inQ = true;
      else if (c === sep) { out.push(cur); cur = ''; }
      else cur += c;
    }
  }
  out.push(cur);
  return out;
}

// Decode UTF-16LE (with or without BOM) to a normal JS string. Sweden FI
// serves its CSV as UTF-16LE; Node's fetch().text() assumes UTF-8 so garbles
// it. We grab the ArrayBuffer and decode manually.
function utf16leToString(buf) {
  const u8 = new Uint8Array(buf);
  let start = 0;
  if (u8.length >= 2 && u8[0] === 0xFF && u8[1] === 0xFE) start = 2; // skip BOM
  // Build a Uint16Array aligned to an even byte offset. Uint16Array requires
  // byte-alignment so we copy into a fresh buffer if start is odd.
  let u16;
  if (start === 0 && u8.byteOffset % 2 === 0) {
    u16 = new Uint16Array(u8.buffer, u8.byteOffset, Math.floor(u8.byteLength / 2));
  } else {
    const slice = u8.slice(start);
    const copy = new Uint8Array(slice.length);
    copy.set(slice);
    u16 = new Uint16Array(copy.buffer, 0, Math.floor(copy.byteLength / 2));
  }
  // Chunk to avoid call-stack limit on large files (~10k char batches).
  const CHUNK = 8192;
  let out = '';
  for (let i = 0; i < u16.length; i += CHUNK) {
    const end = Math.min(i + CHUNK, u16.length);
    out += String.fromCharCode.apply(null, Array.from(u16.subarray(i, end)));
  }
  return out;
}

// ISO-date normalizer. `fmt` hint is 'dmy' (DD/MM/YYYY — FI) or 'mdy'
// (M/D/YYYY — AFM XML). YYYY-MM-DD is always recognized regardless of hint.
function toIsoDate(s, fmt = 'dmy') {
  if (!s) return null;
  const t = String(s).trim();
  const dateOnly = t.split(/[\sT]/)[0];
  let m;
  // YYYY-MM-DD or YYYY/MM/DD (ISO / SQL style — unambiguous)
  if ((m = dateOnly.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/))) {
    return `${m[1]}-${String(m[2]).padStart(2, '0')}-${String(m[3]).padStart(2, '0')}`;
  }
  // Ambiguous slash form — rely on fmt hint
  if ((m = dateOnly.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/))) {
    const a = m[1], b = m[2], y = m[3];
    if (fmt === 'mdy') return `${y}-${String(a).padStart(2, '0')}-${String(b).padStart(2, '0')}`;
    return `${y}-${String(b).padStart(2, '0')}-${String(a).padStart(2, '0')}`; // dmy default
  }
  return null;
}

// Classify MAR "Nature of transaction" / AFM function into is_buy / code.
// MAR standard natures include Acquisition, Disposal, Subscription, Pledge,
// Lending, etc. We flag Acquisition/Subscription as buy and Disposal as sell.
function classifyNature(nature) {
  if (!nature) return { code: null, is_buy: null };
  const n = String(nature).toLowerCase();
  if (/acqui|subscrib|purchase|buy|koop|f[öo]rv[äa]rv/.test(n)) return { code: 'P', is_buy: 1 };
  if (/dispos|sale|sell|verkoop|avytt|f[öo]rs[äa]ljning/.test(n)) return { code: 'S', is_buy: 0 };
  return { code: nature.slice(0, 24), is_buy: null };
}

// Map a position / function string to is_director / is_officer flags. MAR
// doesn't split the two as cleanly as SEC Form 4, but title keywords give a
// reasonable approximation for the rollup.
function classifyTitle(title) {
  const t = String(title || '').toLowerCase();
  const isDirector = /director|board|chair|supervisory|non[- ]?exec|niet.uitvoerend|styrels/.test(t) ? 1 : 0;
  const isOfficer  = /ceo|cfo|coo|cto|president|officer|manag|executive|vd\b|verkst[äa]ll/.test(t) ? 1 : 0;
  // If neither matches, assume PDMR-director (the common case for closely
  // associated persons is also a director under MAR).
  if (!isDirector && !isOfficer) return { is_director: 1, is_officer: 0 };
  return { is_director: isDirector, is_officer: isOfficer };
}

function parseNumber(s) {
  if (s == null || s === '') return null;
  const t = String(s).replace(/\u00a0/g, '').replace(/\s+/g, '').replace(',', '.');
  const n = Number(t);
  return Number.isFinite(n) ? n : null;
}

// --------------------------------------------------------------------------
// Netherlands — AFM MAR 19 register
// --------------------------------------------------------------------------
// XML export has richer fields than CSV (includes LEI, function, close-
// associate). Transaction price / volume / direction are NOT in the summary
// feed — they live in a per-notification detail PDF. For v1 we ingest the
// summary with is_buy = null, matching how the Nordic Google-News feed was
// previously handled.

const AFM_XML_URL =
  'https://www.afm.nl/export.aspx?type=0ee836dc-5520-459d-bcf4-a4a689de6614&format=xml';

async function fetchAfm({ days = 180 } = {}) {
  const cutoff = new Date(Date.now() - days * 24 * 3600 * 1000);
  const cutoffIso = cutoff.toISOString().slice(0, 10);
  try {
    const res = await fetch(AFM_XML_URL, {
      headers: { 'user-agent': UA, accept: 'application/xml, text/xml, */*' },
    });
    if (!res.ok) throw new Error(`http ${res.status}`);
    const xml = await res.text();
    const out = [];
    const re = /<vermelding>\s*<meldingid>([\s\S]*?)<\/vermelding>/gi;
    let m;
    while ((m = re.exec(xml))) {
      const block = m[0];
      const get = (tag) => {
        const mm = block.match(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, 'i'));
        return mm ? mm[1].trim() : '';
      };
      const meldingid = get('meldingid');
      if (!meldingid) continue;
      const txDate = toIsoDate(get('transactiedatum'), 'mdy');
      // Skip rows older than cutoff — AFM register goes back years; we only
      // want recent activity to keep insert volume reasonable.
      if (txDate && txDate < cutoffIso) continue;
      const issuer = get('uitgevendeinstelling');
      const person = get('meldingsplichtige');
      const closeAssociateOf = get('nauwgelieerdaan'); // "closely associated with" — present when notifiable person is an entity owned by a PDMR
      const functieTitle = get('functie');
      const lei = get('lei');

      const insiderName = person || (closeAssociateOf ? `(closely assoc. with ${closeAssociateOf})` : '');
      const { is_director, is_officer } = classifyTitle(functieTitle);

      out.push({
        source: 'afm_nl',
        source_id: `afm:${meldingid}`,
        // No per-notification permalink on AFM; link the user to the register.
        url: 'https://www.afm.nl/en/sector/registers/meldingenregisters/transacties-leidinggevenden-mar19-',
        issuer_name: issuer,
        issuer_country: 'NL',
        issuer_ticker: null,   // resolved downstream via LEI / issuer-name match
        insider_name: insiderName,
        insider_title: functieTitle || 'PDMR',
        is_director,
        is_officer,
        is_ten_percent_owner: 0,
        transaction_date: txDate,
        transaction_code: 'MAR19', // detail not in feed
        is_buy: null,
        shares: null,
        price_local: null,
        value_local: null,
        currency: 'EUR',
        price_usd: null,
        value_usd: null,
        // Extra context (not persisted to column but useful for logs)
        _lei: lei,
        _closeAssociateOf: closeAssociateOf || null,
      });
    }
    console.log(`[insider:afm] ${out.length} rows`);
    return out;
  } catch (e) {
    console.error('[insider:afm]', e.message);
    return [];
  }
}

// --------------------------------------------------------------------------
// Sweden — Finansinspektionen insynsregistret (PDMR register)
// --------------------------------------------------------------------------
// The FI search page offers an Excel export that actually returns CSV in
// UTF-16LE with semicolon separators. We pull the last ~30 days of
// publication-date activity; the register is updated T+0 to T+1.

const FI_EXPORT_BASE =
  'https://marknadssok.fi.se/publiceringsklient/en-GB/Search/Search';

async function fetchSwedenFi({ days = 30 } = {}) {
  const toDate = new Date();
  const fromDate = new Date(toDate.getTime() - days * 24 * 3600 * 1000);
  const fmt = (d) => d.toISOString().slice(0, 10);
  // Filter on publication date so we capture all recently-filed transactions
  // (which may have a tx date from earlier in the 30-day window).
  const params = new URLSearchParams({
    SearchFunctionType: 'Insyn',
    Utgivare: '',
    PersonILedandeStällningNamn: '',
    'Transaktionsdatum.From': '',
    'Transaktionsdatum.To': '',
    'Publiceringsdatum.From': fmt(fromDate),
    'Publiceringsdatum.To': fmt(toDate),
    button: 'export',
  });
  const url = `${FI_EXPORT_BASE}?${params.toString()}`;
  try {
    const res = await fetch(url, { headers: { 'user-agent': UA } });
    if (!res.ok) throw new Error(`http ${res.status}`);
    const buf = await res.arrayBuffer();
    const csv = utf16leToString(buf);
    const lines = csv.split(/\r?\n/).filter(Boolean);
    if (lines.length < 2) return [];
    // Header: Publication date;Issuer;LEI-code;Notifier;PDMR;Position;Closely associated;
    //         Amendment;Details of amendment;Initial notification;Linked to share option programme;
    //         Nature of transaction;Instrument type;Instrument name;ISIN;Transaction date;
    //         Volume;Unit;Price;Currency;Trading venue;Status
    const rows = [];
    for (let li = 1; li < lines.length; li++) {
      const cols = splitCsvLine(lines[li].replace(/\u00a0/g, ' '), ';');
      if (cols.length < 20) continue;
      const [
        pubDate, issuer, lei, notifier, pdmr, position, closelyAssoc,
        /*amendment*/, /*amendDetails*/, /*initial*/, /*optionProg*/,
        nature, /*instrType*/, instrName, isin, txDate,
        volume, /*unit*/, price, currency, venue, status,
      ] = cols;

      if (String(status || '').trim() && !/current|gäll|aktuell/i.test(status)) {
        // Skip cancelled/amended rows — the active one with the same ID will appear too
        continue;
      }
      const { code, is_buy } = classifyNature(nature);
      const { is_director, is_officer } = classifyTitle(position);
      const vol = parseNumber(volume);
      const px  = parseNumber(price);
      const val = vol != null && px != null ? vol * px : null;
      const ccy = (currency || 'SEK').trim().toUpperCase();
      const pxUsd = px != null ? toUsd(px, ccy) : null;
      const valUsd = val != null ? toUsd(val, ccy) : null;

      // Build a stable source_id. FI doesn't expose a per-filing permalink
      // from the export, so we hash the main fields.
      const key = [pubDate, issuer, pdmr, txDate, isin, volume, price, nature].join('|');
      const source_id = `fi:${simpleHash(key)}`;

      rows.push({
        source: 'fi_se',
        source_id,
        url: 'https://marknadssok.fi.se/publiceringsklient/en-GB/Search/Search?SearchFunctionType=Insyn',
        issuer_name: issuer?.trim() || '',
        issuer_country: 'SE',
        issuer_ticker: null,
        insider_name: pdmr?.trim() || notifier?.trim() || '',
        insider_title: (position || 'PDMR').trim(),
        is_director, is_officer,
        is_ten_percent_owner: 0,
        transaction_date: toIsoDate(txDate, 'dmy'),
        transaction_code: code || 'MAR19',
        is_buy,
        shares: vol,
        price_local: px,
        value_local: val,
        currency: ccy,
        price_usd: pxUsd,
        value_usd: valUsd,
        _lei: (lei || '').trim() || null,
        _isin: (isin || '').trim() || null,
        _venue: (venue || '').trim() || null,
      });
    }
    console.log(`[insider:fi_se] ${rows.length} rows (days=${days})`);
    return rows;
  } catch (e) {
    console.error('[insider:fi_se]', e.message);
    return [];
  }
}

// djb2 string hash → 8-char hex. Stable across processes; avoids pulling in crypto.
function simpleHash(s) {
  let h = 5381;
  for (let i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) >>> 0;
  return h.toString(16).padStart(8, '0');
}

module.exports = {
  fetchAfm,
  fetchSwedenFi,
  // test helpers
  _splitCsvLine: splitCsvLine,
  _utf16leToString: utf16leToString,
  _toIsoDate: toIsoDate,
  _classifyNature: classifyNature,
  _classifyTitle: classifyTitle,
};
