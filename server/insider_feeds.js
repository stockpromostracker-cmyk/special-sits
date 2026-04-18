// Insider transactions feeds \u2014 US (SEC Form 4), UK (LSE RNS director dealings),
// and Nordic (MAR PDMR notifications, surfaced via Google News RSS).
//
// All feeds normalize into a common schema in the `insider_transactions` table.
// Each parser is best-effort: if the source is flaky we swallow the error and
// return an empty array rather than blocking the whole cycle.

const Parser = require('rss-parser');
const { query } = require('./db');

const UA = 'SpecialSits Research cfrjacobsson@gmail.com';
const BROWSER_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

const parser = new Parser({
  timeout: 20000,
  headers: { 'user-agent': UA, 'accept': 'application/atom+xml, application/rss+xml, application/xml;q=0.9, */*;q=0.8' },
});
const browserParser = new Parser({
  timeout: 20000,
  headers: {
    'user-agent': BROWSER_UA,
    'accept': 'application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8',
    'accept-language': 'en-US,en;q=0.9',
  },
});

// --------------------------------------------------------------------------
// SEC Form 4 \u2014 US insiders (officers, directors, 10% holders)
// --------------------------------------------------------------------------
// The /cgi-bin/browse-edgar getcurrent endpoint returns a chronological atom
// feed of the 40 most-recent Form 4 filings. We then fetch each filing's
// primary XML document for structured transactionCode / shares / price data.
// --------------------------------------------------------------------------

async function fetchSecForm4Index(count = 100) {
  const url = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&dateb=&owner=include&count=${count}&output=atom`;
  try {
    const feed = await parser.parseURL(url);
    return (feed.items || []).map(e => ({
      source_id: e.id || e.link,
      index_url: e.link,
      title: e.title || '',
      pub: e.pubDate || e.isoDate || null,
    }));
  } catch (e) {
    console.error('[sec_form4:index]', e.message);
    return [];
  }
}

// The filing index page lists the primary XML doc. We derive it from the
// accession number embedded in the EDGAR link rather than HTML-parsing.
// Index URL pattern: https://www.sec.gov/cgi-bin/browse-edgar?...&accession_number=0001234567-26-000123
// Document dir:     https://www.sec.gov/Archives/edgar/data/<CIK>/<ACCESSION_NO_DASHES>/
// XML file name is typically wf-form4_XXXXXXXXXXX.xml \u2014 we list the dir and pick the first .xml.
async function fetchForm4Xml(indexUrl) {
  // Pull accession + CIK from the URL
  const m = indexUrl.match(/CIK=(\d+).*accession_number=([0-9-]+)/i)
          || indexUrl.match(/Archives\/edgar\/data\/(\d+)\/(\d{10}-?\d{2}-?\d{6})/i);
  if (!m) return null;
  const cik = m[1];
  const accDashed = m[2];
  const accNoDashes = accDashed.replace(/-/g, '');
  const dirUrl = `https://www.sec.gov/Archives/edgar/data/${Number(cik)}/${accNoDashes}/`;

  // The primary Form 4 XML file is consistently named "*_doc*.xml" \u2014 we fetch
  // the JSON index file that EDGAR provides for every accession.
  const idxJsonUrl = `${dirUrl}index.json`;
  try {
    const res = await fetch(idxJsonUrl, { headers: { 'user-agent': UA } });
    if (!res.ok) return null;
    const idx = await res.json();
    const xmlDoc = (idx.directory?.item || []).find(
      it => /\.xml$/i.test(it.name) && !/^xslF/i.test(it.name) && !/primary_doc/i.test(it.name)
    ) || (idx.directory?.item || []).find(it => /\.xml$/i.test(it.name));
    if (!xmlDoc) return null;
    const xmlUrl = `${dirUrl}${xmlDoc.name}`;
    const xmlRes = await fetch(xmlUrl, { headers: { 'user-agent': UA } });
    if (!xmlRes.ok) return null;
    return { xml: await xmlRes.text(), xmlUrl };
  } catch (e) {
    return null;
  }
}

// Extract tag content (first match, non-greedy).
function xmlTag(xml, tag) {
  const m = xml.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)</${tag}>`, 'i'));
  return m ? m[1] : '';
}
function xmlTagAll(xml, tag) {
  const re = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)</${tag}>`, 'gi');
  const out = []; let m;
  while ((m = re.exec(xml))) out.push(m[1]);
  return out;
}
function xmlValue(block, tag) {
  // Form 4 wraps values like:  <sharesOwnedFollowingTransaction><value>1234</value></sharesOwnedFollowingTransaction>
  const inner = xmlTag(block, tag);
  if (!inner) return null;
  const v = xmlTag(inner, 'value');
  return (v || inner).replace(/<[^>]+>/g, '').trim() || null;
}

// Transaction codes \u2014 see SEC Form 4 instructions. P/S are the actionable ones.
// We classify P as buy, S as sell, everything else as non-market / null.
const BUY_CODES = new Set(['P']);   // Open-market or private purchase
const SELL_CODES = new Set(['S']);  // Open-market or private sale

function parseForm4({ xml, xmlUrl, indexUrl, pub }) {
  const rows = [];
  const issuerName = xmlValue(xml, 'issuerName') || '';
  const issuerTicker = (xmlValue(xml, 'issuerTradingSymbol') || '').toUpperCase();

  // Owner info
  const ownerName = xmlValue(xml, 'rptOwnerName') || '';
  const isOfficer = /<isOfficer>\s*(?:1|true)\s*<\/isOfficer>/i.test(xml) ? 1 : 0;
  const isDirector = /<isDirector>\s*(?:1|true)\s*<\/isDirector>/i.test(xml) ? 1 : 0;
  const isTen = /<isTenPercentOwner>\s*(?:1|true)\s*<\/isTenPercentOwner>/i.test(xml) ? 1 : 0;
  const officerTitle = xmlValue(xml, 'officerTitle') || '';
  const insiderTitle = officerTitle
    || (isDirector ? 'Director' : isTen ? '10% Owner' : '');

  // Non-derivative transactions block
  const nonDerivBlock = xmlTag(xml, 'nonDerivativeTable');
  const txBlocks = xmlTagAll(nonDerivBlock, 'nonDerivativeTransaction');

  for (const tx of txBlocks) {
    const code = xmlValue(tx, 'transactionCode') || '';
    const acqDisp = xmlValue(tx, 'transactionAcquiredDisposedCode') || '';  // A or D
    const sharesStr = xmlValue(tx, 'transactionShares');
    const priceStr = xmlValue(tx, 'transactionPricePerShare');
    const dateStr = xmlValue(tx, 'transactionDate') || '';

    const shares = sharesStr ? Number(sharesStr) : null;
    const price = priceStr ? Number(priceStr) : null;
    if (!shares || shares <= 0) continue;

    let isBuy = null;
    if (BUY_CODES.has(code) && acqDisp === 'A') isBuy = 1;
    else if (SELL_CODES.has(code) && acqDisp === 'D') isBuy = 0;
    else continue;  // Ignore awards, gifts, exercises, etc.

    const signed = isBuy ? shares : -shares;
    const value = price && shares ? price * shares : null;

    rows.push({
      source: 'sec_form4',
      source_id: `${indexUrl}|${dateStr}|${code}|${shares}|${price || ''}`.slice(0, 300),
      url: xmlUrl,
      issuer_name: issuerName,
      issuer_country: 'US',
      issuer_ticker: issuerTicker ? `US:${issuerTicker}` : null,
      insider_name: ownerName,
      insider_title: insiderTitle,
      is_director: isDirector,
      is_officer: isOfficer,
      is_ten_percent_owner: isTen,
      transaction_date: dateStr,
      transaction_code: code,
      is_buy: isBuy,
      shares: signed,
      price_local: price,
      value_local: value,
      currency: 'USD',
      price_usd: price,
      value_usd: value,
    });
  }
  return rows;
}

async function fetchSecForm4({ limit = 40 } = {}) {
  const index = await fetchSecForm4Index(limit);
  const all = [];
  // Light rate-limit: SEC asks for <=10 req/sec. We do serial with short sleep.
  for (const entry of index) {
    const doc = await fetchForm4Xml(entry.index_url);
    if (!doc) continue;
    const rows = parseForm4({ ...doc, indexUrl: entry.index_url, pub: entry.pub });
    all.push(...rows);
    await sleep(120);
  }
  console.log(`[insider:sec_form4] ${index.length} filings \u2192 ${all.length} rows`);
  return all;
}

// --------------------------------------------------------------------------
// UK \u2014 LSE RNS "Director / PDMR shareholding" via Google News RSS
// --------------------------------------------------------------------------
// The LSE RNS feed isn't publicly available as a clean RSS. We work around by
// querying Google News for the phrase "Director/PDMR Shareholding" which is
// the standard RNS headline for PDMR transactions. We store raw rows with
// whatever structured data we can grep out; the classifier layer later links
// them to deals via issuer_ticker.
// --------------------------------------------------------------------------

async function fetchLseDirectorDealings({ days = 7 } = {}) {
  const queries = [
    '"Director/PDMR Shareholding"',
    '"Director / PDMR Shareholding"',
    '"Transaction by PDMR"',
  ];
  const out = [];
  for (const q of queries) {
    const url = `https://news.google.com/rss/search?q=${encodeURIComponent(q + ` when:${days}d`)}&hl=en-GB&gl=GB&ceid=GB:en`;
    try {
      const res = await fetch(url, { headers: { 'user-agent': BROWSER_UA, 'accept-language': 'en-GB,en;q=0.9' } });
      if (!res.ok) continue;
      const xml = await res.text();
      if (!xml || xml.length < 100) continue;
      const feed = await browserParser.parseString(xml);
      for (const entry of feed.items || []) {
        const title = entry.title || '';
        if (!/PDMR|Director.*Shareholding/i.test(title)) continue;
        // Extract issuer name \u2014 RNS headlines usually lead with the ticker or company name.
        const issuerMatch = title.match(/^([A-Z0-9&\s.,()\-']{2,60?}?)\s*[-\u2013:]/);
        const issuerName = issuerMatch ? issuerMatch[1].trim() : '';
        out.push({
          source: 'lse_rns',
          source_id: entry.guid || entry.link,
          url: entry.link,
          issuer_name: issuerName,
          issuer_country: 'GB',
          issuer_ticker: null,  // resolved later against deals.primary_ticker via name-match
          insider_name: '',
          insider_title: 'PDMR',
          is_director: 1,
          is_officer: 0,
          is_ten_percent_owner: 0,
          transaction_date: entry.isoDate ? String(entry.isoDate).slice(0, 10) : null,
          transaction_code: 'P',        // RNS PDMR dealings are almost always purchases
          is_buy: 1,                    // We optimistically assume purchase; refined by body parsing later
          shares: null,
          price_local: null,
          value_local: null,
          currency: 'GBP',
          price_usd: null,
          value_usd: null,
        });
      }
    } catch (e) {
      console.error(`[insider:lse_rns] ${q}`, e.message);
    }
  }
  console.log(`[insider:lse_rns] ${out.length} rows`);
  return out;
}

// --------------------------------------------------------------------------
// Nordic / EU \u2014 MAR Article 19 PDMR notifications via Google News RSS
// --------------------------------------------------------------------------

async function fetchNordicMar({ days = 7 } = {}) {
  const queries = [
    { q: '"Transactions by persons discharging managerial responsibilities"', geo: 'GB' },
    { q: '"MAR notification" PDMR', geo: 'SE' },
    { q: '"Meldepliktig handel"', geo: 'NO' },    // Norwegian
    { q: '"Anm\u00e4lan om transaktioner"', geo: 'SE' }, // Swedish
    { q: '"Sis\u00e4piiril\u00e4isten liiketoimet"', geo: 'FI' }, // Finnish
  ];
  const out = [];
  for (const { q, geo } of queries) {
    const url = `https://news.google.com/rss/search?q=${encodeURIComponent(q + ` when:${days}d`)}&hl=en-${geo}&gl=${geo}&ceid=${geo}:en`;
    try {
      const res = await fetch(url, { headers: { 'user-agent': BROWSER_UA } });
      if (!res.ok) continue;
      const xml = await res.text();
      if (!xml || xml.length < 100) continue;
      const feed = await browserParser.parseString(xml);
      for (const entry of feed.items || []) {
        out.push({
          source: 'nordic_mar',
          source_id: entry.guid || entry.link,
          url: entry.link,
          issuer_name: (entry.title || '').split(/[-\u2013:]/)[0]?.trim() || '',
          issuer_country: geo === 'GB' ? null : geo,  // the GB pass is a catch-all for EN-language filings
          issuer_ticker: null,
          insider_name: '',
          insider_title: 'PDMR',
          is_director: 1,
          is_officer: 0,
          is_ten_percent_owner: 0,
          transaction_date: entry.isoDate ? String(entry.isoDate).slice(0, 10) : null,
          transaction_code: 'P',
          is_buy: 1,
          shares: null,
          price_local: null,
          value_local: null,
          currency: geo === 'SE' ? 'SEK' : geo === 'NO' ? 'NOK' : geo === 'DK' ? 'DKK' : geo === 'FI' ? 'EUR' : null,
          price_usd: null,
          value_usd: null,
        });
      }
    } catch (e) {
      console.error(`[insider:nordic_mar] ${q}`, e.message);
    }
  }
  console.log(`[insider:nordic_mar] ${out.length} rows`);
  return out;
}

// --------------------------------------------------------------------------
// SEC 13D / 13G — activist / >5% beneficial owner filings (US)
// --------------------------------------------------------------------------
// 13D = active intent (activists); 13G = passive. Either signals concentrated
// ownership worth flagging. We treat these as "large owner" events rather than
// classic insider transactions, storing them with is_ten_percent_owner=1 and
// a NULL is_buy so they don't skew the buy/sell rollups.
// --------------------------------------------------------------------------

// Well-known activist investors — any match in the filer_name gets flagged
// on the deal. Kept deliberately short; expand over time.
const KNOWN_ACTIVISTS = [
  'Pershing Square', 'Elliott', 'Starboard Value', 'Cevian Capital',
  'Third Point', 'Trian', 'ValueAct', 'Corvex', 'Engine No. 1',
  'JANA Partners', 'Icahn', 'Sachem Head', 'Mantle Ridge', 'Ancora',
  'Irenic Capital', 'Jericho Capital', 'Land & Buildings', 'Engine Capital',
  'Browning West', 'Legion Partners', 'Politan Capital',
];

async function fetchSec13dg({ limit = 60 } = {}) {
  const forms = ['SC 13D', 'SC 13D/A', 'SC 13G', 'SC 13G/A'];
  const out = [];
  for (const form of forms) {
    const url = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=${encodeURIComponent(form)}&company=&dateb=&owner=include&count=${limit}&output=atom`;
    try {
      const feed = await parser.parseURL(url);
      for (const entry of feed.items || []) {
        const title = entry.title || '';
        // EDGAR atom titles look like:  "SC 13D/A - Acme Corp (0001234567) (Subject)" or similar,
        // and the filer is the "Filed by" party. Without fetching the primary_doc we can
        // approximate by storing the title + link and relying on the classifier / rollup
        // to name-match against the deal's issuer.
        const isActivist = KNOWN_ACTIVISTS.some(a => new RegExp(a.replace(/[.*+?^${}()|[\]\\]/g,'\\$&'), 'i').test(title));
        out.push({
          source: 'sec_13dg',
          source_id: entry.id || entry.link,
          url: entry.link,
          issuer_name: '',            // not reliably in the title; resolved later
          issuer_country: 'US',
          issuer_ticker: null,
          insider_name: title.replace(/\s*\(\d+\).*$/, '').replace(/^\[[^\]]+\]\s*/, '').trim(),
          insider_title: form.startsWith('SC 13D') ? (isActivist ? 'Activist 13D' : '13D filer (active intent)') : '13G filer (passive)',
          is_director: 0,
          is_officer: 0,
          is_ten_percent_owner: 1,
          transaction_date: entry.isoDate ? String(entry.isoDate).slice(0, 10) : null,
          transaction_code: form,
          is_buy: null,   // 13D/G is not a trade, it's a stake disclosure
          shares: null,
          price_local: null,
          value_local: null,
          currency: 'USD',
          price_usd: null,
          value_usd: null,
        });
      }
    } catch (e) {
      console.error(`[insider:sec_13dg] ${form}`, e.message);
    }
  }
  console.log(`[insider:sec_13dg] ${out.length} rows`);
  return out;
}

// --------------------------------------------------------------------------
// Persist
// --------------------------------------------------------------------------

async function saveInsiderTransactions(rows) {
  let inserted = 0;
  for (const r of rows) {
    if (!r.source_id) continue;
    try {
      const res = await query(
        `INSERT INTO insider_transactions (
           source, source_id, url,
           issuer_name, issuer_country, issuer_ticker,
           insider_name, insider_title,
           is_director, is_officer, is_ten_percent_owner,
           transaction_date, transaction_code, is_buy,
           shares, price_local, value_local, currency,
           price_usd, value_usd
         )
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
         ON CONFLICT (source, source_id) DO NOTHING
         RETURNING id`,
        [
          r.source, r.source_id, r.url,
          r.issuer_name, r.issuer_country, r.issuer_ticker,
          r.insider_name, r.insider_title,
          r.is_director || 0, r.is_officer || 0, r.is_ten_percent_owner || 0,
          r.transaction_date, r.transaction_code, r.is_buy,
          r.shares, r.price_local, r.value_local, r.currency,
          r.price_usd, r.value_usd,
        ]
      );
      if (res.length) inserted++;
    } catch (e) {
      // Duplicate-key on identical source_id is fine; log other errors.
      if (!/duplicate|unique/i.test(e.message)) {
        console.error('[saveInsiderTransactions]', e.message);
      }
    }
  }
  return inserted;
}

async function fetchAllInsider() {
  const [us, stakes, uk, nordic] = await Promise.all([
    fetchSecForm4({ limit: 40 }).catch(e => (console.error('sec form4', e.message), [])),
    fetchSec13dg({ limit: 60 }).catch(e => (console.error('sec 13dg', e.message), [])),
    fetchLseDirectorDealings().catch(e => (console.error('lse rns', e.message), [])),
    fetchNordicMar().catch(e => (console.error('nordic', e.message), [])),
  ]);
  const all = [...us, ...stakes, ...uk, ...nordic];
  const inserted = await saveInsiderTransactions(all);
  console.log(`[insider] fetched ${all.length} (us=${us.length} stakes=${stakes.length} uk=${uk.length} nordic=${nordic.length}), inserted ${inserted}`);
  return { fetched: all.length, inserted };
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

module.exports = {
  fetchAllInsider,
  fetchSecForm4,
  fetchSec13dg,
  fetchLseDirectorDealings,
  fetchNordicMar,
  saveInsiderTransactions,
  KNOWN_ACTIVISTS,
};
