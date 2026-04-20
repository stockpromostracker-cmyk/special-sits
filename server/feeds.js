// Feed ingestors — pull fresh items from every source, dedupe, insert into raw_items.
//
// Strategy: rely on Google News RSS for broad coverage (it indexes LSE RNS, Nasdaq Nordic,
// Euronext, and every regional newswire in one place) and SEC EDGAR direct for US filings.
// Google News RSS is public, rate-limit-friendly, and returns structured headlines that
// Gemini can classify well.

const Parser = require('rss-parser');
const { query } = require('./db');

// SEC bans generic User-Agents — use a descriptive one per their rules.
const UA = 'SpecialSits Research cfrjacobsson@gmail.com';

// Browser-like UA for Google News — required in some environments (Railway cloud IPs)
// where the default rss-parser UA gets empty responses.
const BROWSER_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

const parser = new Parser({
  timeout: 20000,
  headers: { 'user-agent': UA, 'accept': 'application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8' },
});

const googleParser = new Parser({
  timeout: 20000,
  headers: {
    'user-agent': BROWSER_UA,
    'accept': 'application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8',
    'accept-language': 'en-US,en;q=0.9',
  },
});

// ---- Helpers ---------------------------------------------------------------

// SEC atom summaries are HTML-encoded and contain rich structural info. Example:
//   <b>Filed:</b> 2026-04-17 <b>AccNo:</b> 0001...
//   <br>Item 1.01: Entry into a Material Definitive Agreement
//   <br>Item 2.03: Creation of a Direct Financial Obligation
// We want to extract the Item numbers + descriptions so Gemini has real signal.
function extractSecItems(rawSummary) {
  if (!rawSummary) return '';
  // Decode HTML entities minimally
  const decoded = String(rawSummary)
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, ' ');
  // Strip HTML tags
  const text = decoded.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
  return text;
}

// Map 8-K Item numbers to natural language (so Gemini doesn't have to infer).
const ITEM_GLOSS = {
  '1.01': 'Entry into a Material Definitive Agreement',
  '1.02': 'Termination of a Material Definitive Agreement',
  '1.03': 'Bankruptcy or Receivership',
  '2.01': 'Completion of Acquisition or Disposition of Assets',
  '2.02': 'Results of Operations and Financial Condition',
  '2.03': 'Creation of Direct Financial Obligation',
  '2.04': 'Triggering Events That Accelerate a Direct Financial Obligation',
  '2.05': 'Costs Associated with Exit or Disposal Activities',
  '2.06': 'Material Impairments',
  '3.01': 'Notice of Delisting or Failure to Satisfy Listing Rule',
  '3.02': 'Unregistered Sales of Equity Securities',
  '3.03': 'Material Modification to Rights of Security Holders',
  '5.01': 'Changes in Control of Registrant',
  '5.02': 'Departure/Election of Directors or Officers',
  '5.03': 'Amendments to Articles of Incorporation or Bylaws',
  '7.01': 'Regulation FD Disclosure',
  '8.01': 'Other Events',
  '9.01': 'Financial Statements and Exhibits',
};

// Forms known to be interesting — add a human-readable label Gemini can key on.
const FORM_LABELS = {
  '8-K': 'current report (merger, acquisition, material agreement, etc.)',
  'S-1': 'IPO registration statement',
  'F-1': 'foreign issuer IPO registration statement',
  'S-4': 'M&A/business combination registration statement',
  'SC 13E3': 'going-private transaction statement',
  'SC TO-I': 'issuer tender offer',
  'SC TO-T': 'third-party tender offer',
  '10-12B': 'spin-off/spin-out registration',
  '425': 'merger prospectus communication',
};

// ---- SEC EDGAR (US) --------------------------------------------------------
// Pulls the latest filings for special-situation-relevant form types.
async function fetchSecEdgar() {
  const formTypes = Object.keys(FORM_LABELS);
  const items = [];

  for (const form of formTypes) {
    const url = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=${encodeURIComponent(form)}&company=&dateb=&owner=include&count=40&output=atom`;
    try {
      const feed = await parser.parseURL(url);
      for (const entry of feed.items || []) {
        // Raw summary contains the Item numbers we need
        const rawSummary = entry.summary || entry.content || entry['content:encoded'] || '';
        const plainSummary = extractSecItems(rawSummary);

        // Pick out explicit Item N.NN references and annotate with glossary
        const itemMatches = [...plainSummary.matchAll(/Item\s+(\d+\.\d+)\s*:?\s*([^\n]*?)(?=(?:Item\s+\d+\.\d+|$))/gi)];
        const itemLines = itemMatches.map(m => {
          const num = m[1];
          const desc = (m[2] || '').trim();
          const gloss = ITEM_GLOSS[num];
          return `Item ${num}: ${desc || gloss || ''}`.trim();
        });

        const formLabel = FORM_LABELS[form] || form;
        const bodyParts = [
          `Form ${form} — ${formLabel}`,
          plainSummary,
          itemLines.length ? `Items filed:\n${itemLines.join('\n')}` : '',
        ].filter(Boolean);

        items.push({
          source: 'sec_edgar',
          source_id: entry.id || entry.link,
          url: entry.link,
          headline: `[${form}] ${entry.title || ''}`.slice(0, 300),
          body: bodyParts.join('\n\n').slice(0, 4000),
          published_at: entry.pubDate || entry.isoDate || null,
        });
      }
    } catch (e) {
      console.error('[sec_edgar]', form, e.message);
    }
  }
  return items;
}

// ---- Google News RSS (global) ---------------------------------------------
// Broad coverage via keyword searches. Google News indexes LSE/RNS, Nasdaq Nordic,
// Euronext, Reuters, Bloomberg, FT, regional wires etc. — one pipe for everything.
//
// IMPORTANT: On Railway / cloud IPs, Google may return empty or blocked responses
// for non-browser UAs. We use native fetch with a Chrome UA and feed the XML into
// rss-parser's parseString.
async function fetchGoogleNewsQuery(name, q, geo, when) {
  const fullQ = when ? `${q} when:${when}` : q;
  const url = `https://news.google.com/rss/search?q=${encodeURIComponent(fullQ)}&hl=en-${geo}&gl=${geo}&ceid=${geo}:en`;
  try {
    const res = await fetch(url, {
      headers: {
        'user-agent': BROWSER_UA,
        'accept': 'application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
      },
    });
    if (!res.ok) {
      console.error(`[google_news:${name}] HTTP ${res.status}`);
      return [];
    }
    const xml = await res.text();
    if (!xml || xml.length < 100) {
      console.error(`[google_news:${name}] empty response (${xml?.length || 0} bytes)`);
      return [];
    }
    const feed = await googleParser.parseString(xml);
    const out = [];
    for (const entry of feed.items || []) {
      if (/^Google News$/i.test(entry.title || '')) continue;
      if ((entry.title || '').includes('when:')) continue;
      out.push({
        source: `google_news:${name}`,
        source_id: entry.guid || entry.link,
        url: entry.link,
        headline: (entry.title || '').slice(0, 300),
        body: entry.contentSnippet || entry.content || entry.summary || '',
        published_at: entry.pubDate || entry.isoDate || null,
      });
    }
    return out;
  } catch (e) {
    console.error(`[google_news:${name}]`, e.message);
    return [];
  }
}

async function fetchGoogleNews() {
  const queries = [
    { name: 'ma_global',   q: '"definitive agreement to acquire" OR "agreed to acquire"', geo: 'US', when: '2d' },
    { name: 'ma_cash',     q: '"all-cash deal" OR "cash merger" OR "take-private"', geo: 'US', when: '7d' },
    { name: 'uk_rule27',   q: '"Rule 2.7 announcement" OR "recommended cash offer" OR "possible offer"', geo: 'GB', when: '7d' },
    { name: 'eu_ma',       q: '"public takeover" OR "tender offer" OR "squeeze-out" Europe', geo: 'GB', when: '7d' },
    { name: 'spinoffs',    q: '"spin-off" OR "demerger" OR "separation into two companies"', geo: 'US', when: '7d' },
    { name: 'ipo_filing',  q: '"files for IPO" OR "filed S-1" OR "prospectus approved"', geo: 'US', when: '7d' },
    { name: 'ipo_eu',      q: '"intention to float" OR "IPO priced" Europe Nordic', geo: 'GB', when: '7d' },
    { name: 'spac',        q: '"business combination" SPAC OR "de-SPAC"', geo: 'US', when: '7d' },
    { name: 'tender',      q: '"tender offer" OR "Dutch auction" OR "self-tender"', geo: 'US', when: '7d' },
    { name: 'rights',      q: '"rights issue" OR "rights offering"', geo: 'GB', when: '7d' },
    { name: 'activist',    q: '"13D filing" OR "activist investor" OR "proxy contest"', geo: 'US', when: '7d' },
    { name: 'take_private', q: '"take private" OR "management buyout" OR "going private"', geo: 'US', when: '7d' },
    { name: 'nordic',      q: '"Nasdaq Stockholm" OR "Nasdaq Copenhagen" OR "Oslo Børs" acquisition OR merger OR IPO', geo: 'GB', when: '7d' },
    // Country-specific EU special-sits queries
    { name: 'de_spinoff',  q: '"Abspaltung" OR "Spin-off" Xetra OR Frankfurt', geo: 'DE', when: '14d' },
    { name: 'de_takeover', q: '"Übernahmeangebot" OR "Pflichtangebot" BaFin', geo: 'DE', when: '14d' },
    { name: 'fr_spinoff',  q: '"scission" OR "spin-off" Euronext Paris', geo: 'FR', when: '14d' },
    { name: 'fr_offer',    q: '"offre publique d\'achat" OR OPA', geo: 'FR', when: '14d' },
    { name: 'nl_spinoff',  q: '"demerger" OR "afsplitsing" Euronext Amsterdam', geo: 'NL', when: '14d' },
    { name: 'nl_offer',    q: '"openbaar bod" OR "tender offer" AFM', geo: 'NL', when: '14d' },
    { name: 'ch_spinoff',  q: '"spin-off" SIX Swiss Exchange', geo: 'CH', when: '14d' },
    { name: 'it_offer',    q: '"OPA" OR "offerta pubblica di acquisto" Borsa Italiana', geo: 'IT', when: '14d' },
  ];

  const results = await Promise.all(
    queries.map(({ name, q, geo, when }) => fetchGoogleNewsQuery(name, q, geo, when))
  );
  return results.flat();
}

// ---- PR Newswire M&A feed (US) ---------------------------------------------
async function fetchPrNewswireMa() {
  const url = 'https://www.prnewswire.com/rss/acquisitions-mergers-and-takeovers-latest-news/acquisitions-mergers-and-takeovers-latest-news-list.rss';
  try {
    const feed = await parser.parseURL(url);
    return (feed.items || []).map(e => ({
      source: 'prnewswire_ma',
      source_id: e.guid || e.link,
      url: e.link,
      headline: (e.title || '').slice(0, 300),
      body: e.contentSnippet || e.content || e.summary || '',
      published_at: e.pubDate || e.isoDate || null,
    }));
  } catch (e) {
    console.error('[prnewswire_ma]', e.message);
    return [];
  }
}

// ---- BusinessWire M&A feed (US + global) ----------------------------------
async function fetchBusinessWireMa() {
  // BusinessWire category feed for M&A
  const url = 'https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEVtRVg==';
  try {
    const feed = await parser.parseURL(url);
    return (feed.items || []).map(e => ({
      source: 'businesswire_ma',
      source_id: e.guid || e.link,
      url: e.link,
      headline: (e.title || '').slice(0, 300),
      body: e.contentSnippet || e.content || e.summary || '',
      published_at: e.pubDate || e.isoDate || null,
    }));
  } catch (e) {
    console.error('[businesswire_ma]', e.message);
    return [];
  }
}

// ---- Persist ---------------------------------------------------------------
async function saveRawItems(items) {
  let inserted = 0;
  for (const it of items) {
    if (!it.source_id) continue;
    try {
      const res = await query(
        `INSERT INTO raw_items (source, source_id, url, headline, body, published_at)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (source, source_id) DO NOTHING
         RETURNING id`,
        [it.source, it.source_id, it.url, it.headline, it.body, it.published_at]
      );
      if (res.length) inserted++;
    } catch (e) {
      console.error('[saveRawItems]', e.message);
    }
  }
  return inserted;
}

async function fetchAll() {
  const results = await Promise.all([
    fetchSecEdgar().catch(e => (console.error('sec', e.message), [])),
    fetchGoogleNews().catch(e => (console.error('gnews', e.message), [])),
    fetchPrNewswireMa().catch(e => (console.error('prn', e.message), [])),
    fetchBusinessWireMa().catch(e => (console.error('bw', e.message), [])),
  ]);
  const all = results.flat();
  console.log(`[feeds] fetched sec=${results[0].length} gnews=${results[1].length} prn=${results[2].length} bw=${results[3].length}`);
  const inserted = await saveRawItems(all);
  return { fetched: all.length, inserted };
}

module.exports = { fetchAll, saveRawItems };
