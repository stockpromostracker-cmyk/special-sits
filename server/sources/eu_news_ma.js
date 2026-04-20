// Continental European M&A — Google News RSS fallback poller.
//
// For DE/NL/IT/ES/BE we don't have a clean regulator JSON endpoint
// (BaFin/Consob/AFM/CNMV/FSMA are either hostile SPAs or 404'd). Google News
// RSS + site operators give us a daily-fresh stream of take-private / public-
// offer headlines without scraping each regulator directly.
//
// Each target country has a focused `site:<regulator>` + M&A-keyword query
// so we only pull relevant news (not generic market coverage). Confidence is
// 0.7 (lower than regulator-direct because it's aggregated news).

const UA = 'Mozilla/5.0 (compatible; SpecialSits/1.0; +https://special-sits.example)';

// Per-country: { country, language, queries: [{q, deal_type, event_type}] }
// Queries target reputable news outlets (not regulator PDF indexes — those
// return noisy document titles). Reuters/FT/Handelsblatt/Les Echos/NRC/IlSole
// give clean "Bidder offers to buy Target" style headlines with the target
// name near the front.
const COUNTRIES = [
  {
    country: 'DE',
    language: 'de',
    queries: [
      { q: 'Germany tender offer takeover bid site:reuters.com',  event_type: 'merger_pending', deal_type: 'merger_arb' },
      { q: 'Germany "takeover offer" OR "public tender" site:ft.com', event_type: 'merger_pending', deal_type: 'merger_arb' },
      { q: '"Übernahmeangebot" site:handelsblatt.com',            event_type: 'merger_pending', deal_type: 'merger_arb' },
      { q: '"Delisting-Angebot" site:handelsblatt.com',           event_type: 'merger_pending', deal_type: 'merger_arb' },
    ],
  },
  {
    country: 'NL',
    language: 'en',
    queries: [
      { q: 'Netherlands tender offer takeover site:reuters.com',  event_type: 'merger_pending', deal_type: 'merger_arb' },
      { q: 'Amsterdam "public offer" bid site:reuters.com',       event_type: 'merger_pending', deal_type: 'merger_arb' },
      { q: 'Euronext Amsterdam "takeover bid" site:ft.com',       event_type: 'merger_pending', deal_type: 'merger_arb' },
    ],
  },
  {
    country: 'IT',
    language: 'en',
    queries: [
      { q: 'Italy "tender offer" takeover site:reuters.com',      event_type: 'merger_pending', deal_type: 'merger_arb' },
      { q: 'Borsa Italiana takeover bid site:reuters.com',        event_type: 'merger_pending', deal_type: 'merger_arb' },
      { q: 'Milan OPA takeover site:ft.com',                      event_type: 'merger_pending', deal_type: 'merger_arb' },
    ],
  },
  {
    country: 'ES',
    language: 'en',
    queries: [
      { q: 'Spain "tender offer" takeover site:reuters.com',      event_type: 'merger_pending', deal_type: 'merger_arb' },
      { q: 'Madrid CNMV takeover bid site:reuters.com',           event_type: 'merger_pending', deal_type: 'merger_arb' },
    ],
  },
  {
    country: 'BE',
    language: 'en',
    queries: [
      { q: 'Belgium Brussels takeover bid site:reuters.com',      event_type: 'merger_pending', deal_type: 'merger_arb' },
      { q: 'Euronext Brussels "public takeover" site:ft.com',     event_type: 'merger_pending', deal_type: 'merger_arb' },
    ],
  },
  {
    country: 'AT',
    language: 'en',
    queries: [
      { q: 'Austria Vienna takeover bid site:reuters.com',        event_type: 'merger_pending', deal_type: 'merger_arb' },
    ],
  },
];

function rssUrl(q, lang, country) {
  const hl = lang || 'en';
  const gl = country || 'US';
  return `https://news.google.com/rss/search?q=${encodeURIComponent(q)}&hl=${hl}&gl=${gl}&ceid=${gl}:${hl}`;
}

async function fetchRss(url) {
  const res = await fetch(url, {
    headers: {
      'User-Agent': UA,
      'Accept': 'application/rss+xml,application/xml,text/xml',
    },
  });
  if (!res.ok) throw new Error(`Google News RSS HTTP ${res.status}`);
  return res.text();
}

// Minimal RSS parser — matches <item>...</item> blocks
function parseRssItems(xml) {
  const items = [];
  const itemRe = /<item>([\s\S]*?)<\/item>/g;
  let m;
  while ((m = itemRe.exec(xml)) !== null) {
    const body = m[1];
    const get = (tag) => {
      const r = body.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)</${tag}>`, 'i'));
      if (!r) return null;
      let v = r[1].trim();
      v = v.replace(/^<!\[CDATA\[([\s\S]*?)\]\]>$/, '$1').trim();
      return v;
    };
    items.push({
      title: get('title'),
      link: get('link'),
      pubDate: get('pubDate'),
      description: get('description'),
      source: get('source'),
    });
  }
  return items;
}

// Extract a best-guess company name from a Google News headline.
// Strategy:
//   1. Drop the trailing source suffix (" — Reuters", " - Bafin", etc.)
//   2. Look for "for <COMPANY> by", "of <COMPANY>", "Übernahmeangebot ... der <COMPANY>"
//   3. If headline starts with "COMPANY:", take prefix (short form)
//   4. Reject overly long / noisy headlines (>80 chars of pure text) — likely
//      a PDF cover dumped as headline. Skip those rather than polluting DB.
// Boilerplate legal/regulatory phrases that indicate this is a PDF cover
// page or statutory notice — NOT a company name. Rejecting these hard.
const NOISE_PHRASES = [
  /compulsory publication/i,
  /pflichtveröffentlichung/i,
  /pflichtmitteilung/i,
  /in accordance with section/i,
  /gemäß\s+§/i,
  /angebotsunterlage/i,
  /offer document/i,
  /angebot\s+an\s+die\s+aktionäre/i,
  /offre publique/i,  // French boilerplate "public offer" without company name
  /openbaar bod van/i,
  /tender offer statement/i,
  /decision of the/i,
  // Column / opinion pieces — not deal filings
  /^breakingviews/i,
  /^opinion[:\s]/i,
  /^analysis[:\s]/i,
  /^explainer[:\s]/i,
  /^column[:\s]/i,
  /^factbox/i,
  /^timeline[:\s]/i,
  /^explained[:\s]/i,
  /^update\s+\d/i,  // "UPDATE 1-..." Reuters wire updates
  // Generic market chatter without a specific target
  /\bshares\s+(?:rise|fall|jump|tumble|slide|surge)/i,
  /\b(?:weighs|considers|explores|eyes|mulls)\b/i,  // rumor-stage, no firm offer
  /m&a\s+(?:activity|deals|volumes|outlook|boom|slump)/i,
  /\bdeal[s]?\s+(?:of the week|roundup|recap)/i,
];

// Must contain at least one of these to be a credible deal headline.
// Filters out unrelated business news that slips through the site: query.
const DEAL_VERBS = /\b(?:takeover|tender offer|public offer|public bid|buyout|acquires?|acquired|acquiring|acquisition of|to buy|to acquire|bid for|offer for|offers for|offered for|OPA|Übernahme|Übernahmeangebot|Angebot für|openbaar bod|bod op|oferta pública|delisting)\b/i;

function extractCompany(title) {
  if (!title) return null;
  let t = title.replace(/&amp;/g, '&').replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/<[^>]+>/g, '').trim();

  // 1. Strip trailing source attribution (" — Reuters", " - BaFin", etc.)
  t = t.replace(/\s[-—–]\s[^—–-]{1,40}$/, '').trim();

  // 2. Reject obvious PDF-dump / noise headlines
  const noSpacesLen = t.replace(/\s/g, '').length;
  const wordCount = t.split(/\s+/).length;
  if (noSpacesLen > 120 || wordCount > 18) return null;
  // Reject the common BaFin PDF cover pattern: lots of letter-space-letter
  if (/(?:[A-Z]\s){6,}/.test(t)) return null;
  // Reject legal/editorial boilerplate & rumor-stage headlines
  for (const re of NOISE_PHRASES) if (re.test(t)) return null;

  // 3. MUST mention an actual deal verb — otherwise it's generic news
  if (!DEAL_VERBS.test(t)) return null;

  // 4. Extract target from structured patterns. These are strict: require
  //    explicit deal language + the target noun-phrase immediately after.
  const patterns = [
    // English
    /(?:takeover|tender|public)\s+(?:offer|bid)\s+(?:for|on|of)\s+([A-Z][\w&\.\-]*(?:\s+[A-Z0-9&\.\-][\w&\.\-]*){0,5})/,
    /(?:mandatory|voluntary|friendly|hostile)\s+offer\s+for\s+([A-Z][\w&\.\-]*(?:\s+[A-Z0-9&\.\-][\w&\.\-]*){0,5})/,
    /acquires?\s+([A-Z][\w&\.\-]*(?:\s+[A-Z0-9&\.\-][\w&\.\-]*){0,5})/,
    /to\s+(?:buy|acquire)\s+([A-Z][\w&\.\-]*(?:\s+[A-Z0-9&\.\-][\w&\.\-]*){0,5})/,
    /bid\s+for\s+([A-Z][\w&\.\-]*(?:\s+[A-Z0-9&\.\-][\w&\.\-]*){0,5})/,
    /(?:buyout|acquisition)\s+of\s+([A-Z][\w&\.\-]*(?:\s+[A-Z0-9&\.\-][\w&\.\-]*){0,5})/,
    // German
    /Übernahmeangebot[\s\S]*?(?:für|der|an)\s+(?:die\s+)?([A-ZÄÖÜ][\wÄÖÜäöüß&\.\-]*(?:\s+[A-ZÄÖÜ0-9&\.\-][\wÄÖÜäöüß&\.\-]*){0,4}(?:\s+(?:AG|SE|GmbH|KGaA))?)/,
    /Übernahme\s+(?:der|von)\s+([A-ZÄÖÜ][\wÄÖÜäöüß&\.\-]*(?:\s+[A-ZÄÖÜ0-9&\.\-][\wÄÖÜäöüß&\.\-]*){0,4})/,
    // French
    /OPA\s+(?:sur|de)\s+([A-ZÀ-Ý][\wÀ-ÿ&\.\-]*(?:\s+[A-ZÀ-Ý0-9&\.\-][\wÀ-ÿ&\.\-]*){0,4})/,
    // Dutch
    /bod\s+op\s+([A-Z][\w&\.\-]*(?:\s+[A-Z0-9&\.\-][\w&\.\-]*){0,4})/,
    // Spanish
    /oferta\s+(?:pública|de\s+adquisición)\s+(?:por|sobre|de)\s+([A-ZÑ][\wÑñ&\.\-]*(?:\s+[A-ZÑ0-9&\.\-][\wÑñ&\.\-]*){0,4})/i,
  ];
  for (const re of patterns) {
    const m = t.match(re);
    if (m && m[1]) {
      let name = m[1].trim().replace(/\s+/g, ' ');
      // Clean trailing connector words
      name = name.replace(/\s+(?:by|at|from|from|of|for|in|with|on|to)$/i, '').trim();
      // Reject generic words as "names"
      if (/^(?:The|A|An|Its|This|That|Company|Group|Shares|Stock)$/i.test(name)) continue;
      // Reject bare country / region names — patterns like "to buy Italy's Iveco" misfire
      if (/^(?:Italy|Germany|France|Spain|Britain|UK|U\.S\.|US|USA|America|American|Europe|European|Asia|Asian|China|Chinese|Japan|Japanese|Korea|Korean|India|Indian|Russia|Russian|Netherlands|Dutch|Norway|Norwegian|Sweden|Swedish|Denmark|Danish|Finland|Finnish|Switzerland|Swiss|Austria|Austrian|Belgium|Belgian|Poland|Polish|Portugal|Portuguese|Greece|Greek|Turkey|Turkish|Brazil|Brazilian|Mexico|Mexican|Canadian|Australian|Slovakia|Slovak|Czech|Hungarian|Romanian|Bulgarian|Ukrainian|Irish|Ireland|Scottish|English|Welsh|Indonesian|Thai|Vietnamese|Malaysian|Singapore|Singaporean|Israeli|Saudi|Emirati)(?:'s)?$/i.test(name)) continue;
      // Reject trailing "-sources" / "-letter" / "-update" artifacts
      name = name.replace(/\s*-\s*(?:sources?|letter|update|exclusive|report|reports)$/i, '').trim();
      if (name.length >= 3 && name.length <= 60) return name;
    }
  }

  // No clean extraction — drop rather than pollute DB
  return null;
}

function makeDeal({ item, country, event_type, deal_type }) {
  const pub = item.pubDate ? new Date(item.pubDate) : null;
  if (!pub || isNaN(pub.getTime())) return null;
  const announceDate = pub.toISOString().slice(0, 10);
  const target = extractCompany(item.title);
  if (!target || target.length < 3) return null;

  return {
    event_type,
    data_source_tier: 'aggregator',            // news-derived
    primary_source: 'google_news_rss',
    source_filing_url: item.link,
    confidence: 0.7,
    deal_type,
    status: 'announced',
    region: 'EU',
    country,
    primary_ticker: null,
    target_name: target,
    target_ticker: null,
    headline: item.title,
    summary: item.description ? item.description.replace(/<[^>]+>/g, '').slice(0, 400) : null,
    announce_date: announceDate,
    filing_date: announceDate,
    key_dates: { filing_date: announceDate },
    external_key: `gnews_ma:${item.link}`,
  };
}

async function fetchAll() {
  const deals = [];
  const seen = new Set();
  let scanned = 0;
  let errors = 0;

  for (const c of COUNTRIES) {
    for (const q of c.queries) {
      try {
        const xml = await fetchRss(rssUrl(q.q, c.language, c.country));
        const items = parseRssItems(xml);
        scanned += items.length;
        for (const it of items) {
          const d = makeDeal({ item: it, country: c.country, event_type: q.event_type, deal_type: q.deal_type });
          if (!d) continue;
          // Dedupe globally by (target, month) — same headline surfaces in
          // multiple country feeds (e.g. Banco BPM in IT, NL, ES).
          const k = `${d.target_name.toLowerCase()}|${d.announce_date.slice(0,7)}`;
          if (seen.has(k)) continue;
          seen.add(k);
          deals.push(d);
        }
      } catch (e) {
        errors++;
      }
      // Gentle throttle between Google queries
      await new Promise(r => setTimeout(r, 300));
    }
  }
  return { count: deals.length, items_scanned: scanned, errors, deals };
}

module.exports = { fetchAll, parseRssItems, extractCompany, COUNTRIES };
