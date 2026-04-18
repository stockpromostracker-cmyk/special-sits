// Nordic / European MAR-grade disclosures.
//
// Primary: MFN.se RSS feed — the Modular Finance Nordic aggregator that
// carries MAR-regulated press releases for SE / DK / NO / FI listed issuers.
// The feed is keyword-unfiltered so we pull everything and match headlines
// against our special-situations keywords in multiple Nordic languages.
//
// Secondary (future): DGAP/EQS for DE, Euronext official disclosures for
// FR/NL/BE/PT/IE — not implemented here because those platforms require
// per-release scraping without stable APIs. For now we rely on news-feed
// enrichment to catch Continental European events.

const UA = 'Mozilla/5.0 (compatible; SpecialSits/1.0)';

// Multilingual keyword → event type. Case-insensitive substring matching.
// Swedish / Norwegian / Danish / Finnish variants included.
const KEYWORDS = [
  // Spin-off / demerger
  { re: /\b(spin[- ]?off|demerger|carve[- ]?out)\b/i,     event: 'demerger_pending', deal: 'spin_off' },
  { re: /\bavknoppning\b/i,                                event: 'demerger_pending', deal: 'spin_off' }, // SE
  { re: /\butdeling av aksjer\b/i,                         event: 'demerger_pending', deal: 'spin_off' }, // NO
  { re: /\bjakautuminen\b/i,                               event: 'demerger_pending', deal: 'spin_off' }, // FI
  // Intention to float / IPO listing
  { re: /\b(intention to (float|list)|first day of trading|listing on)\b/i, event: 'ipo_recent',   deal: 'ipo' },
  { re: /\b(initial public offering|ipo)\b/i,              event: 'ipo_pending', deal: 'ipo' },
  { re: /\b(b\u00f6rsintroduktion|b\u00f6rsnotering|notering p\u00e5)\b/i, event: 'ipo_pending', deal: 'ipo' }, // SE/DK
  { re: /\blisteoppf\u00f8ring\b/i,                        event: 'ipo_pending', deal: 'ipo' }, // NO
  // Public offer / scheme / take-private
  { re: /\b(public cash offer|recommended (public )?offer|tender offer)\b/i, event: 'merger_pending', deal: 'merger_arb' },
  { re: /\b(offentligt .*erbjudande|\bbud p\u00e5\b)\b/i,  event: 'merger_pending', deal: 'merger_arb' }, // SE
  { re: /\b(pliktig tilbud|frivilligt tilbud)\b/i,         event: 'merger_pending', deal: 'merger_arb' }, // NO/DK
  { re: /\bostotarjous\b/i,                                event: 'merger_pending', deal: 'merger_arb' }, // FI
];

// Nordic country codes we accept. MFN's <x:scope> can be a single ISO code
// (SE / DK / NO / FI / IS), a comma-separated list ("SE,DK"), or a bloc tag
// ("EU", "Nordic"). We extract the *first* Nordic code we find, falling back
// to null if none present.
const NORDIC_CODES = ['SE', 'DK', 'NO', 'FI', 'IS'];
function pickNordicCountry(scope) {
  if (!scope) return null;
  const parts = String(scope).toUpperCase().split(/[,;\s/|]+/).filter(Boolean);
  for (const p of parts) if (NORDIC_CODES.includes(p)) return p;
  // Bloc tags — assume SE if pure EU/Nordic (most MFN issuers)
  if (parts.some(p => ['EU', 'NORDIC', 'SCANDINAVIA'].includes(p))) return 'SE';
  return null;
}

function parseRssItems(xml) {
  const items = [];
  const itemRe = /<item>([\s\S]*?)<\/item>/gi;
  let m;
  while ((m = itemRe.exec(xml)) !== null) {
    const body = m[1];
    const get = (tag) => {
      const rx = new RegExp(`<${tag}[^>]*>(?:<!\\[CDATA\\[)?([\\s\\S]*?)(?:\\]\\]>)?</${tag}>`, 'i');
      const mm = body.match(rx);
      return mm ? mm[1].trim() : null;
    };
    const scopeM = body.match(/<x:scope>([^<]+)<\/x:scope>/i);
    const tagMatches = [...body.matchAll(/<x:tag>([^<]+)<\/x:tag>/gi)].map(t => t[1]);
    items.push({
      title: get('title'),
      link: get('link'),
      description: stripTags(get('description')),
      pubDate: get('pubDate'),
      scope: scopeM ? scopeM[1] : null,
      tags: tagMatches,
    });
  }
  return items;
}

function stripTags(s) {
  return String(s || '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&nbsp;/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// Try to pull ticker from description — Nordic releases usually include
// "(Nasdaq Stockholm: CANTA)" or "(Oslo B\u00f8rs: XXL)" etc.
function extractTicker(description) {
  if (!description) return null;
  const m = description.match(/\((?:Nasdaq (?:Stockholm|Copenhagen|Helsinki)|Oslo\s*B[\u00f8o]rs|Euronext Growth Oslo|First North[^:]*):\s*([A-Z0-9.\-]{2,10})\s*\)/i);
  return m ? m[1] : null;
}

function extractIssuer(title, description) {
  // MFN titles usually start with the issuer name. We take everything up to
  // the first long verb phrase. Heuristic: cut at " announces", " reports",
  // " proposes", " to ", etc.
  const s = title || '';
  const m = s.match(/^([A-Z][\w&\s\u00c0-\u024f\-\.\/]+?)\s+(announces|announces\s+the|proposes|reports|invites|today|to launch|intends|publish|receives|release|commences|initiates|completes|resolves|has decided|to acquire|reveals|unveils|confirms|notifies|invites)\b/i);
  if (m) return m[1].trim();
  // Fallback: description may have "CompanyName (Exchange: TICKER)" — take before "("
  const d = description || '';
  const dm = d.match(/^([A-Z][\w&\s\u00c0-\u024f\-\.\/]+?)\s*\(/);
  return dm ? dm[1].trim() : null;
}

function classifyItem(item) {
  const hay = `${item.title || ''}\n${item.description || ''}`;
  for (const { re, event, deal } of KEYWORDS) {
    if (re.test(hay)) return { event_type: event, deal_type: deal };
  }
  return null;
}

function makeDeal(item) {
  const classification = classifyItem(item);
  if (!classification) return null;
  const country = pickNordicCountry(item.scope);
  if (!country) return null;  // We only keep items tagged with a Nordic scope
  const ticker = extractTicker(item.description);
  const issuer = extractIssuer(item.title, item.description) || item.title?.slice(0, 60);
  const isCompleted = classification.event_type === 'ipo_recent';
  const announceDate = item.pubDate ? new Date(item.pubDate).toISOString().slice(0, 10) : null;
  const ticker_prefix = { SE: 'STO', DK: 'CPH', NO: 'OSE', FI: 'HEL', IS: 'ICE' }[country] || country;
  const prefixedTicker = ticker ? `${ticker_prefix}:${ticker}` : null;

  return {
    event_type: classification.event_type,
    data_source_tier: 'official',
    primary_source: 'mfn',
    source_filing_url: item.link,
    confidence: 0.9,   // Slightly less than SEC filings (keyword-based classification)
    deal_type: classification.deal_type,
    status: isCompleted ? 'completed' : 'announced',
    region: 'EU',
    country,
    primary_ticker: prefixedTicker,
    target_name: issuer,
    target_ticker: prefixedTicker,
    parent_name: classification.event_type.startsWith('demerger') ? issuer : null,
    parent_ticker: classification.event_type.startsWith('demerger') ? prefixedTicker : null,
    headline: `${issuer} — ${item.title}`,
    summary: item.description?.slice(0, 500) || item.title,
    announce_date: announceDate,
    filing_date: announceDate,
    key_dates: { filing_date: announceDate },
    external_key: `mfn:${item.link}`,
  };
}

// Fetch the MFN firehose RSS, which returns the ~50 latest items.
// We also walk backwards via the ?before=<pubDate> param to get an older
// window. Each page returns at most ~50 items; we stop when either we hit the
// configured pageLimit or the feed returns an empty page.
async function fetchRssPages(pageLimit = 6) {
  const all = [];
  const seenLinks = new Set();
  let before = null;
  for (let page = 0; page < pageLimit; page++) {
    const url = before ? `https://mfn.se/a.rss?before=${encodeURIComponent(before)}` : 'https://mfn.se/a.rss';
    let xml;
    try {
      const res = await fetch(url, {
        headers: { 'User-Agent': UA, 'Accept': 'application/rss+xml,application/xml' },
      });
      if (!res.ok) { console.warn(`[nordic] page ${page} HTTP ${res.status}`); break; }
      xml = await res.text();
    } catch (e) {
      console.warn(`[nordic] page ${page} fetch failed:`, e.message);
      break;
    }
    const items = parseRssItems(xml);
    if (!items.length) break;
    let newCount = 0;
    let oldestDate = null;
    for (const it of items) {
      const key = it.link || `${it.title}|${it.pubDate}`;
      if (!seenLinks.has(key)) { seenLinks.add(key); all.push(it); newCount++; }
      if (it.pubDate && (!oldestDate || new Date(it.pubDate) < new Date(oldestDate))) {
        oldestDate = it.pubDate;
      }
    }
    if (newCount === 0 || !oldestDate) break;
    // Step back one second from the oldest item so we don't re-fetch it.
    const d = new Date(oldestDate); d.setSeconds(d.getSeconds() - 1);
    before = d.toISOString();
  }
  return all;
}

async function fetchAll() {
  const items = await fetchRssPages(8); // ~400 items back, covers ~120-180 days
  const deals = items.map(makeDeal).filter(Boolean);

  // De-dupe
  const seen = new Set();
  const deduped = deals.filter(d => {
    if (seen.has(d.external_key)) return false;
    seen.add(d.external_key); return true;
  });
  return { count: deduped.length, items_scanned: items.length, deals: deduped };
}

module.exports = { fetchAll, fetchRssPages, parseRssItems, classifyItem, pickNordicCountry, KEYWORDS };
