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

// Extract ISO country from MFN scope field
const SCOPE_TO_COUNTRY = { SE: 'SE', DK: 'DK', NO: 'NO', FI: 'FI', IS: 'IS' };

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
  const country = SCOPE_TO_COUNTRY[item.scope] || null;
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

async function fetchAll() {
  const res = await fetch('https://mfn.se/a.rss', {
    headers: { 'User-Agent': UA, 'Accept': 'application/rss+xml,application/xml' },
  });
  if (!res.ok) throw new Error(`MFN RSS ${res.status}`);
  const xml = await res.text();
  const items = parseRssItems(xml);
  const deals = items.map(makeDeal).filter(Boolean);

  // De-dupe
  const seen = new Set();
  const deduped = deals.filter(d => {
    if (seen.has(d.external_key)) return false;
    seen.add(d.external_key); return true;
  });
  return { count: deduped.length, items_scanned: items.length, deals: deduped };
}

module.exports = { fetchAll, parseRssItems, classifyItem, KEYWORDS };
