// Pre-filter raw items BEFORE calling Gemini, to minimize API cost.
//
// Two independent gates:
//   1) keywordGate  — headline+body must contain at least one special-sits trigger phrase
//   2) secItemGate  — SEC 8-K filings without a relevant Item code are skipped
//
// If an item fails the prefilter, it gets status = 'skipped_prefilter' and no Gemini call is made.
// If the item is high-signal (SEC S-1/F-1/S-4/SC TO/etc., PR Newswire M&A), it bypasses the
// keyword gate because the source itself is enough signal.

// Trigger keywords — kept broad but high-signal. Matched case-insensitively as substrings.
// Phrases chosen so that routine earnings / product / exec news doesn't match.
const TRIGGER_KEYWORDS = [
  // M&A / take-private
  'acquire', 'acquisition', 'acquires', 'acquired',
  'merger', 'merge with', 'merging',
  'definitive agreement', 'merger agreement', 'transaction agreement',
  'take private', 'take-private', 'go private', 'going private',
  'management buyout', 'mbo ', 'lbo ', 'leveraged buyout',
  'all-cash', 'cash offer', 'cash consideration',
  'scheme of arrangement', 'rule 2.7', 'recommended offer',
  'tender offer', 'dutch auction', 'self-tender', 'self tender',
  'squeeze-out', 'squeeze out', 'mandatory offer',
  'business combination', 'de-spac', 'de spac',
  // Spin-offs
  'spin-off', 'spinoff', 'spin off', 'demerger', 'de-merger',
  'separation', 'separate into', 'separation into',
  // IPO
  'ipo', 'initial public offering', 'direct listing',
  'files for ipo', 'filed s-1', 'filed f-1', 'prospectus',
  'intention to float', 'itf ', 'priced at', 'pricing of',
  // Rights / buybacks
  'rights issue', 'rights offering', 'preemptive rights',
  'share buyback', 'share repurchase', 'tender for own',
  // Activist / governance
  '13d filing', 'schedule 13d', 'activist', 'proxy contest', 'proxy fight',
  'withhold vote', 'board nominee',
  // Liquidation
  'liquidation', 'wind-down', 'wind down', 'dissolve',
  // Structure
  'share class', 'dual-class', 'unification',
];

// SEC 8-K Item codes that carry special-situation signal.
// Other items (2.02 earnings, 5.02 director appointments, 7.01 Reg FD, 9.01 exhibits) are noise.
const INTERESTING_8K_ITEMS = new Set([
  '1.01', // Entry into Material Definitive Agreement (M&A, spin, etc.)
  '1.02', // Termination of Material Definitive Agreement
  '1.03', // Bankruptcy or Receivership
  '2.01', // Completion of Acquisition/Disposition
  '2.05', // Costs Associated with Exit/Disposal (restructuring)
  '2.06', // Material Impairments
  '3.01', // Delisting notice
  '3.03', // Material Modification to Rights of Security Holders
  '5.01', // Change in Control
  '5.03', // Amendments to Articles (often share class / dual class)
  '8.01', // Other Events (often where M&A press releases live)
]);

// Sources that bypass the keyword gate — the source itself is enough signal.
// (SEC form-level filtering is still applied separately below.)
const HIGH_SIGNAL_SOURCES = new Set([
  'prnewswire_ma',
  'businesswire_ma',
  'email',
]);

function hasAnyKeyword(text) {
  if (!text) return false;
  const lower = text.toLowerCase();
  return TRIGGER_KEYWORDS.some(kw => lower.includes(kw));
}

// Parse "Item N.NN" codes out of SEC body text.
function extractItemCodes(body) {
  if (!body) return [];
  const matches = [...String(body).matchAll(/Item\s+(\d+\.\d+)/gi)];
  return [...new Set(matches.map(m => m[1]))];
}

// Returns { pass: boolean, reason: string }
function prefilter({ source, headline, body }) {
  const text = `${headline || ''}\n${body || ''}`;

  // 1) SEC-specific logic — handle BEFORE the generic keyword gate so SEC items
  //    that lack generic keywords in the summary (but have interesting Item codes)
  //    still get through.
  if (source === 'sec_edgar') {
    // Pull form type out of headline, e.g. "[8-K] 8-K - BLOOMIA ..."
    const formMatch = (headline || '').match(/^\[([^\]]+)\]/);
    const form = formMatch ? formMatch[1].trim() : '';

    // Non-8K SEC forms (S-1, F-1, S-4, SC TO, SC 13E3, 10-12B, 425) are always high-signal.
    if (form && form !== '8-K') {
      return { pass: true, reason: `sec form ${form}` };
    }

    // 8-Ks: require an interesting item code.
    const items = extractItemCodes(body);
    const interesting = items.filter(i => INTERESTING_8K_ITEMS.has(i));
    if (interesting.length === 0) {
      return { pass: false, reason: `8-K with no interesting items (has ${items.join(',') || 'none'})` };
    }
    return { pass: true, reason: `8-K Item(s) ${interesting.join(',')}` };
  }

  // 2) High-signal sources bypass the keyword gate.
  if (HIGH_SIGNAL_SOURCES.has(source)) {
    return { pass: true, reason: `high-signal source ${source}` };
  }

  // 3) Everything else (Google News etc.) must hit a trigger keyword.
  if (!hasAnyKeyword(text)) {
    return { pass: false, reason: 'no trigger keyword' };
  }
  return { pass: true, reason: 'keyword match' };
}

// Normalize a headline for dedup purposes:
// lowercase, strip punctuation, collapse whitespace, drop common filler.
function normalizeHeadline(headline) {
  if (!headline) return '';
  return String(headline)
    .toLowerCase()
    .replace(/\[[^\]]+\]/g, '')           // strip [8-K] prefix
    .replace(/\([^)]*\)/g, '')            // strip "(NYSE: ABC)" style tags
    .replace(/[^\p{L}\p{N}\s]/gu, ' ')    // punctuation to space
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 160);
}

module.exports = {
  prefilter,
  normalizeHeadline,
  TRIGGER_KEYWORDS,
  INTERESTING_8K_ITEMS,
};
