// Swiss Takeover Board (takeover.ch) — CH public-offer register.
//
// The TOB exposes /transactions/all with one <article class="transaction">
// per filing. Each has:
//   - id="transactionNNNN" (internal id)
//   - data-tooltip="Transaktions-Nr.: NNNN" (public case number)
//   - <h3>COMPANY NAME <span>(YYYY)</span></h3> (target + year)
//   - data-tooltip="Transaktionseigenschaft: ..." (deal characteristics,
//     e.g. Rückkaufprogramm = buyback, öffentliches Kaufangebot = tender,
//     Dekotierungsgesuch = delisting request)
//
// We skip pure buyback programmes ("Rückkaufprogramm") which aren't
// takeovers. Everything else is classified as merger_arb.

const UA = 'Mozilla/5.0 (compatible; SpecialSits/1.0; +https://special-sits.example)';

const TRANSACTIONS_URL = 'https://www.takeover.ch/transactions/all?language=en';
const DETAIL_URL = (num) => `https://www.takeover.ch/transactions/detail/nr/${num}`;

// The Swiss TOB serves tooltips in English when Accept-Language is en-*,
// German otherwise. We match both forms. Takeover flag regex is inclusive
// across languages.
const TAKEOVER_FLAGS_RE = [
  // German
  /\beingeleitetes\s+angebot\b/i,           // friendly/unfriendly announced offer
  /\bbeendetes\s+angebot\b/i,               // friendly/unfriendly completed offer
  /\bfreiwillige?\s+angebote?\b/i,          // voluntary offer(s)
  /\bpflichtangebote?\b/i,                  // mandatory offer(s)
  /\bkonkurrierende?\s+angebote?\b/i,       // competing offer(s)
  /\bbarangebot\b/i,                        // cash offer
  /\btauschangebot\b/i,                     // share exchange offer
  /gemischtes\s+angebot/i,                  // mixed (cash + stock)
  /\böffentliches\s+(?:kauf|tausch|umtausch|übernahme)\s*angebot\b/i,
  /\bdekotierung/i,                          // delisting petition
  /\bkraftloserklärung\b/i,                  // squeeze-out
  /ausschluss\s+der\s+minderheits/i,         // minority squeeze-out
  /\bopting\s*out\b/i,
  // English
  /(?:friendly|unfriendly|hostile)\s+(?:initiated|launched|announced|completed|terminated)\s+offer/i,
  /\bmandatory\s+offers?\b/i,
  /\bvoluntary\s+offers?\b/i,
  /\bcompeting\s+offers?\b/i,
  /\bcash\s+offer\b/i,
  /\bexchange\s+offer\b/i,
  /\bmixed\s+offer\b/i,
  /\bpublic\s+(?:purchase|exchange|takeover)\s+offer\b/i,
  /\bdelisting\s+(?:request|petition)\b/i,
  /\bsqueeze[- ]?out\b/i,
  /minority\s+shareholder/i,
];

async function fetchHtml(url) {
  const res = await fetch(url, {
    headers: {
      'User-Agent': UA,
      'Accept': 'text/html,application/xhtml+xml',
      'Accept-Language': 'en-GB,en;q=0.9',
    },
  });
  if (!res.ok) throw new Error(`Swiss TOB HTTP ${res.status}`);
  return res.text();
}

function parseTransactions(html) {
  const out = [];
  const articleRe = /<article[^>]*class="transaction[^"]*"[^>]*id="transaction(\d+)"[\s\S]*?<\/article>/g;
  let m;
  while ((m = articleRe.exec(html)) !== null) {
    const inner = m[0];
    const tobId = m[1];

    // Transaction number
    const numMatch = inner.match(/Transaktions[-\s]?Nr\.?:?\s*(\d+)/i);
    const num = numMatch ? numMatch[1] : null;

    // Target company from <h3>NAME <span>(YEAR)</span></h3>
    let target = null;
    let year = null;
    const h3Match = inner.match(/<h3>([\s\S]*?)<\/h3>/);
    if (h3Match) {
      const rawH3 = h3Match[1];
      const yMatch = rawH3.match(/<span>\((\d{4})\)<\/span>/);
      if (yMatch) year = yMatch[1];
      target = rawH3.replace(/<span[\s\S]*?<\/span>/g, '')
                    .replace(/<[^>]+>/g, '')
                    .replace(/\s+/g, ' ')
                    .trim();
    }
    if (!target) {
      const tt = inner.match(/<span[^>]*class="col2[^"]*"[^>]*data-tooltip="([^"]+)"/);
      if (tt) target = tt[1].trim();
    }

    // Deal characteristic flags. Server returns German "Transaktionseigenschaft:"
    // or English "Transaction properties:" depending on Accept-Language.
    const typeMatch = inner.match(/data-tooltip="(?:Transaktionseigenschaft|Transaction\s+properties):\s*([^"]+)"/i);
    const flagsRaw = typeMatch ? typeMatch[1].split(/<br\s*\/?>/).map(s => s.trim()).filter(Boolean) : [];
    const hasTakeoverFlag = flagsRaw.some(f => TAKEOVER_FLAGS_RE.some(re => re.test(f)));
    // A proper takeover MUST have at least one TAKEOVER_FLAGS_RE match.
    // Filters out pure buyback programmes, reporting-exemption filings, and
    // no-mandatory-offer determinations — none are takeover bids.
    const isBuybackOnly = !hasTakeoverFlag;

    // Detail URL
    const detailMatch = inner.match(/href="(\/transactions\/detail\/nr\/\d+)"/);
    const sourceUrl = detailMatch
      ? `https://www.takeover.ch${detailMatch[1]}`
      : (num ? DETAIL_URL(num) : 'https://www.takeover.ch/transactions/all');

    if (!target || target.length < 2) continue;

    out.push({
      tobId,
      transactionNumber: num,
      target,
      year,
      flags: flagsRaw,
      isBuybackOnly,
      sourceUrl,
    });
  }
  return out;
}

function makeDeal(t) {
  // Approximate announce date from transaction year. Detail pages carry the
  // precise release date; list view does not. Using mid-January of the
  // reported year gives stable dedupe + correct year bucketing.
  const announceDate = t.year ? `${t.year}-01-15` : null;
  if (!announceDate) return null;
  if (t.isBuybackOnly) return null;

  const thisYear = new Date().getFullYear();
  const yearNum = Number(t.year);
  // Only keep deals from the last 6 years — older TOB entries are buyback-
  // heavy noise with stale targets.
  if (!Number.isFinite(yearNum) || yearNum < thisYear - 5) return null;
  const isCompleted = Number.isFinite(yearNum) && yearNum < thisYear - 1;
  const status = isCompleted ? 'completed' : 'announced';
  const event_type = isCompleted ? 'merger_completed' : 'merger_pending';

  return {
    event_type,
    data_source_tier: 'official',
    primary_source: 'swiss_takeover_board',
    source_filing_url: t.sourceUrl,
    confidence: 0.9,
    deal_type: 'merger_arb',
    status,
    region: 'EU',
    country: 'CH',
    primary_ticker: null,
    target_name: t.target,
    target_ticker: null,
    acquirer_name: null,
    acquirer_ticker: null,
    headline: `${t.target} — Swiss Takeover Board case ${t.transactionNumber || t.tobId}${t.flags.length ? ` (${t.flags.slice(0,2).join(', ')})` : ''}`,
    summary: `Swiss Takeover Board transaction ${t.transactionNumber || t.tobId} in ${t.year}${t.flags.length ? `. Characteristics: ${t.flags.join('; ')}` : ''}.`,
    announce_date: announceDate,
    filing_date: announceDate,
    completed_date: isCompleted ? `${t.year}-12-31` : null,
    key_dates: { filing_date: announceDate },
    external_key: `swiss_tob:${t.tobId}`,
  };
}

async function fetchAll() {
  let html;
  try { html = await fetchHtml(TRANSACTIONS_URL); }
  catch (e) { console.warn('[swiss_tob] fetch failed:', e.message); return { count: 0, items_scanned: 0, deals: [] }; }

  const parsed = parseTransactions(html);
  const deals = parsed.map(makeDeal).filter(Boolean);

  // Dedupe by target+year (one TOB file per year is enough)
  const seen = new Map();
  for (const d of deals) {
    const k = `${d.target_name.toLowerCase()}|${d.announce_date.slice(0, 4)}`;
    if (!seen.has(k)) seen.set(k, d);
  }
  const final = Array.from(seen.values());
  return { count: final.length, items_scanned: parsed.length, deals: final };
}

module.exports = { fetchAll, parseTransactions, makeDeal };
