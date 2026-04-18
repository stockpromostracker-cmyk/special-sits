// London Stock Exchange RNS — UK regulator-grade announcements.
//
// Uses the LSE's internal /api/v1/components/refresh endpoint (public,
// unauthenticated, same one their own SPA calls). If LSE is unavailable we
// fall back to Investegate's advanced-search endpoint which mirrors the RNS
// stream via HTML.
//
// FCA headline-type codes (3-letter standard):
//   SOA  Scheme of Arrangement           → merger_pending (usually take-private)
//   ITF  Intention to Float              → ipo_pending
//   OFF  Offer for target                → merger_pending
//   OFB  Offer by bidder                 → merger_pending
//   OFD  Possible Offer (Rule 2.4 talks) → merger_pending (early)
//   OUP  Offer Update                    → merger_pending (progression)
//
// Demerger has NO dedicated code — captured via keyword match on DIS/STR/CAR
// and via the Investegate keyword search fallback.

const UA = 'Mozilla/5.0 (compatible; SpecialSits/1.0)';
const NEWS_COMPONENT_ID = 'block_content:431d02ac-09b8-40c9-aba6-04a72a4f2e49';

const HEADLINE_TO_EVENT = {
  SOA: 'merger_pending',
  ITF: 'ipo_pending',
  OFF: 'merger_pending',
  OFB: 'merger_pending',
  OFD: 'merger_pending',
  OUP: 'merger_pending',
};

// ---- Primary: LSE components/refresh -----------------------------------

async function fetchLseComponent(parameters) {
  const res = await fetch(
    'https://api.londonstockexchange.com/api/v1/components/refresh',
    {
      method: 'POST',
      headers: {
        'User-Agent': UA,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Origin': 'https://www.londonstockexchange.com',
        'Referer': 'https://www.londonstockexchange.com/news?tab=news-explorer',
      },
      body: JSON.stringify({
        components: [{ id: NEWS_COMPONENT_ID, timestamp: null }],
        path: 'news',
        parameters,
      }),
    }
  );
  if (!res.ok) throw new Error(`LSE ${res.status}`);
  return await res.json();
}

function extractLseRows(data) {
  if (!Array.isArray(data) || !data.length) return [];
  const newsSection = data[0]?.content?.find?.(c => c.name === 'newsexplorersearch');
  const items = newsSection?.value?.content;
  if (!Array.isArray(items)) return [];
  return items.map(i => ({
    headline: i.title || '',
    date: (i.datetime || '').slice(0, 10),
    ticker: i.companycode || null,
    issuer: i.issuername || null,
    headline_code: i.headlinename || null,
    id: i.idnews || null,
    url: i.idnews ? `https://www.londonstockexchange.com/news-article/${i.companycode}/${i.idnews}` : null,
  }));
}

async function fetchLseByCodes(codes, { afterDate } = {}) {
  const parts = [
    'tab=news-explorer',
    `headlinetypes=${codes.join(',')}`,
  ];
  if (afterDate) {
    parts.push('period=custom');
    parts.push(`afterdate=${afterDate}`);
    parts.push(`beforedate=${new Date().toISOString().slice(0, 10)}`);
  }
  const data = await fetchLseComponent(parts.join('&'));
  return extractLseRows(data);
}

// ---- Fallback: Investegate advanced search ------------------------------

async function fetchInvestegate(keyword) {
  const url = `https://www.investegate.co.uk/advanced-search/draw?categories[]=1&sources[]=RNS&exclude_navs=false&page=1&key_word=${encodeURIComponent(keyword)}`;
  const res = await fetch(url, { headers: { 'User-Agent': UA, 'Accept': 'text/html' } });
  if (!res.ok) throw new Error(`Investegate ${res.status}`);
  const html = await res.text();
  // Parse announcement rows — Investegate renders a table with tr/td; each row
  // contains a link to the RNS announcement. We extract:
  //   - datetime
  //   - ticker (TIDM in parens)
  //   - issuer name
  //   - headline
  //   - href
  // Investegate table structure:
  //   <tr>
  //     <td>02 Mar 2026 06:29 PM</td>
  //     <td>...<a href=".../company/SOLG">Solgold (SOLG)</a>...</td>
  //     <td>...RNS...</td>
  //     <td><a href=".../announcement/rns/.../xyz/123">Court Sanction of Scheme of Arrangement</a></td>
  //   </tr>
  const rows = [];
  const rowRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  let m;
  while ((m = rowRe.exec(html)) !== null) {
    const inner = m[1];
    if (/<th[^>]*>/i.test(inner)) continue;  // skip header

    // Date: first <td>'s text content
    const dateM = inner.match(/<td[^>]*>\s*([^<]+?)\s*<\/td>/i);
    const rawDate = dateM ? dateM[1] : null;

    // Company link: <a href=".../company/TICKER">Issuer (TICKER)</a>
    const compM = inner.match(/\/company\/([A-Z0-9.]{1,6})"[^>]*>([^<]+?)\s*\(([A-Z0-9.]{1,6})\)\s*<\/a>/i);
    const ticker = compM ? compM[3] : null;
    const issuer = compM ? stripTags(compM[2]).trim() : null;

    // Announcement link
    const annM = inner.match(/<a\s+href="([^"]*\/announcement\/[^"]+)"[^>]*>([\s\S]*?)<\/a>/i);
    if (!annM) continue;
    const headline = stripTags(annM[2]).trim();
    if (!headline) continue;

    rows.push({
      headline,
      date: parseDateLoose(rawDate),
      ticker,
      issuer,
      url: annM[1].startsWith('http') ? annM[1] : `https://www.investegate.co.uk${annM[1]}`,
    });
  }
  return rows;
}

function stripTags(s) { return String(s || '').replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim(); }
function parseDateLoose(s) {
  if (!s) return null;
  const d = new Date(s);
  return isNaN(d) ? null : d.toISOString().slice(0, 10);
}

// ---- Row → deal conversion ---------------------------------------------

function inferEventType(row) {
  const hc = row.headline_code;
  if (hc && HEADLINE_TO_EVENT[hc]) return HEADLINE_TO_EVENT[hc];
  const h = String(row.headline || '').toLowerCase();
  if (/\bdemerger\b|\bdistribution in specie\b/.test(h)) return 'demerger_pending';
  if (/\bintention to float\b|\badmission to (aim|official list|trading)\b/.test(h)) return 'ipo_pending';
  if (/\brecommended offer\b|\bscheme of arrangement\b|\boffer for\b|\bpossible offer\b/.test(h)) return 'merger_pending';
  if (/\bprospectus\b/.test(h)) return 'ipo_pending';
  return null;
}

function makeDeal(row) {
  const eventType = inferEventType(row);
  if (!eventType) return null;
  const ticker = row.ticker ? `LSE:${row.ticker}` : null;
  const issuer = row.issuer || '(Unknown issuer)';
  const isCompleted = eventType.endsWith('_completed');
  return {
    event_type: eventType,
    data_source_tier: 'official',
    primary_source: 'lse_rns',
    source_filing_url: row.url || null,
    confidence: 1.0,
    deal_type: eventType.startsWith('merger') ? 'merger_arb'
             : eventType.startsWith('demerger') ? 'spin_off'
             : eventType.startsWith('ipo') ? 'ipo' : 'other',
    status: isCompleted ? 'completed' : 'announced',
    region: 'EU',
    country: 'GB',
    primary_ticker: ticker,
    target_name: issuer,
    target_ticker: ticker,
    parent_name: eventType.startsWith('demerger') ? issuer : null,
    parent_ticker: eventType.startsWith('demerger') ? ticker : null,
    headline: `${issuer} — ${row.headline}`,
    summary: row.headline,
    announce_date: row.date,
    filing_date: row.date,
    key_dates: { filing_date: row.date },
    external_key: `lse_rns:${row.id || row.url || (issuer + ':' + row.date + ':' + row.headline.slice(0, 40))}`,
  };
}

// ---- Public API --------------------------------------------------------

async function fetchAll({ days = 90 } = {}) {
  const afterDate = new Date(Date.now() - days * 86400000).toISOString().slice(0, 10);
  let rows = [];
  let primaryOk = false;

  // Try LSE first
  try {
    const lseRows = await fetchLseByCodes(Object.keys(HEADLINE_TO_EVENT), { afterDate });
    if (lseRows.length > 0) {
      rows = lseRows;
      primaryOk = true;
    }
  } catch (e) {
    console.warn('[lse_rns] primary failed:', e.message);
  }

  // Fallback: Investegate keyword search across our event types
  if (!primaryOk) {
    const keywords = ['demerger', 'intention to float', 'scheme of arrangement', 'recommended offer'];
    const tasks = await Promise.allSettled(keywords.map(k => fetchInvestegate(k)));
    for (const t of tasks) {
      if (t.status === 'fulfilled') rows.push(...t.value);
    }
  }

  // Convert and de-dupe
  const deals = rows.map(makeDeal).filter(Boolean);
  const seen = new Set();
  const deduped = deals.filter(d => {
    if (seen.has(d.external_key)) return false;
    seen.add(d.external_key); return true;
  });

  return {
    source: primaryOk ? 'lse_rns' : 'investegate',
    count: deduped.length,
    deals: deduped,
  };
}

module.exports = { fetchAll, fetchLseByCodes, fetchInvestegate, HEADLINE_TO_EVENT };
