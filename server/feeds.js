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

const parser = new Parser({
  timeout: 20000,
  headers: { 'user-agent': UA, 'accept': 'application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8' },
});

// ---- SEC EDGAR (US) --------------------------------------------------------
// Pulls the latest filings for special-situation-relevant form types.
async function fetchSecEdgar() {
  const formTypes = ['8-K', 'S-1', 'F-1', 'S-4', 'SC 13E3', 'SC TO-I', 'SC TO-T', '10-12B', '425'];
  const items = [];

  for (const form of formTypes) {
    const url = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=${encodeURIComponent(form)}&company=&dateb=&owner=include&count=40&output=atom`;
    try {
      const feed = await parser.parseURL(url);
      for (const entry of feed.items || []) {
        items.push({
          source: 'sec_edgar',
          source_id: entry.id || entry.link,
          url: entry.link,
          headline: `[${form}] ${entry.title || ''}`.slice(0, 300),
          body: entry.summary || entry.contentSnippet || entry.content || '',
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
async function fetchGoogleNews() {
  const queries = [
    // US / global merger arb
    { name: 'ma_global',   q: '"definitive agreement to acquire" OR "agreed to acquire"', geo: 'US', when: '2d' },
    { name: 'ma_cash',     q: '"all-cash deal" OR "cash merger" OR "take-private"', geo: 'US', when: '7d' },
    // UK / Europe deal announcements
    { name: 'uk_rule27',   q: '"Rule 2.7 announcement" OR "recommended offer" OR "cash offer" site:londonstockexchange.com OR site:investegate.co.uk', geo: 'GB', when: '7d' },
    { name: 'eu_ma',       q: '"public takeover" OR "tender offer" OR "squeeze-out" Europe', geo: 'GB', when: '7d' },
    // Spin-offs / demergers (rarer, use longer window)
    { name: 'spinoffs',    q: '"spin-off" OR "demerger" OR "separation into two companies"', geo: 'US', when: '7d' },
    // IPOs
    { name: 'ipo_filing',  q: '"files for IPO" OR "filed S-1" OR "prospectus approved"', geo: 'US', when: '7d' },
    { name: 'ipo_eu',      q: '"intention to float" OR "IPO priced" Europe Nordic', geo: 'GB', when: '7d' },
    // SPACs
    { name: 'spac',        q: '"business combination" SPAC OR "de-SPAC"', geo: 'US', when: '7d' },
    // Tenders / buybacks / rights
    { name: 'tender',      q: '"tender offer" OR "Dutch auction" OR "self-tender"', geo: 'US', when: '7d' },
    { name: 'rights',      q: '"rights issue" OR "rights offering" OR "share buyback"', geo: 'GB', when: '7d' },
    // Activist / going-private
    { name: 'activist',    q: '"13D filing" OR "activist investor" OR "proxy contest"', geo: 'US', when: '7d' },
    { name: 'take_private', q: '"take private" OR "management buyout" OR "going private"', geo: 'US', when: '7d' },
    // Nordic-specific
    { name: 'nordic',      q: '"Nasdaq Stockholm" OR "Nasdaq Copenhagen" OR "Oslo Børs" acquisition OR merger OR IPO', geo: 'GB', when: '7d' },
  ];

  const items = [];
  for (const { name, q, geo, when } of queries) {
    // when:Nd restricts to last N days
    const fullQ = when ? `${q} when:${when}` : q;
    const url = `https://news.google.com/rss/search?q=${encodeURIComponent(fullQ)}&hl=en-${geo}&gl=${geo}&ceid=${geo}:en`;
    try {
      const feed = await parser.parseURL(url);
      for (const entry of feed.items || []) {
        // Skip Google's own feed-header items
        if (/^Google News$/i.test(entry.title || '')) continue;
        if ((entry.title || '').includes('when:')) continue;
        items.push({
          source: `google_news:${name}`,
          source_id: entry.guid || entry.link,
          url: entry.link,
          headline: (entry.title || '').slice(0, 300),
          body: entry.contentSnippet || entry.content || entry.summary || '',
          published_at: entry.pubDate || entry.isoDate || null,
        });
      }
    } catch (e) {
      console.error(`[google_news:${name}]`, e.message);
    }
  }
  return items;
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
  ]);
  const all = results.flat();
  console.log(`[feeds] fetched sec=${results[0].length} gnews=${results[1].length} prn=${results[2].length}`);
  const inserted = await saveRawItems(all);
  return { fetched: all.length, inserted };
}

module.exports = { fetchAll, saveRawItems };
