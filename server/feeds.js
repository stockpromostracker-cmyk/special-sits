// Feed ingestors — pull fresh items from every source, dedupe, insert into raw_items.

const Parser = require('rss-parser');
const { query } = require('./db');

const parser = new Parser({ timeout: 20000, headers: { 'user-agent': 'SpecialSits/1.0 (research tool)' } });

// ---- SEC EDGAR (US) --------------------------------------------------------
// Pulls the latest filings for special-situation-relevant form types.
async function fetchSecEdgar() {
  const formTypes = ['8-K', 'S-1', 'F-1', 'S-4', 'SC 13E3', 'SC TO-I', 'SC TO-T', '10-12B', '425'];
  const items = [];

  for (const form of formTypes) {
    // EDGAR full-text search RSS feed
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

// ---- LSE RNS (UK) ----------------------------------------------------------
// London Stock Exchange news via investegate's public RSS — all-announcements
async function fetchLseRns() {
  const feeds = [
    { name: 'investegate_all', url: 'https://www.investegate.co.uk/Rss.aspx?t=all' },
  ];
  return fetchRssList('lse_rns', feeds);
}

// ---- Nasdaq Nordic ---------------------------------------------------------
async function fetchNasdaqNordic() {
  const feeds = [
    { name: 'nasdaq_stockholm', url: 'https://www.nasdaqomxnordic.com/news/companynews/xml/rss.action?market=SE' },
    { name: 'nasdaq_copenhagen', url: 'https://www.nasdaqomxnordic.com/news/companynews/xml/rss.action?market=DK' },
    { name: 'nasdaq_helsinki',   url: 'https://www.nasdaqomxnordic.com/news/companynews/xml/rss.action?market=FI' },
    { name: 'nasdaq_iceland',    url: 'https://www.nasdaqomxnordic.com/news/companynews/xml/rss.action?market=IS' },
  ];
  return fetchRssList('nasdaq_nordic', feeds);
}

// ---- Euronext --------------------------------------------------------------
async function fetchEuronext() {
  const feeds = [
    { name: 'euronext_paris',   url: 'https://live.euronext.com/en/rss/news/company?marketSegment=XPAR' },
    { name: 'euronext_amsterdam', url: 'https://live.euronext.com/en/rss/news/company?marketSegment=XAMS' },
    { name: 'euronext_brussels', url: 'https://live.euronext.com/en/rss/news/company?marketSegment=XBRU' },
    { name: 'euronext_dublin',  url: 'https://live.euronext.com/en/rss/news/company?marketSegment=XMSM' },
    { name: 'euronext_lisbon',  url: 'https://live.euronext.com/en/rss/news/company?marketSegment=XLIS' },
    { name: 'euronext_oslo',    url: 'https://live.euronext.com/en/rss/news/company?marketSegment=XOSL' },
  ];
  return fetchRssList('euronext', feeds);
}

// ---- Press wires (global) --------------------------------------------------
async function fetchPressWires() {
  const feeds = [
    // Business Wire — mergers & acquisitions topic feed
    { name: 'business_wire_ma', url: 'https://www.businesswire.com/portal/site/home/news/subject/?vnsId=31336' },
    // GlobeNewswire — mergers & acquisitions
    { name: 'globe_newswire_ma', url: 'https://www.globenewswire.com/RssFeed/subjectcode/9-Mergers%20And%20Acquisitions/feedTitle/GlobeNewswire%20-%20Mergers%20And%20Acquisitions' },
    // GlobeNewswire — IPOs
    { name: 'globe_newswire_ipo', url: 'https://www.globenewswire.com/RssFeed/subjectcode/16-Initial%20Public%20Offerings/feedTitle/GlobeNewswire%20-%20IPOs' },
    // PR Newswire — financial
    { name: 'prnewswire_financial', url: 'https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss' },
  ];
  return fetchRssList('press_wire', feeds);
}

// ---- Generic RSS helper ----------------------------------------------------
async function fetchRssList(baseSource, feeds) {
  const items = [];
  for (const { name, url } of feeds) {
    try {
      const feed = await parser.parseURL(url);
      for (const entry of feed.items || []) {
        items.push({
          source: `${baseSource}:${name}`,
          source_id: entry.guid || entry.id || entry.link,
          url: entry.link,
          headline: (entry.title || '').slice(0, 300),
          body: entry.contentSnippet || entry.content || entry.summary || '',
          published_at: entry.pubDate || entry.isoDate || null,
        });
      }
    } catch (e) {
      console.error(`[${baseSource}:${name}]`, e.message);
    }
  }
  return items;
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
      // SQLite doesn't support ON CONFLICT the same way — try a safe fallback
      if (/ON CONFLICT/i.test(e.message) || /syntax/i.test(e.message)) {
        try {
          await query(
            `INSERT OR IGNORE INTO raw_items (source, source_id, url, headline, body, published_at)
             VALUES ($1,$2,$3,$4,$5,$6)`,
            [it.source, it.source_id, it.url, it.headline, it.body, it.published_at]
          );
          inserted++;
        } catch (e2) {
          console.error('[saveRawItems fallback]', e2.message);
        }
      } else {
        console.error('[saveRawItems]', e.message);
      }
    }
  }
  return inserted;
}

async function fetchAll() {
  const [sec, rns, nordic, euronext, wires] = await Promise.all([
    fetchSecEdgar().catch(e => (console.error('sec', e.message), [])),
    fetchLseRns().catch(e => (console.error('rns', e.message), [])),
    fetchNasdaqNordic().catch(e => (console.error('nordic', e.message), [])),
    fetchEuronext().catch(e => (console.error('euronext', e.message), [])),
    fetchPressWires().catch(e => (console.error('wires', e.message), [])),
  ]);
  const all = [...sec, ...rns, ...nordic, ...euronext, ...wires];
  const inserted = await saveRawItems(all);
  return { fetched: all.length, inserted };
}

module.exports = { fetchAll, saveRawItems };
