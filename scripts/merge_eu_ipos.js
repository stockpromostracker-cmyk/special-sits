// Merge /home/user/workspace/eu_ipos_5y.json into server/known_events.json
// Dedupes by ticker vs. existing known_events and the live deals index.

const fs = require('fs');
const path = require('path');

const IN_PATH = process.env.IPO_JSON || '/home/user/workspace/eu_ipos_5y.json';
const KE_PATH = path.join(__dirname, '..', 'server', 'known_events.json');

if (!fs.existsSync(IN_PATH)) {
  console.error(`Input not found: ${IN_PATH}`);
  process.exit(1);
}

const incoming = JSON.parse(fs.readFileSync(IN_PATH, 'utf8'));
const existing = JSON.parse(fs.readFileSync(KE_PATH, 'utf8'));

const norm = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9:]/g, '').trim();
const bareOf = (t) => {
  if (!t) return '';
  const s = String(t);
  const parts = s.split(':');
  return norm(parts[parts.length - 1]);
};

function region(country) {
  if (!country) return 'EU';
  if (country === 'US' || country === 'CA') return 'NA';
  return 'EU';
}

const existingKeys = new Set();
for (const e of existing) {
  for (const f of ['primary_ticker','target_ticker','spinco_ticker','parent_ticker']) {
    if (e[f]) {
      existingKeys.add(norm(e[f]));
      existingKeys.add(bareOf(e[f]));
    }
  }
}

// Also load live existing tickers
try {
  const live = fs.readFileSync('/home/user/workspace/existing_tickers.txt','utf8')
    .split('\n').map(s => s.trim()).filter(Boolean);
  for (const t of live) { existingKeys.add(norm(t)); existingKeys.add(bareOf(t)); }
} catch {}

function toEvent(d) {
  const tkr = d.ticker || d.bare_ticker;
  const bare = d.bare_ticker || (tkr ? tkr.split(':').pop() : '');
  const slug = (d.company_name || bare).toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,'').slice(0,40);
  const status = d.ipo_date ? 'completed' : 'pending';
  const event_type = status === 'completed' ? 'ipo_priced' : 'ipo_filed';
  return {
    external_key: `known:ipo-${slug}-${d.ipo_date || d.announce_date || 'undated'}`,
    event_type,
    data_source_tier: 'official',
    primary_source: 'exchange_listing_notice',
    source_filing_url: d.source_url,
    confidence: 0.95,
    deal_type: 'ipo',
    status,
    region: region(d.country),
    country: d.country || null,
    headline: `${d.company_name} (${bare}) — IPO ${d.ipo_date || ''} on ${d.exchange || ''}${d.is_carveout ? ' (carve-out)' : ''}`.trim(),
    summary: d.summary || '',
    parent_name: d.is_carveout ? (d.parent_name || null) : null,
    parent_ticker: d.is_carveout ? (d.parent_ticker || null) : null,
    spinco_name: d.is_carveout ? d.company_name : null,
    spinco_ticker: d.is_carveout ? tkr : null,
    primary_ticker: tkr,
    target_name: d.company_name,
    target_ticker: tkr,
    ipo_price: d.ipo_price ?? null,
    announce_date: d.announce_date || d.ipo_date || null,
    filing_date: d.announce_date || null,
    completed_date: d.ipo_date || null,
    ex_date: d.ipo_date || null,
    key_dates: {
      announce_date: d.announce_date || null,
      ipo_date: d.ipo_date || null,
      completed_date: d.ipo_date || null,
    },
  };
}

let added = 0, skipped = 0;
const addedEntries = [];
for (const d of incoming) {
  const tkr = d.ticker || d.bare_ticker;
  if (!tkr) { skipped++; continue; }
  const k1 = norm(tkr);
  const k2 = bareOf(tkr);
  if (existingKeys.has(k1) || existingKeys.has(k2)) { skipped++; continue; }
  const evt = toEvent(d);
  existing.push(evt);
  existingKeys.add(k1); existingKeys.add(k2);
  added++;
  addedEntries.push(evt.external_key);
}

fs.writeFileSync(KE_PATH, JSON.stringify(existing, null, 2));
console.log(`[merge_eu_ipos] Added: ${added}, Skipped: ${skipped}, Total: ${existing.length}`);
console.log('First 10 keys added:', addedEntries.slice(0,10));
