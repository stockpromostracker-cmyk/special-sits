// Merge /home/user/workspace/eu_mergers_5y.json AND /home/user/workspace/historical_ma_2020_2023.json
// into server/known_events.json. Dedupes by target_ticker vs existing + live tickers.

const fs = require('fs');
const path = require('path');

const INPUTS = (process.env.MERGER_JSONS || '/home/user/workspace/eu_mergers_5y.json,/home/user/workspace/historical_ma_2020_2023.json').split(',');
const KE_PATH = path.join(__dirname, '..', 'server', 'known_events.json');

const existing = JSON.parse(fs.readFileSync(KE_PATH, 'utf8'));

const norm = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9:]/g, '').trim();
const bareOf = (t) => {
  if (!t) return '';
  const parts = String(t).split(':');
  return norm(parts[parts.length - 1]);
};

function region(c) {
  if (!c) return 'EU';
  if (c === 'US' || c === 'CA') return 'NA';
  return 'EU';
}

const existingKeys = new Set();
for (const e of existing) {
  for (const f of ['primary_ticker','target_ticker','spinco_ticker','parent_ticker']) {
    if (e[f]) { existingKeys.add(norm(e[f])); existingKeys.add(bareOf(e[f])); }
  }
}
try {
  const live = fs.readFileSync('/home/user/workspace/existing_tickers.txt','utf8').split('\n').map(s=>s.trim()).filter(Boolean);
  for (const t of live) { existingKeys.add(norm(t)); existingKeys.add(bareOf(t)); }
} catch {}

function toEvent(d) {
  const tkr = d.target_ticker || d.target_bare_ticker;
  const bare = d.target_bare_ticker || (tkr ? tkr.split(':').pop() : '');
  const slug = (d.target_name || bare).toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,'').slice(0,40);
  const status = d.status || (d.completed_date ? 'completed' : 'pending');
  let event_type;
  if (status === 'completed') event_type = 'merger_completed';
  else if (status === 'pending') event_type = 'merger_announced';
  else if (status === 'terminated' || status === 'failed') event_type = 'merger_terminated';
  else event_type = 'merger_announced';
  return {
    external_key: `known:ma-${slug}-${d.announce_date || 'undated'}`,
    event_type,
    data_source_tier: 'official',
    primary_source: 'regulator_filing',
    source_filing_url: d.source_url,
    confidence: 0.95,
    deal_type: 'merger_arb',
    status,
    region: region(d.target_country),
    country: d.target_country || null,
    headline: `${d.target_name} (${bare}) — ${d.bidder_name ? `${d.bidder_name} bid` : 'takeover'}${d.offer_price ? ` at ${d.offer_price} ${d.currency || ''}` : ''}${d.status ? ` [${d.status}]` : ''}`.trim(),
    summary: d.summary || '',
    parent_name: null,
    parent_ticker: null,
    spinco_name: null,
    spinco_ticker: null,
    primary_ticker: tkr,
    target_name: d.target_name,
    target_ticker: tkr,
    acquirer_name: d.bidder_name || null,
    acquirer_ticker: d.bidder_ticker || null,
    offer_price: d.offer_price ?? null,
    consideration_type: d.offer_type || null,
    deal_value_usd: d.deal_value_musd ? d.deal_value_musd * 1e6 : null,
    announce_date: d.announce_date || null,
    filing_date: d.announce_date || null,
    completed_date: d.completed_date || null,
    ex_date: d.completed_date || null,
    key_dates: {
      announce_date: d.announce_date || null,
      completed_date: d.completed_date || null,
    },
  };
}

let totalAdded = 0, totalSkipped = 0;
for (const input of INPUTS) {
  if (!fs.existsSync(input)) { console.log(`[skip] ${input} not found`); continue; }
  const rows = JSON.parse(fs.readFileSync(input,'utf8'));
  let added = 0, skipped = 0;
  for (const d of rows) {
    const tkr = d.target_ticker || d.target_bare_ticker;
    if (!tkr || !d.announce_date) { skipped++; continue; }
    const k1 = norm(tkr), k2 = bareOf(tkr);
    if (existingKeys.has(k1) || existingKeys.has(k2)) { skipped++; continue; }
    existing.push(toEvent(d));
    existingKeys.add(k1); existingKeys.add(k2);
    added++;
  }
  console.log(`[${path.basename(input)}] Added: ${added}, Skipped (dupe/invalid): ${skipped}`);
  totalAdded += added; totalSkipped += skipped;
}

fs.writeFileSync(KE_PATH, JSON.stringify(existing, null, 2));
console.log(`[merge_mergers] TOTAL added: ${totalAdded}, skipped: ${totalSkipped}, file total: ${existing.length}`);
