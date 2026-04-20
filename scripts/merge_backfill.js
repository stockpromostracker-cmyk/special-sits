// Merge /home/user/workspace/europe_spinoffs_5y.json into server/known_events.json.
// Dedupes by normalized (parent_ticker, spinco_ticker) key vs. existing entries.

const fs = require('fs');
const path = require('path');

const RESEARCH = JSON.parse(fs.readFileSync('/home/user/workspace/europe_spinoffs_5y.json', 'utf8'));
const KE_PATH = path.join(__dirname, '..', 'server', 'known_events.json');
const existing = JSON.parse(fs.readFileSync(KE_PATH, 'utf8'));

const norm = (s) => String(s || '').toLowerCase()
  .replace(/[^a-z0-9:]/g, '')
  .trim();

// Build a set of existing (parent, spinco) keys for dedup
const existingKeys = new Set();
for (const e of existing) {
  const k = `${norm(e.parent_ticker)}|${norm(e.spinco_ticker)}`;
  existingKeys.add(k);
  // Also add reverse and name-based keys for extra safety
  existingKeys.add(`${norm(e.spinco_ticker)}`);
  existingKeys.add(`${norm(e.parent_name)}|${norm(e.spinco_name)}`);
}

function region(country) {
  if (!country) return 'EU';
  if (country === 'US' || country === 'CA') return 'NA';
  if (['SE','NO','DK','FI','IS'].includes(country)) return 'EU';
  if (['GB','IE'].includes(country)) return 'EU';
  if (['DE','FR','NL','CH','IT','ES','BE','AT'].includes(country)) return 'EU';
  return 'EU';
}

function toKnownEvent(d) {
  const ex = d.completed_date || d.ex_date || null;
  const status = ex ? 'completed' : 'pending';
  const eventType = status === 'completed' ? 'spin_off_completed' : 'spin_off_announced';
  const slug = (d.spinco_name || d.parent_name || 'unknown')
    .toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 40);
  const external_key = `known:${slug}-${d.announce_date || ex || 'undated'}`;
  const headline = `${d.spinco_name} (${(d.spinco_ticker || '').split(':').pop()}) — spun off from ${d.parent_name}${ex ? `, completed ${ex}` : ''}`;

  return {
    external_key,
    event_type: eventType,
    data_source_tier: 'official',
    primary_source: 'company_press_release',
    source_filing_url: d.source_url,
    confidence: 1.0,
    deal_type: 'spin_off',
    status,
    region: region(d.country),
    country: d.country,
    headline,
    summary: d.summary,
    parent_name: d.parent_name,
    parent_ticker: d.parent_ticker,
    spinco_name: d.spinco_name,
    spinco_ticker: d.spinco_ticker,
    primary_ticker: d.spinco_ticker,
    target_name: d.spinco_name,
    target_ticker: d.spinco_ticker,
    announce_date: d.announce_date || null,
    filing_date: d.announce_date || null,
    completed_date: d.completed_date || null,
    ex_date: d.ex_date || d.completed_date || null,
    key_dates: {
      announce_date: d.announce_date || null,
      ex_date: d.ex_date || d.completed_date || null,
      completed_date: d.completed_date || null,
    },
  };
}

let added = 0, skipped = 0;
const added_entries = [];
for (const d of RESEARCH) {
  const k1 = `${norm(d.parent_ticker)}|${norm(d.spinco_ticker)}`;
  const k2 = norm(d.spinco_ticker);
  const k3 = `${norm(d.parent_name)}|${norm(d.spinco_name)}`;
  if (existingKeys.has(k1) || existingKeys.has(k2) || existingKeys.has(k3)) {
    skipped++;
    continue;
  }
  const evt = toKnownEvent(d);
  existing.push(evt);
  existingKeys.add(k1);
  added++;
  added_entries.push(evt.external_key);
}

fs.writeFileSync(KE_PATH, JSON.stringify(existing, null, 2));
console.log(`Added: ${added}, Skipped (dupes): ${skipped}, Total in file: ${existing.length}`);
console.log('New keys (first 10):', added_entries.slice(0, 10));
