// Known-events seed — hand-curated famous spin-offs / demergers that either:
//   (a) fall outside the auto-discovery windows (e.g. Amrize 2024 10-12B, GE
//       HealthCare Jan 2023), or
//   (b) happen in markets we don't yet auto-ingest (e.g. Swiss SIX, Finnish
//       Nasdaq Helsinki before MFN coverage kicked in).
//
// Each entry is treated as data_source_tier='official' with a real filing URL.
// Upserts go through the same dedupe (source_filing_url) as every other source.

const fs = require('fs');
const path = require('path');

function load() {
  const p = path.join(__dirname, '..', 'known_events.json');
  try {
    const raw = fs.readFileSync(p, 'utf8');
    const events = JSON.parse(raw);
    if (!Array.isArray(events)) throw new Error('known_events.json must be an array');
    return events;
  } catch (e) {
    console.warn('[known_events] load failed:', e.message);
    return [];
  }
}

async function fetchAll() {
  const events = load();
  return { count: events.length, deals: events };
}

module.exports = { fetchAll, load };
