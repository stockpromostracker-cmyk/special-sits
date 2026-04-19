// Smoke test for short-interest ingestors. No DB writes.
const { fetchFinraRegSho, fetchAfmShort, fetchFcaShort } = require('../server/sources/short_interest');

(async () => {
  console.log('--- FINRA regShoDaily (last 2 days) ---');
  const us = await fetchFinraRegSho(2);
  console.log(`Total rows: ${us.length}`);
  console.log('Sample:', us.slice(0, 3));

  console.log('\n--- AFM disclosed positions ---');
  const nl = await fetchAfmShort();
  console.log(`Total rows: ${nl.length}`);
  console.log('Sample:', nl.slice(0, 3));

  console.log('\n--- FCA disclosed positions ---');
  const gb = await fetchFcaShort();
  console.log(`Total rows: ${gb.length}`);
  console.log('Sample:', gb.slice(0, 3));
})();
