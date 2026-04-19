// Smoke test for the EU MAR ingestors. Hits live AFM + Sweden FI endpoints
// and prints a summary. Does not write to the DB.
//
//   node scripts/test-eu-mar.js

const { fetchAfm, fetchSwedenFi } = require('../server/sources/eu_mar');

async function main() {
  console.log('--- AFM (Netherlands) ---');
  const nl = await fetchAfm();
  console.log(`rows: ${nl.length}`);
  if (nl.length) {
    const issuers = new Set(nl.map(r => r.issuer_name));
    console.log(`distinct issuers: ${issuers.size}`);
    console.log('first 3:');
    for (const r of nl.slice(0, 3)) {
      console.log(`  ${r.transaction_date} ${r.issuer_name} / ${r.insider_name} (${r.insider_title})`);
    }
  }

  console.log('');
  console.log('--- Sweden FI ---');
  const se = await fetchSwedenFi({ days: 14 });
  console.log(`rows: ${se.length}`);
  if (se.length) {
    const issuers = new Set(se.map(r => r.issuer_name));
    console.log(`distinct issuers: ${issuers.size}`);
    const buys = se.filter(r => r.is_buy === 1).length;
    const sells = se.filter(r => r.is_buy === 0).length;
    console.log(`buys=${buys} sells=${sells} other=${se.length - buys - sells}`);
    console.log('first 5:');
    for (const r of se.slice(0, 5)) {
      const dir = r.is_buy === 1 ? 'BUY' : r.is_buy === 0 ? 'SELL' : '???';
      console.log(`  ${r.transaction_date} ${dir} ${r.issuer_name} / ${r.insider_name} ${r.shares || '?'} @ ${r.price_local || '?'} ${r.currency}`);
    }
  }

  // Field sanity: check that rows match the insider_transactions column set.
  const sample = (nl[0] || se[0]);
  if (sample) {
    const expected = [
      'source', 'source_id', 'url', 'issuer_name', 'issuer_country', 'issuer_ticker',
      'insider_name', 'insider_title', 'is_director', 'is_officer',
      'is_ten_percent_owner', 'transaction_date', 'transaction_code', 'is_buy',
      'shares', 'price_local', 'value_local', 'currency', 'price_usd', 'value_usd',
    ];
    const missing = expected.filter(k => !(k in sample));
    if (missing.length) {
      console.error('\nSCHEMA MISMATCH — missing fields:', missing);
      process.exit(1);
    } else {
      console.log('\nschema check: OK');
    }
  }

  // Basic assertions
  if (nl.length === 0 && se.length === 0) {
    console.error('\nERROR: both feeds returned zero rows');
    process.exit(1);
  }
}

main().catch(e => { console.error(e); process.exit(1); });
