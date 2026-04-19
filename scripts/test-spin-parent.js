// Quick validation of extractSpinParent against known-real filings
const { extractSpinParent } = require('../server/sources/sec.js');

const tests = [
  {
    name: 'FedEx Freight (filer=FedEx Freight Holding Company) expects parent=FedEx Corporation',
    cik: '2082247',
    accession: '0001104659-26-041977',
    exclude: 'FedEx Freight Holding Company, Inc.',
    expect: /FedEx/i,
    notExpect: /FedEx Freight/i,
  },
  {
    name: 'Versigent (filer=Versigent) expects parent=Aptiv',
    cik: '2078008',
    accession: '0001193125-26-096776', // 10-12B/A latest
    exclude: 'Versigent',
    expect: /Aptiv/i,
    notExpect: /Versigent/i,
  },
  {
    name: 'Sunbelt Rentals Holdings (filer=Sunbelt Rentals Holdings, Inc.) expects parent=Ashtead',
    cik: '2083785',
    accession: '0001193125-26-050612', // 10-12B/A latest
    exclude: 'Sunbelt Rentals Holdings, Inc.',
    expect: /Ashtead/i,
    notExpect: /Sunbelt/i,
  },
];

(async () => {
  for (const t of tests) {
    console.log(`\n=== ${t.name} ===`);
    try {
      const r = await extractSpinParent(t.accession, t.cik, t.exclude);
      console.log('  result:', r);
      if (!r) { console.log('  ❌ no match'); continue; }
      const ok = t.expect.test(r.parent_name) && !t.notExpect.test(r.parent_name);
      console.log('  ', ok ? '✅ PASS' : '❌ FAIL');
    } catch (e) {
      console.log('  error:', e.message);
    }
  }
})();
