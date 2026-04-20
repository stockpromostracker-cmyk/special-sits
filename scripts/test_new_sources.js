const amf = require('../server/sources/amf_bdif');
const sw = require('../server/sources/swiss_tob');
const eu = require('../server/sources/eu_news_ma');

(async () => {
  console.log('=== AMF BDIF ===');
  try {
    const r = await amf.fetchAll(500);
    console.log('scanned:', r.items_scanned, 'deals:', r.count);
    console.log('Sample:', JSON.stringify(r.deals.slice(0,3), null, 2).slice(0,1500));
  } catch (e) { console.error('AMF fail:', e.message); }

  console.log('\n=== Swiss TOB ===');
  try {
    const r = await sw.fetchAll();
    console.log('scanned:', r.items_scanned, 'deals:', r.count);
    console.log('Sample:', JSON.stringify(r.deals.slice(0,3), null, 2).slice(0,1500));
  } catch (e) { console.error('Swiss fail:', e.message); }

  console.log('\n=== EU News M&A ===');
  try {
    const r = await eu.fetchAll();
    console.log('scanned:', r.items_scanned, 'deals:', r.count, 'errors:', r.errors);
    console.log('Sample:', JSON.stringify(r.deals.slice(0,3), null, 2).slice(0,1500));
  } catch (e) { console.error('EU news fail:', e.message); }
})();
