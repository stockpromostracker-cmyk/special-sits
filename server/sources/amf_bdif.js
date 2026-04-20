// AMF (France) BDIF — live takeover-bid register.
//
// The AMF exposes a clean JSON API that backs their public bdif.amf-france.org
// search UI. We hit the `informations` endpoint filtered to TypesInformation=OPA
// (takeover bids / public offers).
//
// Docs of interest:
//   typesDocument:   DepotOffre         → offer filing       (deal start)
//                    PreOffre           → pre-offer talks     (early catalyst)
//                    CalendrierOffre    → AMF-set calendar    (key dates)
//                    ResultatOffre      → result published    (completion)
//                    RetraitObligatoire → squeeze-out         (completion)
//                    Decisions          → AMF decision        (other)
//                    DeclarationAchatVente → shareholder reporting  (NOISE, skip)
//
// We ONLY keep docs that represent an actual deal event, not routine
// shareholder reporting. Target company is the `societe` with role
// SocieteConcernee; bidder is role Initiateur.

const UA = 'Mozilla/5.0 (compatible; SpecialSits/1.0; +https://special-sits.example)';

const API = 'https://bdif.amf-france.org/back/api/v1/informations';

// Document types that represent actual deal events (not shareholder noise)
const DEAL_DOC_TYPES = new Set([
  'DepotOffre',           // offer filing
  'PreOffre',             // pre-offer talks
  'CalendrierOffre',      // calendar set by AMF
  'ResultatOffre',        // offer result
  'RetraitObligatoire',   // squeeze-out
  'NotesEtAutresInformations', // offer note supplements
]);

// Map document type → event_type / status
function mapEventType(doc) {
  if (doc === 'DepotOffre')          return { event_type: 'merger_pending',    status: 'announced' };
  if (doc === 'PreOffre')            return { event_type: 'merger_announced',  status: 'announced' };
  if (doc === 'CalendrierOffre')     return { event_type: 'merger_pending',    status: 'announced' };
  if (doc === 'NotesEtAutresInformations') return { event_type: 'merger_pending', status: 'announced' };
  if (doc === 'ResultatOffre')       return { event_type: 'merger_completed',  status: 'completed' };
  if (doc === 'RetraitObligatoire')  return { event_type: 'merger_completed',  status: 'completed' };
  return null;
}

async function fetchPage(from = 0, size = 100) {
  const url = `${API}?TypesInformation=OPA&From=${from}&Size=${size}`;
  const res = await fetch(url, {
    headers: {
      'User-Agent': UA,
      'Accept': 'application/json',
      'Accept-Language': 'en-GB,en;q=0.9',
      'Origin': 'https://bdif.amf-france.org',
      'Referer': 'https://bdif.amf-france.org/en?typesInformation=OPA',
    },
  });
  if (!res.ok) throw new Error(`AMF BDIF HTTP ${res.status}`);
  return res.json();
}

function extractCompanies(societes) {
  const out = { target: null, bidder: null };
  for (const s of (societes || [])) {
    const name = s.raisonSociale || s.denomination;
    if (!name) continue;
    const role = String(s.role || '').toLowerCase();
    if (!out.target && (role === 'societeconcernee' || role === 'cible')) out.target = name;
    if (!out.bidder && (role === 'initiateur' || role === 'auteur')) out.bidder = name;
  }
  // If we only have one society and no explicit target, assume it's the target
  if (!out.target && (societes || []).length === 1) out.target = societes[0].raisonSociale || societes[0].denomination;
  return out;
}

function makeDeal(rec) {
  const docTypes = rec.typesDocument || [];
  // Find the most-informative doc type we care about
  const primaryType = docTypes.find(t => DEAL_DOC_TYPES.has(t));
  if (!primaryType) return null;

  const mapping = mapEventType(primaryType);
  if (!mapping) return null;

  const { target, bidder } = extractCompanies(rec.societes);
  if (!target) return null;  // No target name → not useful

  const docDate = (rec.dateInformation || rec.datePublication || '').slice(0, 10);
  if (!docDate) return null;

  const pdfPath = rec.documents?.[0]?.path;
  // Public document URL pattern on AMF's file server
  const pdfUrl = pdfPath
    ? `https://bdif.amf-france.org/back/api/v1/documents/${rec.id}/download`
    : `https://bdif.amf-france.org/en/recherche/detail/${rec.id}`;

  return {
    event_type: mapping.event_type,
    data_source_tier: 'official',
    primary_source: 'amf_bdif',
    source_filing_url: pdfUrl,
    confidence: 0.9,
    deal_type: 'merger_arb',
    status: mapping.status,
    region: 'EU',
    country: 'FR',
    primary_ticker: null,          // BDIF does not expose tickers; later enrichment resolves
    target_name: target,
    target_ticker: null,
    acquirer_name: bidder,
    acquirer_ticker: null,
    headline: `${target} — ${bidder ? `offer from ${bidder}` : primaryType}`,
    summary: `AMF filing ${rec.numero || rec.numeroConcatene || rec.id}: ${docTypes.join(', ')}${bidder ? `, bidder ${bidder}` : ''}.`,
    announce_date: docDate,
    filing_date: docDate,
    completed_date: mapping.status === 'completed' ? docDate : null,
    key_dates: { filing_date: docDate },
    external_key: `amf_bdif:${rec.id}`,
  };
}

async function fetchAll(maxRecords = 2000) {
  const PAGE = 200;  // API supports large pages
  const seen = new Map();  // key = external_key → deal
  let scanned = 0;
  let errors = 0;
  // Walk 5y = ~1200 records based on observed volumes; cap at maxRecords.
  for (let from = 0; from < maxRecords; from += PAGE) {
    let payload;
    try {
      payload = await fetchPage(from, PAGE);
    } catch (e) {
      errors++;
      if (errors >= 3) break;
      continue;
    }
    const rows = payload?.result || [];
    if (!rows.length) break;
    scanned += rows.length;
    for (const r of rows) {
      const d = makeDeal(r);
      if (!d) continue;
      // Dedupe: keep first seen per (target_name, quarter) so we don't spam
      // one deal with 10 AMF docs (DepotOffre + calendar + note + etc).
      const quarterKey = `${d.target_name.toLowerCase()}|${d.announce_date.slice(0,7)}`;
      if (!seen.has(quarterKey)) seen.set(quarterKey, d);
    }
    // Tiny throttle so we don't hammer AMF
    await new Promise(r => setTimeout(r, 150));
  }
  const deals = Array.from(seen.values());
  return { count: deals.length, items_scanned: scanned, deals };
}

module.exports = { fetchAll, makeDeal, extractCompanies };
