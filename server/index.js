require('dotenv').config();
const path = require('path');
const express = require('express');
const cors = require('cors');
const { query, migrate, parseJson, serializeJson } = require('./db');
const { runCycle, classifyPending } = require('./ingest');
const { classify } = require('./classifier');
const { saveRawItems } = require('./feeds');
const { refreshAllDeals, refreshDeal } = require('./market_data');
const { marketCapBucket, dealSizeBucket, COUNTRY_TO_REGION, REGION_HIERARCHY, UI_REGIONS } = require('./tickers');
const { rollupAll, rollupDeal, listTransactionsForDeal } = require('./incentives');
const { fetchAllInsider } = require('./insider_feeds');
const { runAuthoritativeCycle } = require('./authoritative_ingest');

const app = express();
app.use(cors({ origin: process.env.PUBLIC_URL || '*' }));
app.use(express.json({ limit: '5mb' }));
app.use(express.static(path.join(__dirname, '..', 'public')));

// ---- Global safety net ----------------------------------------------------
// Never let an unhandled promise rejection (e.g. Yahoo throttling) kill the
// Node process. Log and continue.
process.on('unhandledRejection', (err) => {
  console.error('[unhandledRejection]', err && err.message ? err.message : err);
});
process.on('uncaughtException', (err) => {
  console.error('[uncaughtException]', err && err.message ? err.message : err);
});

const PORT = process.env.PORT || 3000;
const INGEST_TOKEN = process.env.INGEST_TOKEN || '';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '';

// ---- Helpers --------------------------------------------------------------
function requireIngestToken(req, res, next) {
  const got = req.header('x-ingest-token');
  if (!INGEST_TOKEN || got !== INGEST_TOKEN) {
    return res.status(401).json({ error: 'bad ingest token' });
  }
  next();
}
function requireAdmin(req, res, next) {
  const got = req.header('x-admin-password');
  if (!ADMIN_PASSWORD || got !== ADMIN_PASSWORD) {
    return res.status(401).json({ error: 'bad admin password' });
  }
  next();
}
function serializeDeal(d) {
  const out = { ...d, source_ids: parseJson(d.source_ids) || [] };
  // key_dates may be JSONB (object) or TEXT (string) depending on backend
  if (d.key_dates && typeof d.key_dates === 'string') {
    out.key_dates = parseJson(d.key_dates) || null;
  }
  return out;
}

// Human labels for the new event_type enum
const EVENT_LABELS = {
  spin_off_pending:   'Spin-off — pending',
  spin_off_completed: 'Spin-off — completed',
  ipo_pending:        'IPO — pending',
  ipo_recent:         'IPO — recent',
  merger_pending:     'Merger — pending',
  merger_completed:   'Merger — completed',
  demerger_pending:   'Demerger — pending',
};
function enrichEvent(d) {
  // Compute derived day-counters at read time so they're always fresh
  if (d.key_dates && typeof d.key_dates === 'object' && !Array.isArray(d.key_dates)) {
    const futures = Object.values(d.key_dates)
      .filter(Boolean)
      .map(v => new Date(v))
      .filter(v => !isNaN(v) && v > new Date())
      .sort((a, b) => a - b);
    if (futures.length) {
      d.days_to_event = Math.round((futures[0] - Date.now()) / 86400000);
    }
  }
  if (d.completed_date) {
    const diff = Math.round((Date.now() - new Date(d.completed_date)) / 86400000);
    d.days_since_event = diff >= 0 ? diff : null;
  }
  d.event_label = EVENT_LABELS[d.event_type] || null;
  return d;
}

// ---- Public API -----------------------------------------------------------
app.get('/api/health', (_req, res) => res.json({ ok: true, time: new Date().toISOString() }));

app.get('/api/stats', async (_req, res) => {
  const [totals] = await query(
    `SELECT
       COUNT(*) AS total,
       SUM(CASE WHEN status='announced' OR status='pending' OR status='rumored' THEN 1 ELSE 0 END) AS active,
       SUM(CASE WHEN deal_type='merger_arb' THEN 1 ELSE 0 END) AS merger_arb,
       SUM(CASE WHEN deal_type='spin_off' THEN 1 ELSE 0 END) AS spin_off,
       SUM(CASE WHEN deal_type='ipo' THEN 1 ELSE 0 END) AS ipo,
       SUM(CASE WHEN deal_type='spac' THEN 1 ELSE 0 END) AS spac,
       COALESCE(SUM(deal_value_usd),0) AS total_value_usd,
       SUM(CASE WHEN data_source_tier='official' THEN 1 ELSE 0 END) AS tier_official,
       SUM(CASE WHEN data_source_tier='aggregator' THEN 1 ELSE 0 END) AS tier_aggregator,
       SUM(CASE WHEN days_to_event IS NOT NULL AND days_to_event >= 0 AND days_to_event <= 90 THEN 1 ELSE 0 END) AS upcoming_90d,
       SUM(CASE WHEN days_since_event IS NOT NULL AND days_since_event >= 0 AND days_since_event <= 90 THEN 1 ELSE 0 END) AS recent_90d
     FROM deals`
  );
  const [pending] = await query(
    `SELECT COUNT(*) AS pending_items FROM raw_items WHERE status = $1`, ['new']
  );
  const eventRows = await query(
    `SELECT event_type, COUNT(*) AS n FROM deals
     WHERE event_type IS NOT NULL GROUP BY event_type`
  );
  // Normalize — Postgres returns strings for COUNT/SUM, SQLite returns numbers.
  const num = (v) => v == null ? 0 : Number(v);
  const events = {};
  for (const r of eventRows) events[r.event_type] = num(r.n);
  res.json({
    total: num(totals?.total), active: num(totals?.active),
    merger_arb: num(totals?.merger_arb), spin_off: num(totals?.spin_off),
    ipo: num(totals?.ipo), spac: num(totals?.spac),
    total_value_usd: num(totals?.total_value_usd),
    pending_items: num(pending?.pending_items),
    tier_official: num(totals?.tier_official),
    tier_aggregator: num(totals?.tier_aggregator),
    upcoming_90d: num(totals?.upcoming_90d),
    recent_90d: num(totals?.recent_90d),
    events,
  });
});

// Bucket thresholds — mirror tickers.js and used for server-side filtering.
const MCAP_BUCKETS = {
  mega:  [200e9, null],
  large: [10e9,  200e9],
  mid:   [2e9,   10e9],
  small: [300e6, 2e9],
  micro: [50e6,  300e6],
  nano:  [0,     50e6],
};
const DEAL_BUCKETS = {
  mega:  [10e9, null],
  large: [1e9,  10e9],
  mid:   [100e6, 1e9],
  small: [0,    100e6],
};

app.get('/api/deals', async (req, res, next) => {
  try {
  const { type, status, region, q, country, market_cap_bucket, deal_size_bucket,
          insider_signal, event_type, data_source_tier, timeframe,
          include_spacs } = req.query;
  const where = [];
  const params = [];
  if (type)   { params.push(type);   where.push(`deal_type = $${params.length}`); }
  // Hide SPAC shells by default (they dominate the IPO feed with no-price rows).
  // Opt-in with ?include_spacs=1 to see them.
  if (!include_spacs || include_spacs === '0' || include_spacs === 'false') {
    where.push(`(is_spac IS NULL OR is_spac = 0) AND deal_type != 'spac'`);
  }
  if (status) { params.push(status); where.push(`status = $${params.length}`); }
  // Nested region filter: selecting 'Europe' expands to UK/Nordic/Switzerland/EU-Continental/Europe.
  if (region) {
    const expanded = REGION_HIERARCHY[region] || [region];
    const placeholders = expanded.map(() => { params.push(null); return `$${params.length}`; });
    // Replace the just-pushed nulls with actual values
    for (let i = 0; i < expanded.length; i++) params[params.length - expanded.length + i] = expanded[i];
    where.push(`region IN (${placeholders.join(',')})`);
  }
  if (country){ params.push(country);where.push(`country = $${params.length}`); }
  if (event_type)       { params.push(event_type);       where.push(`event_type = $${params.length}`); }
  if (data_source_tier) { params.push(data_source_tier); where.push(`data_source_tier = $${params.length}`); }
  // Timeframe buckets based on completed_date / key_dates lookahead
  if (timeframe === 'upcoming') {
    where.push(`days_to_event IS NOT NULL AND days_to_event >= 0 AND days_to_event <= 90`);
  } else if (timeframe === 'recent') {
    where.push(`days_since_event IS NOT NULL AND days_since_event >= 0 AND days_since_event <= 90`);
  } else if (timeframe === 'last_30') {
    where.push(`days_since_event IS NOT NULL AND days_since_event >= 0 AND days_since_event <= 30`);
  }
  if (market_cap_bucket && MCAP_BUCKETS[market_cap_bucket]) {
    const [lo, hi] = MCAP_BUCKETS[market_cap_bucket];
    params.push(lo); where.push(`market_cap_usd >= $${params.length}`);
    if (hi != null) { params.push(hi); where.push(`market_cap_usd < $${params.length}`); }
  }
  if (deal_size_bucket && DEAL_BUCKETS[deal_size_bucket]) {
    const [lo, hi] = DEAL_BUCKETS[deal_size_bucket];
    params.push(lo); where.push(`deal_value_usd >= $${params.length}`);
    if (hi != null) { params.push(hi); where.push(`deal_value_usd < $${params.length}`); }
  }
  // Insider / incentive signal filter
  if (insider_signal === 'cluster')         where.push(`cluster_buying = 1`);
  else if (insider_signal === 'mgmt_spin')  where.push(`mgmt_moves_to_spinco = 1`);
  else if (insider_signal === 'rollover')   where.push(`(founder_rollover = 1 OR mgmt_retention_pct >= 20)`);
  else if (insider_signal === 'activist')   where.push(`activist_on_register = 1`);
  else if (insider_signal === 'any')        where.push(`(cluster_buying = 1 OR mgmt_moves_to_spinco = 1 OR founder_rollover = 1 OR activist_on_register = 1)`);
  if (q) {
    // Postgres: case-insensitive ILIKE + pg_trgm GIN indexes on headline /
    // target_name make substring searches fast. SQLite: LIKE is case-insensitive
    // for ASCII by default, translates fine.
    params.push(`%${q}%`);
    const op = process.env.DATABASE_URL ? 'ILIKE' : 'LIKE';
    where.push(`(headline ${op} $${params.length} OR summary ${op} $${params.length}
                  OR target_name ${op} $${params.length}
                  OR parent_name ${op} $${params.length} OR spinco_name ${op} $${params.length}
                  OR target_ticker ${op} $${params.length} OR acquirer_ticker ${op} $${params.length}
                  OR spinco_ticker ${op} $${params.length} OR primary_ticker ${op} $${params.length})`);
  }
  // Sort order: upcoming → nearest event first; recent → newest first; default → announce/first-seen
  // COALESCE requires matching types on Postgres; cast first_seen_at (TIMESTAMPTZ)
  // to TEXT so it can combine with announce_date (TEXT YYYY-MM-DD).
  // On SQLite first_seen_at is already TEXT — no cast needed.
  const castFsa = process.env.DATABASE_URL ? 'first_seen_at::text' : 'first_seen_at';
  let orderBy = `COALESCE(announce_date, ${castFsa}) DESC`;
  if (timeframe === 'upcoming') orderBy = 'days_to_event ASC NULLS LAST';
  else if (timeframe === 'recent' || timeframe === 'last_30') {
    orderBy = 'completed_date DESC NULLS LAST, filing_date DESC NULLS LAST';
  }
  const sql = `SELECT * FROM deals ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
               ORDER BY ${orderBy} LIMIT 500`;
  const rows = await query(sql, params);
  res.json(rows.map(d => {
    const s = enrichEvent(serializeDeal(d));
    const ap = s.announce_price != null ? Number(s.announce_price) : null;
    const cp = s.current_price   != null ? Number(s.current_price)   : null;
    s.return_pct = (ap && cp) ? ((cp - ap) / ap) * 100 : null;
    // Normalise the split spin-off returns (stored as NUMERIC → string on PG)
    if (s.parent_return_pct != null) s.parent_return_pct = Number(s.parent_return_pct);
    if (s.spinco_return_pct != null) s.spinco_return_pct = Number(s.spinco_return_pct);
    if (s.offer_price != null)         s.offer_price = Number(s.offer_price);
    if (s.unaffected_price != null)    s.unaffected_price = Number(s.unaffected_price);
    if (s.spread_to_deal_pct != null)  s.spread_to_deal_pct = Number(s.spread_to_deal_pct);
    s.market_cap_bucket = marketCapBucket(s.market_cap_usd);
    s.deal_size_bucket  = dealSizeBucket(s.deal_value_usd);
    // Compact incentive badges for the screener row
    s.incentive_badges = buildIncentiveBadges(s);
    return s;
  }));
  } catch (e) { console.error('[deals]', e.message); res.status(500).json({ error: e.message }); }
});

// ---- Events (calendar / recent) -----------------------------------------
// Thin convenience views over /api/deals with pre-set timeframe filters.
app.get('/api/events/upcoming', async (req, res) => {
  const days = Math.min(parseInt(req.query.days || '90', 10), 365);
  const eventType = req.query.event_type;
  const where = ['days_to_event IS NOT NULL', 'days_to_event >= 0'];
  const params = [days];
  where.push(`days_to_event <= $1`);
  if (eventType) { params.push(eventType); where.push(`event_type = $${params.length}`); }
  const rows = await query(
    `SELECT * FROM deals WHERE ${where.join(' AND ')} ORDER BY days_to_event ASC LIMIT 200`,
    params
  );
  res.json(rows.map(d => enrichEvent(serializeDeal(d))));
});

app.get('/api/events/recent', async (req, res) => {
  const days = Math.min(parseInt(req.query.days || '90', 10), 365);
  const eventType = req.query.event_type;
  const where = ['days_since_event IS NOT NULL', 'days_since_event >= 0'];
  const params = [days];
  where.push(`days_since_event <= $1`);
  if (eventType) { params.push(eventType); where.push(`event_type = $${params.length}`); }
  const rows = await query(
    `SELECT * FROM deals WHERE ${where.join(' AND ')}
     ORDER BY completed_date DESC NULLS LAST, days_since_event ASC LIMIT 200`,
    params
  );
  res.json(rows.map(d => enrichEvent(serializeDeal(d))));
});

// Build a small list of badge objects summarising the incentive layer. Shown
// as the "Skin" column on the screener.
function buildIncentiveBadges(d) {
  const out = [];
  if (d.cluster_buying === 1 || d.cluster_buying === true) {
    out.push({ key: 'cluster', label: 'Cluster buy', icon: '🟢',
      tooltip: `${d.insider_buy_count_6m || 0} insiders bought in last 180d` });
  }
  if (d.mgmt_moves_to_spinco === 1 || d.mgmt_moves_to_spinco === true) {
    out.push({ key: 'mgmt_spin', label: 'Mgmt to SpinCo', icon: '👤',
      tooltip: 'A senior exec is moving to lead the SpinCo' });
  }
  if (d.founder_rollover === 1 || (d.mgmt_retention_pct != null && Number(d.mgmt_retention_pct) >= 20)) {
    out.push({ key: 'rollover', label: 'Rollover', icon: '💼',
      tooltip: d.mgmt_retention_pct ? `Mgmt rolls ${d.mgmt_retention_pct}%` : 'Founder rolling equity' });
  }
  if (d.activist_on_register === 1 || d.activist_on_register === true) {
    out.push({ key: 'activist', label: 'Activist', icon: '⚔️',
      tooltip: 'Known activist on the register' });
  }
  if (d.trading_below_insider_price === 1 || d.trading_below_insider_price === true) {
    out.push({ key: 'below_insider', label: 'Below insider $', icon: '📍',
      tooltip: `Current price below avg insider buy ($${Number(d.avg_insider_buy_price || 0).toFixed(2)})` });
  }
  return out;
}

app.get('/api/deals/:id', async (req, res) => {
  const [deal] = await query(`SELECT * FROM deals WHERE id = $1`, [req.params.id]);
  if (!deal) return res.status(404).json({ error: 'not found' });
  const d = enrichEvent(serializeDeal(deal));
  // Normalise NUMERIC → number so the client can safely .toFixed() etc.
  for (const k of ['parent_return_pct','spinco_return_pct','parent_baseline_price',
                    'spinco_baseline_price','parent_current_price','spinco_current_price',
                    'announce_price','current_price','market_cap_usd','deal_value_usd',
                    'offer_price','unaffected_price','spread_to_deal_pct',
                    'consideration_cash','consideration_stock_ratio']) {
    if (d[k] != null && typeof d[k] === 'string') d[k] = Number(d[k]);
  }

  // Attach news_items linked to this deal (from authoritative news-link step)
  try {
    const news = await query(
      `SELECT id, source, url, headline, summary, published_at, matched_ticker, match_kind
       FROM news_items WHERE deal_id = $1
       ORDER BY published_at DESC NULLS LAST, id DESC LIMIT 50`,
      [req.params.id]
    );
    d.news_items = news;
  } catch (_e) { d.news_items = []; }

  if (d.source_ids?.length) {
    // Postgres IN with array param — use ANY(); for SQLite expand to placeholders
    let sources;
    if (process.env.DATABASE_URL) {
      sources = await query(
        `SELECT id, source, url, headline, published_at FROM raw_items WHERE id = ANY($1::int[])`,
        [d.source_ids]
      );
    } else {
      const placeholders = d.source_ids.map((_, i) => `$${i+1}`).join(',');
      sources = await query(
        `SELECT id, source, url, headline, published_at FROM raw_items WHERE id IN (${placeholders})`,
        d.source_ids
      );
    }
    d.sources = sources;
  } else {
    d.sources = [];
  }
  res.json(d);
});

// ---- Ingest endpoints -----------------------------------------------------
app.post('/api/ingest/email', requireIngestToken, async (req, res) => {
  const { fromAddress, subject, body, receivedAt, links } = req.body || {};
  if (!subject && !body) return res.status(400).json({ error: 'empty email' });
  try {
    const rows = await query(
      `INSERT INTO raw_items (source, source_id, url, headline, body, published_at)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (source, source_id) DO NOTHING
       RETURNING id`,
      ['email', `${fromAddress || 'unknown'}|${receivedAt || Date.now()}|${(subject||'').slice(0,80)}`,
       (links && links[0]) || null, (subject || '').slice(0, 300),
       [body, links ? '\nLINKS:\n' + links.join('\n') : ''].filter(Boolean).join(''),
       receivedAt || null]
    );
    if (rows.length === 0) return res.status(200).json({ ok: true, duplicate: true });
    res.status(201).json({ id: rows[0].id });
  } catch (e) {
    console.error('[ingest/email]', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/ingest/run', requireIngestToken, async (_req, res) => {
  try {
    const result = await runCycle();
    res.json({ ok: true, ...result });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Refresh market data (prices + mcap + FX) for all active deals.
// Accepts EITHER x-ingest-token (for the cron) OR x-admin-password (for manual UI).
app.post('/api/admin/refresh-prices', async (req, res) => {
  const ingestOk = INGEST_TOKEN && req.header('x-ingest-token') === INGEST_TOKEN;
  const adminOk  = ADMIN_PASSWORD && req.header('x-admin-password') === ADMIN_PASSWORD;
  if (!ingestOk && !adminOk) return res.status(401).json({ error: 'unauthorized' });
  try {
    const activeOnly = req.query.all !== '1';
    const limit = Math.min(parseInt(req.query.limit || '500', 10), 2000);
    const result = await refreshAllDeals({ activeOnly, limit });
    // Re-roll incentives so trading_below_insider_price uses fresh prices.
    const roll = await rollupAll({ activeOnly, limit }).catch(e => ({ error: e.message }));
    res.json({ ok: true, ...result, incentive_rollup: roll });
  } catch (e) {
    console.error('[refresh-prices]', e);
    res.status(500).json({ error: e.message });
  }
});

// Reconciler: auto-mark completed spin-offs, fix country misclassifications.
// Safe to run daily — idempotent and conservative.
app.post('/api/admin/run-reconcile', async (req, res) => {
  const ingestOk = INGEST_TOKEN && req.header('x-ingest-token') === INGEST_TOKEN;
  const adminOk  = ADMIN_PASSWORD && req.header('x-admin-password') === ADMIN_PASSWORD;
  if (!ingestOk && !adminOk) return res.status(401).json({ error: 'unauthorized' });
  try {
    const { reconcileAll } = require('./reconcile');
    const dryRun = req.query.dry === '1';
    const limit = Math.min(parseInt(req.query.limit || '500', 10), 2000);
    const onlyIds = req.query.ids ? String(req.query.ids).split(',').map(s => parseInt(s, 10)).filter(Boolean) : null;
    const result = await reconcileAll({ dryRun, limit, onlyIds });
    res.json({ ok: true, dryRun, ...result });
  } catch (e) {
    console.error('[reconcile]', e);
    res.status(500).json({ error: e.message });
  }
});

// One-shot backfill: flag existing IPO deals as SPACs by name/symbol pattern.
// Run once after deploy. Idempotent.
app.post('/api/admin/backfill-spacs', async (req, res) => {
  const ingestOk = INGEST_TOKEN && req.header('x-ingest-token') === INGEST_TOKEN;
  const adminOk  = ADMIN_PASSWORD && req.header('x-admin-password') === ADMIN_PASSWORD;
  if (!ingestOk && !adminOk) return res.status(401).json({ error: 'unauthorized' });
  try {
    const rows = await query(
      `SELECT id, headline, target_name, target_ticker, primary_ticker
         FROM deals
        WHERE deal_type IN ('ipo','spac') AND (is_spac IS NULL OR is_spac = 0)`
    );
    const nameRe = /\bacquisition\s+(corp|company|limited|plc|inc)\b|\bcapital\s+acquisition\b|\bblank[-\s]?check\b|\bSPAC\b/i;
    let flagged = 0;
    for (const r of rows) {
      const blob = `${r.headline || ''} ${r.target_name || ''}`;
      if (nameRe.test(blob)) {
        await query(`UPDATE deals SET is_spac = 1, deal_type = 'spac' WHERE id = $1`, [r.id]);
        flagged++;
      }
    }
    res.json({ ok: true, scanned: rows.length, flagged });
  } catch (e) {
    console.error('[backfill-spacs]', e);
    res.status(500).json({ error: e.message });
  }
});

// One-shot backfill: re-run SEC merger enrichment on existing merger_arb deals
// to populate offer_price / expected_close / deal_value_usd / acquirer_name.
app.post('/api/admin/backfill-merger-terms', async (req, res) => {
  const ingestOk = INGEST_TOKEN && req.header('x-ingest-token') === INGEST_TOKEN;
  const adminOk  = ADMIN_PASSWORD && req.header('x-admin-password') === ADMIN_PASSWORD;
  if (!ingestOk && !adminOk) return res.status(401).json({ error: 'unauthorized' });
  try {
    const dryRun = req.query.dry === '1';
    const limit = Math.min(parseInt(req.query.limit || '150', 10), 500);
    const { extractOfferTerms } = require('./sources/sec');
    // Deals missing at least one key merger-arb field.
    const rows = await query(
      `SELECT id, source_filing_url, source_cik, offer_price, expected_close_date,
              deal_value_usd, acquirer_name, consideration_type
         FROM deals
        WHERE deal_type = 'merger_arb'
          AND primary_source IN ('sec_defm14a', 'sec_prem14a', 'sec_s4', 'sec_defs14a')
          AND source_filing_url IS NOT NULL
          AND source_cik IS NOT NULL
          AND (offer_price IS NULL OR expected_close_date IS NULL OR deal_value_usd IS NULL OR acquirer_name IS NULL)
        ORDER BY filing_date DESC
        LIMIT $1`, [limit]
    );
    let updated = 0, scanned = 0, errors = 0;
    const samples = [];
    // Extract accession from source_filing_url.
    // Format: https://www.sec.gov/Archives/edgar/data/<cik>/<accession-no-dashes>/<doc>
    const accRe = /\/Archives\/edgar\/data\/\d+\/(\d{18})\//;
    for (const r of rows) {
      scanned++;
      const m = String(r.source_filing_url).match(accRe);
      if (!m) continue;
      // Reconstruct dashed accession: 18 digits → 10-2-6.
      const raw = m[1];
      const dashed = `${raw.slice(0, 10)}-${raw.slice(10, 12)}-${raw.slice(12)}`;
      try {
        const terms = await extractOfferTerms(dashed, r.source_cik);
        if (!terms) continue;
        const sets = [];
        const vals = [];
        if (!r.offer_price && terms.offer_price != null) { sets.push(`offer_price = $${sets.length + 1}`); vals.push(terms.offer_price); }
        if (!r.expected_close_date && terms.expected_close_date) { sets.push(`expected_close_date = $${sets.length + 1}`); vals.push(terms.expected_close_date); }
        if (!r.deal_value_usd && terms.deal_value_usd) { sets.push(`deal_value_usd = $${sets.length + 1}`); vals.push(terms.deal_value_usd); }
        if (!r.acquirer_name && terms.acquirer_name) { sets.push(`acquirer_name = $${sets.length + 1}`); vals.push(terms.acquirer_name); }
        if (!r.consideration_type && terms.consideration_type) {
          sets.push(`consideration_type = $${sets.length + 1}`); vals.push(terms.consideration_type);
          if (terms.consideration_cash != null) { sets.push(`consideration_cash = $${sets.length + 1}`); vals.push(terms.consideration_cash); }
          if (terms.consideration_stock_ratio != null) { sets.push(`consideration_stock_ratio = $${sets.length + 1}`); vals.push(terms.consideration_stock_ratio); }
        }
        if (!sets.length) continue;
        if (samples.length < 5) samples.push({ id: r.id, ...terms });
        if (!dryRun) {
          vals.push(r.id);
          await query(`UPDATE deals SET ${sets.join(', ')} WHERE id = $${vals.length}`, vals);
        }
        updated++;
      } catch (e) {
        errors++;
      }
    }
    res.json({ ok: true, dryRun, scanned, updated, errors, samples });
  } catch (e) {
    console.error('[backfill-merger-terms]', e);
    res.status(500).json({ error: e.message });
  }
});

// Incentive detail for a single deal — returns transactions list + rollup.
app.get('/api/deals/:id/incentives', async (req, res) => {
  const [deal] = await query(`SELECT * FROM deals WHERE id = $1`, [req.params.id]);
  if (!deal) return res.status(404).json({ error: 'not found' });
  try {
    const transactions = await listTransactionsForDeal(deal);
    const rollup = {
      insider_buy_count_6m: deal.insider_buy_count_6m,
      insider_buy_usd_6m: deal.insider_buy_usd_6m,
      insider_sell_usd_6m: deal.insider_sell_usd_6m,
      insider_net_usd_6m: deal.insider_net_usd_6m,
      cluster_buying: deal.cluster_buying,
      avg_insider_buy_price: deal.avg_insider_buy_price,
      trading_below_insider_price: deal.trading_below_insider_price,
      mgmt_moves_to_spinco: deal.mgmt_moves_to_spinco,
      mgmt_retention_pct: deal.mgmt_retention_pct,
      sponsor_promote_pct: deal.sponsor_promote_pct,
      founder_rollover: deal.founder_rollover,
      bidder_stake_pre_deal: deal.bidder_stake_pre_deal,
      activist_on_register: deal.activist_on_register,
      incentive_notes: deal.incentive_notes,
      insider_refreshed_at: deal.insider_refreshed_at,
    };
    res.json({
      deal_id: deal.id,
      ticker: deal.primary_ticker,
      rollup,
      incentive_badges: buildIncentiveBadges(deal),
      transactions,
    });
  } catch (e) {
    console.error('[incentives]', e);
    res.status(500).json({ error: e.message });
  }
});

// Refresh insider feeds + rollup (admin or ingest token). Runs in the cron too.
app.post('/api/admin/refresh-insider', async (req, res) => {
  const ingestOk = INGEST_TOKEN && req.header('x-ingest-token') === INGEST_TOKEN;
  const adminOk  = ADMIN_PASSWORD && req.header('x-admin-password') === ADMIN_PASSWORD;
  if (!ingestOk && !adminOk) return res.status(401).json({ error: 'unauthorized' });
  try {
    const feeds = await fetchAllInsider();
    const roll = await rollupAll({ activeOnly: req.query.all !== '1', limit: 500 });
    res.json({ ok: true, insider: feeds, rollup: roll });
  } catch (e) {
    console.error('[refresh-insider]', e);
    res.status(500).json({ error: e.message });
  }
});

// Refresh a single deal by id (admin only).
app.post('/api/admin/deals/:id/refresh', async (req, res) => {
  const adminOk = ADMIN_PASSWORD && req.header('x-admin-password') === ADMIN_PASSWORD;
  if (!adminOk) return res.status(401).json({ error: 'unauthorized' });
  try {
    const [deal] = await query(`SELECT * FROM deals WHERE id = $1`, [req.params.id]);
    if (!deal) return res.status(404).json({ error: 'not found' });
    const r = await refreshDeal(deal);
    // Reload the deal so the rollup sees the fresh current_price
    const [updated] = await query(`SELECT * FROM deals WHERE id = $1`, [req.params.id]);
    const incRoll = await rollupDeal(updated).catch(e => ({ error: e.message }));
    res.json({ market: r, incentives: incRoll });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Purge deals matching a filter AND reset their source raw_items to 'new' so
// they get re-classified on next run. Useful after tightening the classifier
// prompt (e.g., to clean up the old overly-broad merger_arb bucket).
app.post('/api/ingest/purge-and-reclassify', requireIngestToken, async (req, res) => {
  try {
    const { deal_type, region } = req.body || {};
    if (!deal_type && !region) {
      return res.status(400).json({ error: 'must provide deal_type and/or region' });
    }
    const where = [], params = [];
    if (deal_type) { params.push(deal_type); where.push(`deal_type = $${params.length}`); }
    if (region)    { params.push(region);    where.push(`region = $${params.length}`); }

    // Collect all source raw_item ids attached to matching deals
    const deals = await query(
      `SELECT id, source_ids FROM deals WHERE ${where.join(' AND ')}`, params
    );
    const rawIds = new Set();
    for (const d of deals) {
      const ids = parseJson(d.source_ids) || [];
      ids.forEach(i => rawIds.add(i));
    }

    // Delete matching deals
    await query(`DELETE FROM deals WHERE ${where.join(' AND ')}`, params);

    // Reset their raw items to 'new'
    let resetCount = 0;
    if (rawIds.size) {
      const idList = [...rawIds];
      if (process.env.DATABASE_URL) {
        const result = await query(
          `UPDATE raw_items SET status = 'new' WHERE id = ANY($1::int[]) RETURNING id`,
          [idList]
        );
        resetCount = result.length;
      } else {
        const placeholders = idList.map((_, i) => `$${i+1}`).join(',');
        await query(`UPDATE raw_items SET status = 'new' WHERE id IN (${placeholders})`, idList);
        resetCount = idList.length;
      }
    }

    // Re-classify them
    const cls = await classifyPending();
    res.json({ ok: true, deals_purged: deals.length, raw_reset: resetCount, ...cls });
  } catch (e) {
    console.error('[purge-and-reclassify]', e);
    res.status(500).json({ error: e.message });
  }
});

// Reset classification on already-seen items so they get re-classified on next run.
// Useful after improving the classifier prompt or body extraction.
app.post('/api/ingest/reclassify', requireIngestToken, async (req, res) => {
  try {
    const onlyMisses = req.body?.onlyMisses !== false; // default: reset misses only
    const limit = Math.min(parseInt(req.body?.limit || '500', 10), 2000);
    const whereStatus = onlyMisses ? "status IN ('classified_miss','error')" : "status != 'new'";
    // Reset to 'new' so classifyPending picks them up
    const result = await query(
      `UPDATE raw_items SET status = 'new'
       WHERE id IN (SELECT id FROM raw_items WHERE ${whereStatus} ORDER BY id DESC LIMIT $1)
       RETURNING id`,
      [limit]
    );
    const reset = result.length;
    // Now classify them
    const cls = await classifyPending();
    res.json({ ok: true, reset, ...cls });
  } catch (e) {
    console.error('[reclassify]', e);
    res.status(500).json({ error: e.message });
  }
});

// ---- Admin ---------------------------------------------------------------
app.post('/api/admin/login', (req, res) => {
  if ((req.body?.password || '') === ADMIN_PASSWORD && ADMIN_PASSWORD) {
    return res.json({ ok: true });
  }
  res.status(401).json({ error: 'bad password' });
});

app.get('/api/admin/raw', requireAdmin, async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || '200', 10), 2000);
  const src = req.query.source;
  const status = req.query.status;
  const where = [];
  const params = [];
  if (src)    { params.push(src);    where.push(`source = $${params.length}`); }
  if (status) { params.push(status); where.push(`status = $${params.length}`); }
  const sql = `SELECT id, source, headline, body, status, classification, published_at, fetched_at
               FROM raw_items ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
               ORDER BY id DESC LIMIT ${limit}`;
  const rows = await query(sql, params);
  res.json(rows);
});

app.get('/api/admin/raw/:id', requireAdmin, async (req, res) => {
  const [row] = await query(`SELECT * FROM raw_items WHERE id = $1`, [req.params.id]);
  if (!row) return res.status(404).json({ error: 'not found' });
  res.json(row);
});

app.post('/api/admin/deals/:id', requireAdmin, async (req, res) => {
  const fields = ['status', 'deal_type', 'region', 'summary', 'thesis', 'risks',
    'current_price', 'spread_pct', 'expected_close_date'];
  const set = [], params = [];
  for (const f of fields) {
    if (req.body[f] !== undefined) {
      params.push(req.body[f]); set.push(`${f} = $${params.length}`);
    }
  }
  if (!set.length) return res.status(400).json({ error: 'no fields' });
  params.push(req.params.id);
  await query(`UPDATE deals SET ${set.join(', ')} WHERE id = $${params.length}`, params);
  const [row] = await query(`SELECT * FROM deals WHERE id = $1`, [req.params.id]);
  res.json(serializeDeal(row));
});

app.delete('/api/admin/deals/:id', requireAdmin, async (req, res) => {
  await query(`DELETE FROM deals WHERE id = $1`, [req.params.id]);
  res.json({ ok: true });
});

// ---- Authoritative ingest ------------------------------------------------
// Runs the regulator-first pipeline (SEC → LSE → Nordic → StockAnalysis → news link).
// Accepts EITHER x-ingest-token (for cron) OR x-admin-password (for manual UI).
app.post('/api/admin/run-auth-ingest', async (req, res) => {
  const ingestOk = INGEST_TOKEN && req.header('x-ingest-token') === INGEST_TOKEN;
  const adminOk  = ADMIN_PASSWORD && req.header('x-admin-password') === ADMIN_PASSWORD;
  if (!ingestOk && !adminOk) return res.status(401).json({ error: 'unauthorized' });
  try {
    const wipe = req.body?.wipe === true || req.query.wipe === '1';
    const skipMarket = req.body?.skipMarket === true || req.query.skipMarket === '1';
    // Heavy operation — may take several minutes. Run async and return immediately
    // if client asks for fire-and-forget mode via ?async=1.
    if (req.query.async === '1') {
      runAuthoritativeCycle({ wipeExisting: wipe, skipMarket }).then(
        r => console.log('[auth-ingest] async done', JSON.stringify(r)),
        e => console.error('[auth-ingest] async failed', e)
      );
      return res.json({ ok: true, async: true, started: new Date().toISOString() });
    }
    const result = await runAuthoritativeCycle({ wipeExisting: wipe, skipMarket });
    res.json({ ok: true, ...result });
  } catch (e) {
    console.error('[run-auth-ingest]', e);
    res.status(500).json({ error: e.message });
  }
});

// ---- Start ---------------------------------------------------------------
migrate().then(() => {
  app.listen(PORT, () => console.log(`SpecialSits listening on :${PORT}`));
}).catch(e => {
  console.error('migrate failed', e);
  process.exit(1);
});
