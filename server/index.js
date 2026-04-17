require('dotenv').config();
const path = require('path');
const express = require('express');
const cors = require('cors');
const { query, migrate, parseJson, serializeJson } = require('./db');
const { runCycle, classifyPending } = require('./ingest');
const { classify } = require('./classifier');
const { saveRawItems } = require('./feeds');

const app = express();
app.use(cors({ origin: process.env.PUBLIC_URL || '*' }));
app.use(express.json({ limit: '5mb' }));
app.use(express.static(path.join(__dirname, '..', 'public')));

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
  return { ...d, source_ids: parseJson(d.source_ids) || [] };
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
       COALESCE(SUM(deal_value_usd),0) AS total_value_usd
     FROM deals`
  );
  const [pending] = await query(
    `SELECT COUNT(*) AS pending_items FROM raw_items WHERE status = $1`, ['new']
  );
  // Normalize — Postgres returns strings for COUNT/SUM, SQLite returns numbers.
  const num = (v) => v == null ? 0 : Number(v);
  res.json({
    total: num(totals?.total), active: num(totals?.active),
    merger_arb: num(totals?.merger_arb), spin_off: num(totals?.spin_off),
    ipo: num(totals?.ipo), spac: num(totals?.spac),
    total_value_usd: num(totals?.total_value_usd),
    pending_items: num(pending?.pending_items),
  });
});

app.get('/api/deals', async (req, res) => {
  const { type, status, region, q } = req.query;
  const where = [];
  const params = [];
  if (type)   { params.push(type);   where.push(`deal_type = $${params.length}`); }
  if (status) { params.push(status); where.push(`status = $${params.length}`); }
  if (region) { params.push(region); where.push(`region = $${params.length}`); }
  if (q) {
    params.push(`%${q}%`);
    where.push(`(headline LIKE $${params.length} OR summary LIKE $${params.length}
                  OR target_ticker LIKE $${params.length} OR acquirer_ticker LIKE $${params.length}
                  OR spinco_ticker LIKE $${params.length})`);
  }
  const sql = `SELECT * FROM deals ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
               ORDER BY COALESCE(announce_date, first_seen_at) DESC LIMIT 500`;
  const rows = await query(sql, params);
  res.json(rows.map(serializeDeal));
});

app.get('/api/deals/:id', async (req, res) => {
  const [deal] = await query(`SELECT * FROM deals WHERE id = $1`, [req.params.id]);
  if (!deal) return res.status(404).json({ error: 'not found' });
  const d = serializeDeal(deal);
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

// ---- Start ---------------------------------------------------------------
migrate().then(() => {
  app.listen(PORT, () => console.log(`SpecialSits listening on :${PORT}`));
}).catch(e => {
  console.error('migrate failed', e);
  process.exit(1);
});
