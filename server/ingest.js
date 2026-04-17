#!/usr/bin/env node
// Daily ingestion cycle:
//  1. Pull all feeds into raw_items
//  2. Classify every 'new' raw_item with Gemini
//  3. Promote classified special situations into the deals table (or update existing)

require('dotenv').config();
const { query, migrate, parseJson, serializeJson } = require('./db');
const { fetchAll } = require('./feeds');
const { classify } = require('./classifier');

const MAX_CLASSIFY_PER_RUN = parseInt(process.env.MAX_CLASSIFY_PER_RUN || '200', 10);

async function classifyPending() {
  const rows = await query(
    `SELECT id, source, headline, body FROM raw_items WHERE status = $1 ORDER BY id DESC LIMIT $2`,
    ['new', MAX_CLASSIFY_PER_RUN]
  );
  console.log(`[classify] ${rows.length} pending`);
  let classified = 0, promoted = 0;

  for (const row of rows) {
    try {
      const result = await classify({
        headline: row.headline,
        body: row.body,
        source: row.source,
      });

      await query(
        `UPDATE raw_items SET status = $1, classification = $2 WHERE id = $3`,
        [result.is_special_situation ? 'classified_hit' : 'classified_miss',
         serializeJson(result), row.id]
      );
      classified++;

      if (result.is_special_situation) {
        await upsertDeal(result, row.id);
        promoted++;
      }
    } catch (e) {
      console.error('[classify item]', row.id, e.message);
      await query(`UPDATE raw_items SET status = $1 WHERE id = $2`, ['error', row.id]);
    }
  }
  return { classified, promoted };
}

async function upsertDeal(deal, sourceRawId) {
  // Try to find an existing deal for the same primary ticker pair to merge sources.
  const key = deal.target_ticker || deal.spinco_ticker || deal.acquirer_ticker;
  let existing = null;
  if (key) {
    const rows = await query(
      `SELECT * FROM deals WHERE target_ticker = $1 OR spinco_ticker = $1 OR acquirer_ticker = $1 LIMIT 1`,
      [key]
    );
    existing = rows[0];
  }

  if (existing) {
    const prev = parseJson(existing.source_ids) || [];
    if (!prev.includes(sourceRawId)) prev.push(sourceRawId);
    await query(
      `UPDATE deals SET
         status = COALESCE($1, status),
         headline = $2,
         summary = $3,
         thesis = COALESCE($4, thesis),
         risks = COALESCE($5, risks),
         source_ids = $6,
         updated_at = ${process.env.DATABASE_URL ? 'NOW()' : "datetime('now')"}
       WHERE id = $7`,
      [deal.status, deal.headline, deal.summary, deal.thesis, deal.risks,
       serializeJson(prev), existing.id]
    );
    return existing.id;
  }

  const rows = await query(
    `INSERT INTO deals (
      deal_type, status, region, headline, summary, thesis, risks,
      acquirer_name, acquirer_ticker, target_name, target_ticker,
      parent_name, parent_ticker, spinco_name, spinco_ticker,
      deal_value_usd, consideration, offer_price,
      announce_date, expected_close_date, record_date, ex_date,
      source_ids
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
    RETURNING id`,
    [
      deal.deal_type, deal.status || 'announced', deal.region, deal.headline,
      deal.summary, deal.thesis, deal.risks,
      deal.acquirer_name, deal.acquirer_ticker, deal.target_name, deal.target_ticker,
      deal.parent_name, deal.parent_ticker, deal.spinco_name, deal.spinco_ticker,
      deal.deal_value_usd, deal.consideration, deal.offer_price,
      deal.announce_date, deal.expected_close_date, deal.record_date, deal.ex_date,
      serializeJson([sourceRawId]),
    ]
  );
  return rows[0]?.id;
}

async function runCycle() {
  await migrate();
  console.log('[ingest] fetching feeds…');
  const { fetched, inserted } = await fetchAll();
  console.log(`[ingest] fetched ${fetched} items, ${inserted} new`);

  const cls = await classifyPending();
  console.log(`[ingest] classified ${cls.classified}, promoted ${cls.promoted} to deals`);
  return { fetched, inserted, ...cls };
}

if (require.main === module) {
  runCycle()
    .then(res => { console.log('[ingest] done', res); process.exit(0); })
    .catch(e => { console.error('[ingest] fatal', e); process.exit(1); });
}

module.exports = { runCycle, classifyPending, upsertDeal };
