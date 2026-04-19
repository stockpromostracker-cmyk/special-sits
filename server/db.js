// Dual-driver database layer. Uses Postgres if DATABASE_URL is set
// (Railway prod), otherwise SQLite at ./data/specialsits.db (local dev).
// Exposes a single `query(sql, params)` helper that returns rows.

const fs = require('fs');
const path = require('path');

const USE_PG = !!process.env.DATABASE_URL;
let pg, sqlite, sqliteDb;

if (USE_PG) {
  // Railway's internal Postgres (*.railway.internal) doesn't use SSL;
  // external / proxy hosts typically do. Auto-detect.
  const url = process.env.DATABASE_URL;
  const needsSsl = !/railway\.internal/.test(url) && !/localhost/.test(url);
  pg = new (require('pg').Pool)({
    connectionString: url,
    ssl: needsSsl ? { rejectUnauthorized: false } : false,
  });
} else {
  const Database = require('better-sqlite3');
  const dir = path.join(__dirname, '..', 'data');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  sqliteDb = new Database(path.join(dir, 'specialsits.db'));
  sqliteDb.pragma('journal_mode = WAL');
}

// Translate $1,$2 placeholders (pg) <-> ?,? (sqlite) transparently.
function toSqlite(sql) {
  let i = 0;
  return sql.replace(/\$\d+/g, () => '?');
}

async function query(sql, params = []) {
  if (USE_PG) {
    const res = await pg.query(sql, params);
    return res.rows;
  }
  // SQLite path — translate Postgres dialect where possible.
  let liteSql = toSqlite(sql);
  // SQLite (3.35+) supports RETURNING and ON CONFLICT DO NOTHING natively.
  if (/^\s*(select|with|pragma)/i.test(liteSql)) {
    return sqliteDb.prepare(liteSql).all(...params);
  }
  // If the SQL has RETURNING, SQLite can execute it directly as a statement with .all()
  if (/returning\s+/i.test(liteSql)) {
    try {
      return sqliteDb.prepare(liteSql).all(...params);
    } catch (e) {
      // Fallback: run + lookup for older SQLite
      const stripped = liteSql.replace(/\s+returning[\s\S]+$/i, '');
      const info = sqliteDb.prepare(stripped).run(...params);
      if (info.lastInsertRowid) {
        const table = liteSql.match(/(?:insert\s+into|update|delete\s+from)\s+(\w+)/i)?.[1];
        if (table) {
          const row = sqliteDb.prepare(`SELECT * FROM ${table} WHERE rowid = ?`).get(info.lastInsertRowid);
          return row ? [row] : [];
        }
      }
      return [];
    }
  }
  sqliteDb.prepare(liteSql).run(...params);
  return [];
}

// Schema — same DDL works for both Postgres and SQLite
// (with small dialect tweaks handled inline).
async function migrate() {
  const pkSerial = USE_PG ? 'SERIAL PRIMARY KEY' : 'INTEGER PRIMARY KEY AUTOINCREMENT';
  const ts = USE_PG ? 'TIMESTAMPTZ DEFAULT NOW()' : "TEXT DEFAULT (datetime('now'))";
  const jsonType = USE_PG ? 'JSONB' : 'TEXT';

  await query(`CREATE TABLE IF NOT EXISTS raw_items (
    id ${pkSerial},
    source TEXT NOT NULL,
    source_id TEXT,
    url TEXT,
    headline TEXT,
    body TEXT,
    published_at TEXT,
    fetched_at ${ts},
    status TEXT DEFAULT 'new',
    classification ${jsonType},
    UNIQUE(source, source_id)
  )`);

  await query(`CREATE TABLE IF NOT EXISTS deals (
    id ${pkSerial},
    deal_type TEXT NOT NULL,
    status TEXT DEFAULT 'announced',
    region TEXT,
    headline TEXT NOT NULL,
    summary TEXT,
    thesis TEXT,
    risks TEXT,
    acquirer_name TEXT,
    acquirer_ticker TEXT,
    target_name TEXT,
    target_ticker TEXT,
    parent_name TEXT,
    parent_ticker TEXT,
    spinco_name TEXT,
    spinco_ticker TEXT,
    deal_value_usd NUMERIC,
    consideration TEXT,
    offer_price NUMERIC,
    current_price NUMERIC,
    spread_pct NUMERIC,
    announce_date TEXT,
    expected_close_date TEXT,
    record_date TEXT,
    ex_date TEXT,
    source_ids ${jsonType},
    first_seen_at ${ts},
    updated_at ${ts}
  )`);

  await query(`CREATE INDEX IF NOT EXISTS idx_raw_status ON raw_items(status)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_deals_type ON deals(deal_type)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_deals_status ON deals(status)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_deals_region ON deals(region)`);

  // ---- Market-data + geo columns (added later; use ALTER IF NOT EXISTS) ----
  // Postgres supports ADD COLUMN IF NOT EXISTS natively (9.6+).
  // SQLite pre-3.35 does not, so we try/ignore duplicate-column errors.
  const newCols = [
    ['primary_ticker',       'TEXT'],
    ['yahoo_symbol',         'TEXT'],
    ['country',              'TEXT'],
    ['sector',               'TEXT'],
    ['industry',             'TEXT'],
    ['market_cap_usd',       'NUMERIC'],
    ['currency',             'TEXT'],
    ['announce_price',       'NUMERIC'],
    ['market_refreshed_at',  USE_PG ? 'TIMESTAMPTZ' : 'TEXT'],
    // Incentive layer columns ---------------------------------------
    // Deal-type-specific signals extracted by the LLM classifier.
    ['mgmt_moves_to_spinco', 'INTEGER'],   // 0/1 — a senior exec (CEO/CFO/COO) announced to lead the spin-off SpinCo
    ['mgmt_retention_pct',   'NUMERIC'],   // Rollover equity % by mgmt in a take-private / going-private
    ['sponsor_promote_pct',  'NUMERIC'],   // SPAC sponsor promote %, typically 20
    ['founder_rollover',     'INTEGER'],   // 0/1 — founder rolling equity rather than cashing out
    ['bidder_stake_pre_deal','NUMERIC'],   // Acquirer's pre-existing % stake in target (merger_arb)
    ['activist_on_register', 'INTEGER'],   // 0/1 — known activist holds an on-register stake
    ['incentive_notes',      'TEXT'],      // Free-text LLM summary of incentive signals
    // Rollup metrics computed from insider_transactions ---------------
    ['insider_buy_count_6m',       'INTEGER'],
    ['insider_buy_usd_6m',         'NUMERIC'],
    ['insider_sell_usd_6m',        'NUMERIC'],
    ['insider_net_usd_6m',         'NUMERIC'],
    ['cluster_buying',             'INTEGER'],  // 0/1 — >=3 distinct insiders bought in last 6m
    ['avg_insider_buy_price',      'NUMERIC'],  // Volume-weighted avg USD price
    ['trading_below_insider_price','INTEGER'],  // 0/1 — current_price below avg_insider_buy_price
    ['insider_refreshed_at',       USE_PG ? 'TIMESTAMPTZ' : 'TEXT'],
    // Authoritative-source layer (regulator-first rebuild) --------------
    ['event_type',           'TEXT'],      // spin_off_pending | spin_off_completed | ipo_recent | ipo_pending | merger_pending | merger_completed | demerger_pending
    ['data_source_tier',     'TEXT'],      // official (regulator) | aggregator (stockanalysis) | news
    ['primary_source',       'TEXT'],      // sec_10_12b | sec_8k_201 | sec_s1 | sec_424b4 | sec_defm14a | lse_rns | euronext | mfn | stockanalysis_spin | stockanalysis_ipo | news
    ['source_filing_url',    'TEXT'],      // Direct link to regulator filing / official announcement
    ['source_cik',           'TEXT'],      // For US: SEC CIK of the filer
    ['confidence',           'NUMERIC'],   // 0–1: 1.0 = regulator filing; 0.7 = aggregator; 0.3 = news-only
    ['key_dates',            jsonType],    // JSON: {filing_date, record_date, ex_date, first_trade_date, effective_date, expected_close_date, completed_date}
    ['completed_date',       'TEXT'],      // YYYY-MM-DD for completed events (first trade / spin effective / merger close)
    ['filing_date',          'TEXT'],      // First regulator filing date
    ['ipo_price',            'NUMERIC'],   // For IPOs, offering price
    ['days_to_event',        'INTEGER'],   // Cached: days from today to next key date (NULL if past)
    ['days_since_event',     'INTEGER'],   // Cached: days since completion (NULL if pending)
    // Separate spin-off returns: parent (RemainCo) vs spinco are economically different
    // investments post-spin. Both calculated since the ex-date baseline close.
    ['parent_return_pct',    'NUMERIC'],   // % return on the parent ticker since ex-date
    ['spinco_return_pct',    'NUMERIC'],   // % return on the spinco ticker since first-trade date
    ['parent_baseline_price','NUMERIC'],   // Parent close ON ex-date (when stub started trading)
    ['spinco_baseline_price','NUMERIC'],   // SpinCo open/first-trade price (USD)
    ['parent_current_price', 'NUMERIC'],   // Latest parent price in USD
    ['spinco_current_price', 'NUMERIC'],   // Latest spinco price in USD
    // Merger arb columns -----------------------------------------------
    ['unaffected_price',     'NUMERIC'],   // Close 1 trading day before announce_date (pre-leak reference)
    ['spread_to_deal_pct',   'NUMERIC'],   // (offer_price - current_price) / current_price * 100
    ['consideration_type',   'TEXT'],      // 'cash' | 'stock' | 'mixed' | 'unknown'
    ['consideration_cash',   'NUMERIC'],   // Per-share cash component in USD
    ['consideration_stock_ratio', 'NUMERIC'], // Per-share stock exchange ratio (target shares per acquirer share)
    ['acquirer_proxy_ticker','TEXT'],      // If stock deal, acquirer ticker used to value stock component
    ['announce_date_source', 'TEXT'],      // 'sec_8k_101' | 'sec_defa14a' | 'sec_prem14a' | 'filing_date' — how we derived announce_date
    ['is_spac',              'INTEGER'],   // 0/1 — SPAC / blank-check shell (hidden by default from IPO feed)
  ];
  for (const [name, type] of newCols) {
    try {
      if (USE_PG) {
        await query(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS ${name} ${type}`);
      } else {
        // SQLite: try to add; ignore if already present.
        await query(`ALTER TABLE deals ADD COLUMN ${name} ${type}`);
      }
    } catch (e) {
      if (!/duplicate column|already exists/i.test(e.message)) {
        console.warn(`[migrate] ADD COLUMN ${name} failed:`, e.message);
      }
    }
  }
  await query(`CREATE INDEX IF NOT EXISTS idx_deals_country ON deals(country)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_deals_mcap   ON deals(market_cap_usd)`);

  // ---- Insider transactions (universal schema across US / UK / Nordic) ----
  // One row per disclosed transaction. Linked to a deal only when the reporting
  // issuer matches a deal's primary_ticker — otherwise rows are orphaned and
  // available for later rollup if a matching deal appears.
  await query(`CREATE TABLE IF NOT EXISTS insider_transactions (
    id ${pkSerial},
    source TEXT NOT NULL,              -- sec_form4 | lse_rns | nasdaq_nordic | oslo | euronext_notif
    source_id TEXT,                    -- e.g. SEC accession number; RNS announcement id
    url TEXT,
    issuer_name TEXT,
    issuer_country TEXT,               -- ISO-2
    issuer_ticker TEXT,                -- normalized as EXCHANGE:SYMBOL
    insider_name TEXT,
    insider_title TEXT,                -- e.g. CEO, CFO, Director, 10% owner
    is_director INTEGER DEFAULT 0,
    is_officer INTEGER DEFAULT 0,
    is_ten_percent_owner INTEGER DEFAULT 0,
    transaction_date TEXT,             -- YYYY-MM-DD
    transaction_code TEXT,             -- P (open market buy), S (open market sale), A (award), etc.
    is_buy INTEGER,                    -- 1 for open-market purchase, 0 for sale, NULL for other
    shares NUMERIC,                    -- signed: positive=acquired, negative=disposed
    price_local NUMERIC,
    value_local NUMERIC,
    currency TEXT,
    price_usd NUMERIC,
    value_usd NUMERIC,
    fetched_at ${ts},
    UNIQUE(source, source_id)
  )`);
  await query(`CREATE INDEX IF NOT EXISTS idx_insider_tx_ticker ON insider_transactions(issuer_ticker)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_insider_tx_date   ON insider_transactions(transaction_date)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_insider_tx_buy    ON insider_transactions(is_buy)`);

  await query(`CREATE INDEX IF NOT EXISTS idx_deals_event_type  ON deals(event_type)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_deals_tier        ON deals(data_source_tier)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_deals_filing_date ON deals(filing_date)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_deals_completed   ON deals(completed_date)`);
  // Search speed: screener 'q' filter does substring on these text columns.
  // Plain btree doesn't help LIKE '%foo%' — Postgres needs pg_trgm+GIN, SQLite
  // builds an in-memory scan. We add basic btree indexes which at least help
  // equality lookups, and (on PG) a trigram index for ILIKE.
  await query(`CREATE INDEX IF NOT EXISTS idx_deals_primary_ticker ON deals(primary_ticker)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_deals_target_ticker  ON deals(target_ticker)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_deals_spinco_ticker  ON deals(spinco_ticker)`);
  if (USE_PG) {
    try { await query(`CREATE EXTENSION IF NOT EXISTS pg_trgm`); } catch (_) {}
    try { await query(`CREATE INDEX IF NOT EXISTS idx_deals_headline_trgm ON deals USING gin (headline gin_trgm_ops)`); } catch (_) {}
    try { await query(`CREATE INDEX IF NOT EXISTS idx_deals_target_name_trgm ON deals USING gin (target_name gin_trgm_ops)`); } catch (_) {}
  }

  // ---- News items: attached to a deal, never create a deal on their own ----
  // Sourced from the legacy news firehose + Gemini classifier. Each item is
  // linked to a deal via ticker match or fuzzy company-name match.
  await query(`CREATE TABLE IF NOT EXISTS news_items (
    id ${pkSerial},
    deal_id INTEGER,
    raw_item_id INTEGER,               -- Back-reference to raw_items.id
    source TEXT,                       -- google_news | rns | etc.
    url TEXT,
    headline TEXT NOT NULL,
    summary TEXT,
    published_at TEXT,
    matched_ticker TEXT,               -- The ticker/name that caused the match
    match_kind TEXT,                   -- ticker | issuer_name | fuzzy
    fetched_at ${ts}
  )`);
  await query(`CREATE INDEX IF NOT EXISTS idx_news_items_deal ON news_items(deal_id)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_news_items_published ON news_items(published_at)`);

  // Ensure unique constraint on raw + deal so we don't double-link
  try {
    await query(`CREATE UNIQUE INDEX IF NOT EXISTS uq_news_items_raw_deal
                 ON news_items(raw_item_id, deal_id)`);
  } catch (e) { /* ignore */ }
}

// Portable JSON get/set — SQLite stores JSON as TEXT
function parseJson(val) {
  if (val == null) return null;
  if (typeof val === 'object') return val;
  try { return JSON.parse(val); } catch { return null; }
}
function serializeJson(val) {
  if (val == null) return null;
  if (USE_PG) return val;
  return typeof val === 'string' ? val : JSON.stringify(val);
}

module.exports = { query, migrate, parseJson, serializeJson, USE_PG };
