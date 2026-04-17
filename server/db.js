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
