# SpecialSits — Global Special Situations Tracker

Sources and classifies special situations investments worldwide:

- **Spin-offs & split-offs** (Form 10, 10-12B, RNS "demerger" disclosures)
- **Merger arb** (announced cash / stock / mixed deals — 8-K item 1.01, S-4, SC 13E3, RNS 2.7 announcements, EU takeover notices)
- **IPOs & direct listings** (S-1, F-1, prospectus approvals)
- **SPACs** (business combinations, redemption events, extensions)
- **Tender offers, Dutch auctions, share buybacks, rights offerings**
- **Liquidations, going-private, activist campaigns, share class collapses**

## Stack

Same as PromoWatch:

- Node / Express API
- Postgres on Railway (SQLite fallback for local dev)
- Gemini 2.0 Flash for classification + structured extraction
- Static HTML frontend (screener + deal detail + admin)
- Google Apps Script for the email intake pipeline

## Sourcing pipelines (all free)

| Source | Regions | What it catches |
|---|---|---|
| SEC EDGAR JSON feed | US | 8-K (1.01, 2.01), S-1, S-4, SC 13E3, Form 10 |
| LSE RNS via London Stock Exchange RSS | UK | Rule 2.7 offer announcements, scheme docs, IPO intentions |
| Nasdaq OMX Nordic disclosures RSS | Sweden, Denmark, Finland, Norway, Iceland | Deal disclosures, spin-off notices |
| Euronext company news RSS | Paris, Amsterdam, Brussels, Dublin, Lisbon, Oslo | M&A, IPO, tender offers |
| Deutsche Börse / XETRA news RSS | Germany | M&A, squeeze-outs, delistings |
| SIX Swiss Exchange RSS | Switzerland | Public tender offers, mergers |
| Business Wire / PR Newswire / GlobeNewswire RSS | Global | Press announcements |
| Email inbox (via Apps Script) | Global | Forward any newsletter or alert |

All feeds poll **once per day** by default (configurable in `server/ingest.js`). Raw items land in `raw_items`, are classified by Gemini, and promoted to `deals` when they match a special situation category.

## Running locally

```bash
cp .env.example .env     # fill in GEMINI_API_KEY, INGEST_TOKEN, ADMIN_PASSWORD
npm install
npm start                # API + UI on http://localhost:3000
npm run ingest           # manually trigger one ingestion cycle
```

## Deploy to Railway

1. Push this folder to a new GitHub repo.
2. In Railway, **New Project → Deploy from GitHub repo** → pick the repo.
3. Add a **Postgres** plugin. `DATABASE_URL` is injected automatically.
4. In the service's **Variables** tab, set `GEMINI_API_KEY`, `INGEST_TOKEN`, `ADMIN_PASSWORD`.
5. Add a **Cron** service pointing at your main service with command `node server/ingest.js` and schedule `0 6 * * *` (daily at 06:00 UTC).

## Email intake (optional)

Open `apps-script/Code.gs`, paste into a new Apps Script project, set the `API_BASE` and `INGEST_TOKEN` script properties, and schedule `processInbox` every 15 min on the Gmail account you forward alerts to.

## API endpoints

- `GET  /api/deals` — list (filters: `?type=&status=&region=&q=`)
- `GET  /api/deals/:id` — single deal with source docs
- `GET  /api/stats` — header KPIs
- `POST /api/ingest/email` — Apps Script intake (auth: `x-ingest-token`)
- `POST /api/ingest/run` — trigger feed poll (auth: `x-ingest-token`)
- `POST /api/admin/deals/:id` — edit deal (auth: `x-admin-password`)

## Data model

`raw_items` — every inbound item (filing, RSS, email). Holds source, URL, headline, body, fetched date, and classification status.

`deals` — promoted, classified special situations. Fields:

- `deal_type` — spin_off | merger_arb | ipo | spac | tender | buyback | rights | liquidation | going_private | activist | share_class | other
- `status` — rumored | announced | pending | closed | terminated
- `region` — US | UK | EU | Nordic | Switzerland | Global
- Tickers (acquirer / target / parent / spinco), deal value, consideration, key dates (announce, expected close, record, ex-date)
- AI summary, thesis, risks, current spread %
- Array of `source_ids` linking back to `raw_items`
