# Data Limitations & Coverage Caveats

This document catalogues what the tracker does and does **not** cover, along
with known data-quality issues. Use it to calibrate how much trust to put on
any individual deal / metric.

Last updated: 2026-04-19

---

## 1. Geographic coverage

### Well-covered
- **United States** — SEC EDGAR pipeline ingests 10‑12B/A, 8‑K (item 1.01 / 2.01 / 8.01), S‑1, 424B4, DEFM14A, PREM14A, S‑4, DEFS14A in near‑real‑time.
- **United Kingdom** — LSE RNS feed (regulatory news) ingests press releases from Main Market and AIM listings.
- **Sweden, Denmark, Finland, Iceland, Norway** — Nasdaq Nordic press-release page ingested; corporate actions & listings.
- **Netherlands, Belgium, France (Euronext Amsterdam / Brussels / Paris)** — **No regulatory feed**. Deals land via news mentions only; confidence is low. Workarounds: the reconciler can auto-detect completion by probing Yahoo for first-trade-date, but filings/record dates are not captured.
- **Germany, Switzerland, Italy, Spain** — **No regulatory feed**. Same limitation as Euronext.

### Not covered
- Asia (Hong Kong, Singapore, Japan, Korea) — out of scope; not a user priority.
- Emerging markets — out of scope.
- OTC pink sheets — excluded intentionally.

---

## 2. Event-type coverage

| Event | Detection method | Completion tracked? | Common gaps |
|---|---|---|---|
| **US spin-offs** | 10‑12B/A + 8‑K item 2.01 | Yes (Yahoo first-trade probe) | record_date rarely populated |
| **UK / Nordic demergers** | LSE RNS + Nasdaq Nordic | Partial — reconciler uses name-probe fallback | Pre-listing ticker often unknown |
| **Euronext / Frankfurt spins** | News mentions + reconciler probe | Partial | Manual ticker/region fixes sometimes required (see Magnum, Coffee Stain) |
| **US merger arb** | DEFM14A / PREM14A / S‑4 | **No** — merger completion is not auto-detected | See §3 |
| **Non-US merger arb** | LSE RNS / news | Partial | No structured offer-price extraction |
| **IPOs (US)** | SEC S‑1 → 424B4; stockanalysis.com aggregator | Yes (via aggregator current price) | SPAC shells flood the feed (see §4) |
| **IPOs (ex-US)** | Nasdaq Nordic / LSE RNS / news | Partial | IPO price rarely captured |
| **Tender offers** | Basic detection only | No | Low-confidence bucket |
| **SPACs** | Name regex classifier | N/A | Hidden by default (see §4) |

---

## 3. Merger-arb data gaps

The merger-arb bucket has the weakest structured data. Current fill rates:

| Field | Fill rate | Why |
|---|---|---|
| `announce_date` | 82 % | Extracted from 8‑K item 1.01 and DEFA14A/PREM14A cover pages |
| `consideration` / `consideration_type` | 43 % | Regex-based; cash-only deals extract cleanly, stock & mixed often miss |
| `offer_price` | 40 % | Only all-cash deals extract reliably. Stock deals need acquirer reference price |
| `acquirer_name` | low | New naive regex extractor added; many proxies don't use the standard phrasing |
| `expected_close_date` | low | Extracted from "expected to close in Q_X 20YY" or named-date language; filings that say only "subject to customary closing conditions" yield nothing |
| `deal_value_usd` | low | Extracted from "transaction valued at $X billion"; many filings don't restate the headline EV |
| `spread_to_deal_pct` | 0 % at rest | Computed on demand only when offer_price AND current_price both populated |
| Merger **completion** | **Not tracked** | Would require quote-disappearance tracking or post-merger 8‑K scanning — not yet implemented |

**Backfill available**: `POST /api/admin/backfill-merger-terms` re-scans DEFM14A proxies to populate offer_price / expected_close_date / deal_value_usd / acquirer_name with the updated extractor.

---

## 4. SPAC shells in the IPO feed

stockanalysis.com's `/ipos/` page lists **every** new US listing including SPAC blank-check entities. Prior to filtering, roughly 22 of ~305 completed-IPO rows were SPAC shells with no Yahoo price (symbols like `APGE`, `PONO`, `KENS`, `SUMA`, `QDRO`, `MYWD`, `ACPU`, `BLUU`), which surfaced as "invalid ticker" rows.

**Current handling**:
- Classifier tags SPAC issuers by name regex (`/acquisition (corp|company|limited)/i`, `/capital acquisition/i`, `/blank[-\s]?check/i`, `/\bSPAC\b/`) at ingest and sets `deal_type='spac'` + `is_spac=1`.
- Default screener query hides `is_spac=1`. Toggle **"Show SPACs"** in the filter bar to include them.
- One-shot backfill admin endpoint: `POST /api/admin/backfill-spacs` re-classifies existing rows.

**Residual risk**: regex may miss SPACs named without "Acquisition" in the issuer name (e.g. "Churchill VII"). These will still show as regular IPOs. False-positive rate should be near zero.

---

## 5. Pricing & market data

- **Yahoo Finance** is the primary price source via `yahoo-finance2`. Known gaps:
  - SPAC warrants/units (tickers ending `U` / `W`) often unavailable.
  - Day-1 listings sometimes take 24–48 h to populate; first-trade date detection tolerates this.
  - Nordic / Euronext symbols work only with Yahoo suffix (`.ST`, `.CO`, `.AS`, `.HE`, `.OL`); we auto-generate these in `tickers.js`.
- **FX conversion to USD** happens at market-data refresh time; historical FX is not back-applied so older EUR/GBP/SEK prices may be slightly off if the pair moved significantly.
- **No intraday prices** — daily close only. Announce-day price calibration uses the prior day's close ("unaffected price").

---

## 6. TradingView charts

- Embedded chart uses the mini-symbol-overview widget (`s.tradingview.com/embed-widget/mini-symbol-overview/`).
- **Known issue**: some Euronext symbols (e.g. `MICC` for Magnum) return *"Invalid symbol"* inside the widget even though the same prefix (`EURONEXT:MICC`) works on the TradingView website. This is a widget-side limitation; the chip link beneath the chart opens TradingView directly and works every time.
- For spin-offs, both parent and spinco charts render side-by-side (when both tickers are resolved).
- Widget is lazy-loaded and sandboxed (`sandbox="allow-scripts allow-same-origin allow-popups"`).

---

## 7. Insider & incentive data

- **US only** — sourced from SEC Form 4 filings via EDGAR.
- UK SDR (PDMR) / Euronext insider disclosures not ingested.
- Rollups: 6-month lookback, aggregated by ticker. Cluster-buying flag triggers at ≥ 3 distinct insiders buying within 6 months.
- Retention / rollover / sponsor-promote metrics are LLM-extracted from filings; subject to parsing error.

---

## 8. Ingest cadence

- SEC EDGAR: every 10 minutes via cron.
- LSE RNS, Nasdaq Nordic, stockanalysis.com: every 60 minutes.
- Market-data refresh (prices, market cap): every 15 minutes.
- Insider-transactions refresh: every 6 hours.
- Reconciler (spin-off completion detection, country fixes): daily.

A deal announced right before the cron boundary can be up to 60 minutes late.

---

## 9. Unified timeline

The per-deal timeline merges:
- Primary filing (green — official)
- Secondary / related filings from the same issuer (green — official)
- Linked news items (blue — news)
- Key dates: announce, ex-date, first-trade, expected-close, completed (yellow if upcoming, green if past)

**Every event now carries a click-through URL**: filings link to SEC / LSE RNS; first-trade / completed events link to Yahoo history; upcoming events link to the primary filing. Sort order is newest-first.

---

## 10. Reconciler safety

The nightly reconciler is conservative by design:
- **Spin completion** requires ≥ 3 real Yahoo bars (prevents false positives on quote-system glitches).
- **Country / region fixes** require shortName token overlap (won't rewrite a ticker's country unless the issuer name matches the exchange's listing records).
- When in doubt, the reconciler logs a skip rather than making a wrong update.

---

## 11. What's NOT implemented

- User authentication / per-user watchlists / alerts.
- Mobile-optimised UI (desktop-first; usable on tablet).
- Historical event archive beyond 180 days for IPOs.
- Post-merger performance of the combined entity.
- Short-interest / borrow data.
- Options implied probability of completion for merger arb.
- Fairness-opinion extraction from DEFM14As.
- Regulatory approval progress tracking (HSR, antitrust, CFIUS).
- Withdrawn-deal archival (deals that fall out of the pipeline are simply no longer refreshed).

---

## Quick reference: data source tiers

| Tier | Meaning | Confidence |
|---|---|---|
| `official` | Regulator filing (SEC / LSE RNS / Nasdaq Nordic press release) | 1.0 |
| `aggregator` | Third-party aggregator (stockanalysis.com) | 0.7 |
| `news` | News article only — awaiting filing corroboration | 0.3 |

Use the **Source filter** in the screener to restrict to Official-only if you need the strictest dataset.
