# Non-US Insider / PDMR Transaction Feed Scoping

**Objective:** Extend the existing SEC Form 4 pipeline to cover PDMR / managers' transaction disclosures in eight Western European markets.  
**Regulatory basis for EU/EEA markets:** EU Market Abuse Regulation (MAR) Article 19, with each national competent authority (NCA) acting as receiver and custodian.  
**Date of research:** April 2025

---

## Table of Contents
1. [United Kingdom](#1-united-kingdom)
2. [Sweden](#2-sweden)
3. [Denmark](#3-denmark)
4. [Finland](#4-finland)
5. [Norway](#5-norway)
6. [Netherlands](#6-netherlands)
7. [Germany](#7-germany)
8. [Switzerland](#8-switzerland)
9. [Recommended Priority Order](#9-recommended-priority-order)

---

## 1. United Kingdom

### Regulator & Regulation
Financial Conduct Authority (FCA). Regulation: **UK MAR Article 19** (retained EU law) + **DTR 3** (Disclosure Guidance and Transparency Rules). PDMR notifications are filed with the FCA and stored in the **National Storage Mechanism (NSM)** at [data.fca.org.uk](https://data.fca.org.uk/#/nsm/nationalstoragemechanism).

### Public Feed / Database URL
- **NSM search portal:** `https://data.fca.org.uk/#/nsm/nationalstoragemechanism`
- **No RSS/Atom or JSON API** for consumers; results can be **exported to CSV** from the search UI.
- As of **PS24/19 (December 2024)**, the FCA is standardising PIP submission via a common XML schema and API (between PIPs and FCA internally). A **public bulk-download / consumer-facing API** was requested by respondents but has *not yet been committed to*. New rules take effect **3 November 2025**.
- PDMR notifications arrive via Primary Information Providers (PIPs — e.g., RNS/LSEG, Regulatory News Service); each PIP also publishes a newsfeed, some of which carry RSS.

### Typical Latency
Regulation requires notification within **T+3 business days** of the transaction. PIPs publish near real-time upon receipt. NSM ingests within minutes of PIP submission.

### Sample Entry Format / Fields
PDMR notification form captures:
| Field | Note |
|---|---|
| Insider name / legal entity | Person or closely-associated entity |
| Position / status within issuer | e.g., CEO, CFO, Non-exec Director |
| Issuer name + LEI | LEI not always present pre-Nov 2025 |
| Initial / amendment flag | |
| Instrument description + type | Share, debt, derivative, emission allowance |
| Instrument identification code (ISIN) | |
| Nature of transaction | Purchase, sale, pledge, etc. |
| Price(s) and volume(s) | Up to 20 price/size lines per transaction |
| Weighted average price (auto-calc) | |
| Currency | |
| Transaction date | UTC |
| Place of transaction | MIC code or "outside trading venue" |

Ticker is **not** a standard field; must be mapped from ISIN. Full document is a structured HTML page (not a machine-readable XML payload to consumers).

### Rate Limits / API Key / Terms of Use
Free public access. No API key required for manual UI. No stated rate limits, but automated bulk access is not explicitly supported today. PS24/19 signals a future bulk-download or API; timeline TBD post-November 2025 implementation.

### Coverage
All issuers with transferable securities admitted to trading on **UK regulated markets** (LSE Main Market, AIM is *not* a regulated market; AIM-listed company PDMRs file with the FCA directly but are sometimes published via RNS). Coverage is comprehensive for Main Market; AIM filings exist but routing differs.

### Difficulty Score
**3 / 5**  
No dedicated machine-readable feed today. Workable approach: poll PIP RSS feeds (RNS/LSEG publishes an RSS filtered by headline category "Director/PDMR Shareholding") and parse HTML notifications. Full-coverage ingest requires either monitoring multiple PIPs or waiting for the post-Nov 2025 NSM improvements. Fields are well-structured within the HTML document.

---

## 2. Sweden

### Regulator & Regulation
Finansinspektionen (FI). Regulation: **EU MAR Article 19** (directly applicable); Swedish Securities Market Act supplements. Register: **Insynsregistret (PDMR Transactions Register)**.

### Public Feed / Database URL
- **Register search:** `https://www.fi.se/en/our-registers/pdmr-transactions/`
- **No API, no RSS.** The register supports:
  - Web search by issuer, PDMR name, transaction date, or publication date.
  - **Excel export** of filtered results (effectively an `.xlsx` download, convertible to CSV).
- A Java scraping library exists: [github.com/w3stling/insynsregistret](https://github.com/w3stling/insynsregistret) — confirms the register is web-only with no official API.
- FI explicitly warns it may rate-limit or suspend heavy users.

### Typical Latency
Transactions are published **automatically upon receipt**, with **no review delay**. Notifications submitted via e-ID are published **immediately**. Regulatory deadline: T+3 business days. In practice, many filings appear T+0 to T+1.

### Sample Entry Format / Fields
| Field | Available |
|---|---|
| Issuer name | ✓ |
| ISIN | ✓ |
| PDMR name | ✓ |
| Position / title | ✓ |
| Transaction type (buy/sell/gift etc.) | ✓ |
| Security type | ✓ |
| Quantity | ✓ |
| Price | ✓ |
| Currency | ✓ (implied) |
| Linked to share option programme | ✓ |
| Publication date | ✓ |
| Transaction date | ✓ |

Ticker not standardised; derives from ISIN.

### Rate Limits / Terms of Use
Free public access. FI may limit search frequency or suspend users who impair availability for others. No API key. No explicit commercial-use restriction found, but automated bulk polling is in a grey area.

### Coverage
All issuers on Swedish regulated markets and MTFs: **Nasdaq Stockholm** (Large Cap, Mid Cap, Small Cap), **NGM**, and other trading venues. First North (Nasdaq SME MTF) is an MTF so is included. Broad SME coverage.

### Difficulty Score
**2 / 5**  
Good structured data available; well-known data fields; Excel export is straightforward to automate (HTTP request + xlsx parse). No API but the extraction pattern is well-documented by open-source libraries. Rate-limiting note warrants polling throttle. Closest analog to Form 4 in this group.

---

## 3. Denmark

### Regulator & Regulation
Finanstilsynet (the Danish FSA / DFSA). Regulation: **EU MAR Article 19**. Denmark's OAM (Officially Appointed Mechanism) is operated by Finanstilsynet. Threshold raised to **EUR 50,000** per calendar year (above MAR's EUR 20,000 floor).

### Public Feed / Database URL
- **OAM portal:** `https://oam.finanstilsynet.dk/en/search-oam` — **requires login** to access.
- A PDF guide to OAM search exists ([cdn.finanstilsynet.dk guide](https://cdn.finanstilsynet.dk/finanstilsynet/Media/638562881556160794/Search_OAM_EN.pdf)) confirming free-text and metadata search for announcements, but public (unauthenticated) access is **not confirmed**.
- Notifications are submitted and stored in the OAM; the system is the same platform used for major shareholding and short-selling notifications. Some third-party aggregators (e.g., [insidertransactionterminal.com/insider-transactions-denmark.html](https://insidertransactionterminal.com/insider-transactions-denmark.html)) claim to monitor the register daily, suggesting a public read path exists but may not be well-documented.
- **No CSV, XML, RSS, or API confirmed** for unauthenticated bulk access.

### Typical Latency
**T+3 business days** per MAR Article 19(1). OAM publishing appears near-real-time once submitted.

### Sample Entry Format / Fields
Per DFSA data-privacy disclosures, the OAM collects:
- Name + position of PDMR or closely associated person
- Transaction details: nature, financial instrument, amount/value, date
- CPR (national ID) stored but **not published**

Published fields likely match the standard MAR implementing regulation form (EU 2016/523): issuer, PDMR name, position, transaction type, instrument type, ISIN, volume, price, currency, date, venue.

### Rate Limits / Terms of Use
Authentication-gated portal. No stated API or public bulk-export terms.

### Coverage
All issuers on Danish regulated markets: **Nasdaq Copenhagen** (primarily large/mid-caps), plus MTFs.

### Difficulty Score
**4 / 5**  
Login-gated OAM portal with no confirmed public API or export. Ingest would require either account-based web automation or reliance on third-party aggregators. Fields likely match MAR standard but extraction path is unclear without direct portal access.

---

## 4. Finland

### Regulator & Regulation
Finanssivalvonta (Finnish FSA / FIN-FSA). Regulation: **EU MAR Article 19**. Notifications submitted via FIN-FSA's electronic services portal; public disclosure is made via **Nasdaq Helsinki's message storage facility** using message category "Managers' transactions".

### Public Feed / Database URL
- **Nasdaq Helsinki disclosure feed:** Disclosed via Nasdaq's OAM system. Public announcements are stored at Nasdaq Helsinki and published on the **Nasdaq Baltic/Nordic Newsroom** or issuer websites.
- FIN-FSA electronic submission portal: `https://asiointi.finanssivalvonta.fi/` — **login required** to submit; no public read-access documented.
- **No dedicated public CSV, XML, RSS, or JSON API** identified at the regulator level.
- Nasdaq Helsinki's OAM may have a searchable public portal, similar to other Nordic Nasdaq OAMs.
- FIN-FSA registers page ([finanssivalvonta.fi/en/registers/](https://www.finanssivalvonta.fi/en/registers/)) lists supervised entities, not PDMR transactions.

### Typical Latency
**T+3 business days** per MAR. Issuers must also maintain the notification on their website for **5 years**.

### Sample Entry Format / Fields
Based on the standard EU MAR implementing regulation form (EU 2016/523) and Finland-specific templates:
- PDMR name, position, issuer name + LEI/ISIN
- Transaction type, instrument type, ISIN
- Volume, price, currency, date, venue
- Close-associate relationship flag (if applicable)

### Rate Limits / Terms of Use
Portal is authentication-gated for submissions; public read access terms not documented.

### Coverage
All issuers on **Nasdaq Helsinki** regulated market (Helsinki Stock Exchange) and MTFs. Includes First North Finland (SME growth market).

### Difficulty Score
**4 / 5**  
Public disclosure is routed through Nasdaq Helsinki's OAM (not a standalone FIN-FSA public register). Ingest path requires interfacing with Nasdaq's Nordic disclosure system or individual issuer websites. No confirmed bulk feed. The [Finnish ESAP OAM portal](https://oam.fi.ee/) referenced in ESMA's register appears to be Estonian, not Finnish — Finland's OAM operator is listed as "Stock Exchange" (Nasdaq Helsinki) in the ESMA register.

---

## 5. Norway

### Regulator & Regulation
Finanstilsynet (Norway). Regulation: MAR has applied in Norway since **1 March 2021** (EEA/EFTA adoption). PDMRs ("primary insiders") report via **Altinn** (Norway's government digital services portal). Public disclosure is published on **Oslo Børs / Euronext Oslo's NewsWeb** system. As of **1 April 2025**, supervision transferred from Oslo Børs to Finanstilsynet, but **NewsWeb remains the OAM** for public storage.

### Public Feed / Database URL
- **NewsWeb:** `https://newsweb.oslobors.no` — public, searchable, real-time. Filterable by message category ("Mandatory Notification of Trade Primary Insiders") and market.
- **Euronext Live primary insiders list:** `https://live.euronext.com/en/markets/oslo/equities/primary-insiders` — shows PDMR roster (ticker, company, name, title, position). Not transaction-level data.
- **No RSS, JSON API, or CSV export confirmed** from NewsWeb. Each notification is an HTML/PDF document.
- Altinn receipt is for submission only; Finanstilsynet does not maintain a separate public transaction register.

### Typical Latency
Per Norwegian Securities Trading Act implementing MAR: **T+3 business days**. In practice, issuers publish promptly after receiving PDMR notification; many appear **T+0 to T+1** on NewsWeb.

### Sample Entry Format / Fields
Standard MAR notification form (text-based, published as HTML announcement on NewsWeb):
- Company name + ticker
- PDMR name, role (board member, CEO, CFO, etc.)
- Nature of transaction: purchase/sale, shares/bonds/derivatives
- Volume (shares), price per share, total value
- Currency (NOK)
- Transaction date

ISIN may or may not appear; ticker is typically included. No structured XML payload to consumers — each NewsWeb entry is a free-text press-release-style announcement.

### Rate Limits / Terms of Use
NewsWeb is publicly accessible; no API key. No documented rate limits, but no official API. Euronext offers paid XML/SFTP data products (e.g., for bonds) but no confirmed PDMR-specific structured feed without commercial arrangement.

### Coverage
**Oslo Børs (main regulated market), Euronext Expand, and Euronext Growth Oslo** (SME MTF). Broad coverage including energy, shipping, seafood blue-chips and SMEs.

### Difficulty Score
**3 / 5**  
NewsWeb has a reliable, consistent URL pattern and freely searchable category filter. Content is free-text HTML, not structured XML, so field extraction requires parsing. Ticker is usually present, which eases mapping. Similar effort to parsing RNS (UK PIP) notifications.

---

## 6. Netherlands

### Regulator & Regulation
Autoriteit Financiële Markten (AFM). Regulation: **EU MAR Article 19** + Dutch Financial Supervision Act (Wft) Article 5:48 (for executive/supervisory directors — satisfies MAR obligation when filed). AFM maintains the public **MAR 19 Managers' Transactions Register**.

### Public Feed / Database URL
- **Public register:** `https://www.afm.nl/en/sector/registers/meldingenregisters/transacties-leidinggevenden-mar19-`
- **CSV export:** ✓ Available directly from the register page (no login required).
- **XML export:** ✓ Available directly from the register page (no login required).
- **No RSS or API**; exports are triggered on demand from the web UI.
- The AFM also offers a **daily email update service** for changes across all registers.

### Typical Latency
**T+3 business days** per MAR. The register page states "Date last update: [daily]", indicating at least daily refresh.

### Sample Entry Format / Fields
From the register table and XML/CSV exports (inferred from UI):
| Field | Available |
|---|---|
| Transaction date | ✓ |
| Issuing institution (issuer name) | ✓ |
| Notifiable person (insider name or entity) | ✓ |
| Transaction type | ✓ (implied) |
| Instrument type, ISIN | ✓ (in full record) |
| Volume, price, currency | ✓ (in full record) |
| Position / title | ✓ |

Ticker mapping requires ISIN lookup. The register currently shows ~68 active results (reflecting relatively small Dutch-listed issuer universe).

### Rate Limits / Terms of Use
Free public access, no API key. No stated rate limits for the CSV/XML downloads. AFM registers are governed by general Dutch data-protection law; automated collection for financial research is customary.

### Coverage
All issuers on **Euronext Amsterdam** (regulated market) and MTFs. The AFM covers a focused large-to-mid-cap universe (AEX, AMX, AScX, Euronext Growth Amsterdam). SMEs on Euronext Growth Amsterdam also covered.

### Difficulty Score
**1 / 5**  
Closest to SEC EDGAR in terms of ease. Free CSV and XML downloads, no login required, standard MAR fields. The universe is small (~200–300 listed issuers) but data is immediately machine-ingestible. Only limitation: no push/streaming API — requires polling.

---

## 7. Germany

### Regulator & Regulation
Bundesanstalt für Finanzdienstleistungsaufsicht (BaFin). Regulation: **EU MAR Article 19** (directly applicable) + German Securities Trading Act (WpHG §26). BaFin receives notifications; issuers must **simultaneously publish** notifications via an approved dissemination system. Germany's **Unternehmensregister** (Company Register) stores capital market notifications including directors' dealings. As of **1 January 2026**, BaFin raised the reporting threshold to **EUR 50,000** per calendar year (from EUR 20,000), exercising the EU Listing Act discretion.

### Public Feed / Database URL
- **BaFin Directors' Dealings database:** `https://www.bafin.de/EN/PublikationenDaten/Datenbanken/DirektorsDeals/` — accessible via BaFin's website. Direct URL was blocked to automated access during testing; manual navigation confirms the database exists.
- **Unternehmensregister:** `https://www.unternehmensregister.de/en` — central German company data portal; contains capital market publications including directors' dealings. Searchable but primarily HTML-rendered; **no confirmed CSV/XML bulk export** for directors' dealings specifically.
- **No RSS, JSON API, or CSV export confirmed** for either BaFin or Unternehmensregister for this data class.
- German issuers also disseminate via **EQS Group** or other approved distribution systems, which may offer RSS.

### Typical Latency
WpHG §26 requires publication "without undue delay, no later than three business days" (T+3). Issuers must transmit to the Unternehmensregister immediately after publication.

### Sample Entry Format / Fields
Standard MAR 2016/523 form, published as an ad-hoc announcement:
- Issuer name + ISIN (typically)
- PDMR name, role (Vorstand / Aufsichtsrat member)
- Transaction type (purchase/sale)
- Instrument type, ISIN/WKN
- Volume, price, currency
- Transaction date + place

Ticker (WKN) usually included alongside ISIN.

### Rate Limits / Terms of Use
BaFin database: free public access; automated access blocked by robots.txt, requiring careful rate-limiting. Unternehmensregister: free read access for publications, no API key for basic queries.

### Coverage
All issuers registered in Germany with securities on a **regulated market or MTF** in the EU. This includes DAX/MDAX/SDAX/TecDAX constituents plus several hundred smaller issuers. Very large universe (~700+ listed equity issuers).

### Difficulty Score
**3 / 5**  
Data exists and is public, but the direct feed path is fragmented: BaFin's database has no confirmed export API; the Unternehmensregister contains the data in HTML. Best practical approach is monitoring the EQS or similar newswire RSS feeds filtered for the "Directors' Dealings" notification type. Parsing requires ISIN/WKN extraction from semi-structured HTML. The Listing Act threshold increase means slightly fewer filings from Jan 2026.

---

## 8. Switzerland

### Regulator & Regulation
SIX Exchange Regulation (SER) — a subsidiary of SIX Group, acting as the regulatory arm of SIX Swiss Exchange. **Not an EU/EEA member**, so does not apply MAR. Governed by: **Swiss Financial Market Infrastructure Act (FinMIA)**, SIX Listing Rules **Art. 56**, and the **Directive on the Disclosure of Management Transactions (DMT)**. Revised rules effective **1 February 2024** extended scope to related-party transactions.

### Public Feed / Database URL
- **Public register:** `https://www.ser-ag.com/en/resources/notifications-market-participants/management-transactions.html`  
  — Searchable UI with date range filter and issuer dropdown. Publicly accessible, no login.
- **No CSV, XML, RSS, or API confirmed** for bulk/programmatic access. The page renders data from a backend database but offers no explicit download button or documented API endpoint.
- SIX Group's **Exfeed** market data service delivers high-quality data feeds (including management transactions) but is a **commercial product** requiring a contract.
- Each published notification is an HTML page/entry; some are accompanied by PDFs.

### Typical Latency
Insider reports to issuer within **T+2 trading days** of transaction. Issuer reports to SER within **T+3 trading days** of receiving notification. Total typical latency: **T+3 to T+5 trading days** from transaction date. SER publishes "immediately" upon receipt.

### Sample Entry Format / Fields
Published fields per SIX Listing Rules Art. 56(5) (name and DOB **not** published):
| Field | Published |
|---|---|
| Issuer name | ✓ |
| Function of reporting person (board/executive, executive/non-executive) | ✓ |
| Transaction type (Purchase / Sale / Grant) | ✓ |
| Rights type (ordinary shares, options, other securities) | ✓ |
| Total amount of rights (quantity) | ✓ |
| Transaction value (CHF) | ✓ |
| ISIN | ✓ |
| Principal terms (if no ISIN, or for unlisted instruments) | ✓ |
| Ticker (SIX symbol, e.g., ROG, NESN) | ✓ |
| Transaction date | ✓ |

**Name of insider is not published** — only function/role. This limits usefulness relative to SEC Form 4.

### Rate Limits / Terms of Use
Free public web access to the search UI. SIX's privacy notice states data is used strictly for disclosed purposes and not forwarded to third parties; bulk commercial redistribution likely requires an Exfeed contract.

### Coverage
Only **primary-listed equity issuers on SIX Swiss Exchange**. Covers all SMI, SLI, and SPI constituents (~240 primary-listed equity issuers). Does **not** cover BX Swiss or foreign-primary-listed companies.

### Difficulty Score
**4 / 5**  
No machine-readable export from the public portal; requires structured web extraction from the SER search page. Additionally, **insider names are not published** (only function), which limits signal value for insider-trading research vs. other markets. Fields are otherwise clean and standardised. Exfeed API would reduce difficulty to 2 but carries licensing costs.

---

## 9. Recommended Priority Order

| Rank | Market | Difficulty | Coverage | Rationale |
|---|---|---|---|---|
| **1** | **Netherlands (AFM)** | 1 | Mid (Euronext AMS ~200 issuers) | Free CSV + XML downloads, no auth, MAR-standard fields, already machine-ingestible. Ideal proof-of-concept for EU MAR pipeline. |
| **2** | **Sweden (FI)** | 2 | High (Nasdaq Stockholm + First North) | Immediate-publish register, Excel export scriptable, large and liquid market, open-source extraction precedent. |
| **3** | **Norway (Oslo Børs / NewsWeb)** | 3 | High (Oslo Børs + Euronext Growth) | Ticker usually present in free-text filings, reliable NewsWeb category filter, high signal from energy/shipping blue-chips. Parsing effort comparable to RNS (UK). |

**Secondary tier (tackle after pipeline is validated):**

| Rank | Market | Difficulty | Notes |
|---|---|---|---|
| 4 | **Germany (BaFin/EQS)** | 3 | Large universe but fragmented distribution; EQS newswire RSS approach mitigates difficulty. |
| 5 | **United Kingdom (FCA NSM/PIPs)** | 3 | Important market but no single clean feed; PIP RSS patchwork. Nov 2025 NSM improvements may lower to difficulty 2. Revisit after regulatory change. |
| 6 | **Switzerland (SIX SER)** | 4 | Insider names not published — reduces research value. Exfeed API available commercially. |
| 7 | **Denmark (Finanstilsynet OAM)** | 4 | Login-gated portal; small universe (~150 issuers). |
| 8 | **Finland (Nasdaq Helsinki OAM)** | 4 | Disclosure routed through Nasdaq Helsinki; no standalone regulator feed confirmed. |

### Strategic Notes
- **Common EU MAR implementation** (markets 1–5, 7) means field schemas are largely identical (EU Implementing Regulation 2016/523 standard form), which enables a shared parser with country-specific adapters for delivery mechanism only.
- **Switzerland** is a standalone non-EU system with different field availability (no insider name published); treat as a separate adapter.
- **UK** will become significantly easier post-November 2025 once the NSM standardised API and bulk-download are live — worth scheduling a re-evaluation then.
- **ESAP** (European Single Access Point, launched 2024) is intended to eventually centralise MAR Article 19 data across EU NCAs; monitor for API availability as it matures.

---

*Sources: FCA PS24/19 (Dec 2024); Finansinspektionen PDMR register; AFM MAR 19 register; BaFin WpHG §26 / Listing Act Dec 2025 decree; SIX Listing Rules Art. 56 + DMT Guideline Feb 2024; Finanstilsynet Norway MAR page; Euronext Oslo NewsWeb; ESMA OAM register; DFSA (Denmark) OAM portal; Finanssivalvonta Finland.*
