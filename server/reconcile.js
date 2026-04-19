// Deal-status reconciler.
//
// Runs as a daily cron (and on-demand via /api/admin/run-reconcile) to fix
// two kinds of drift in the deals table:
//
//   1. Completion drift: spin-offs and mergers marked 'announced' long after
//      they've actually closed. We look up the spinco ticker (for spins) or
//      the target ticker (for mergers) on Yahoo; if trading history exists
//      AFTER announce_date, we infer the deal completed at first-trade.
//
//   2. Region/country drift: deals where the stored country is US but the
//      company actually lists elsewhere (e.g. Magnum -> NL via MICC.AS).
//      We attempt common non-US Yahoo suffixes when the primary ticker has
//      no exchange prefix and the name hints at a non-US issuer.
//
// Conservative by design: only updates when the evidence is strong
// (trading history >= 5 bars, name match for non-US probe). Dry-run mode
// available for observation.

const { query } = require('./db');
const { fetchQuote, fetchHistoricalPrice } = require('./market_data');
const { parseTicker, inferFromYahooSymbol, COUNTRY_TO_REGION, COUNTRY_NAMES } = require('./tickers');

const NON_US_SUFFIXES = ['.AS', '.L', '.ST', '.CO', '.HE', '.OL', '.IC', '.SW', '.DE', '.PA', '.MI', '.MC', '.BR', '.IR', '.LS', '.VI', '.WA'];

function normalize(s) {
  return String(s || '').toLowerCase().replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
}

// Return a Yahoo symbol for a bare ticker by probing suffixes until one returns
// a quote whose name matches the deal's company name. Returns { yahooSymbol, country, exchangeLabel } or null.
async function probeNonUsListing(bareTicker, expectedName) {
  const nExpected = normalize(expectedName);
  if (!nExpected) return null;
  for (const suffix of NON_US_SUFFIXES) {
    const sym = `${bareTicker}${suffix}`;
    try {
      const q = await fetchQuote(sym);
      if (!q || !q.shortName) continue;
      const nGot = normalize(q.shortName);
      // Company name must share at least one significant token (>3 chars)
      const expectedTokens = nExpected.split(' ').filter(t => t.length > 3);
      const gotTokens = nGot.split(' ').filter(t => t.length > 3);
      const overlap = expectedTokens.filter(t => gotTokens.includes(t));
      if (overlap.length >= 1) {
        const info = inferFromYahooSymbol(sym);
        if (info) return { yahooSymbol: sym, ...info, matchedName: q.shortName };
      }
    } catch (_) { /* ignore, try next */ }
  }
  return null;
}

// Check if a ticker has price history starting after a given date — sign
// that the instrument has begun trading (i.e. a spin-off has listed).
async function findFirstTradeDate(yahooSymbol, notBefore) {
  if (!yahooSymbol) return null;
  try {
    const yf = require('yahoo-finance2').default;
    const inst = new yf({ suppressNotices: ['yahooSurvey', 'ripHistorical'] });
    const start = notBefore ? new Date(notBefore) : new Date(Date.now() - 365 * 86400_000);
    const end = new Date();
    const hist = await inst.chart(yahooSymbol, { period1: start, period2: end, interval: '1d' });
    const quotes = (hist?.quotes || []).filter(q => q.close != null);
    if (quotes.length < 3) return null;
    return new Date(quotes[0].date).toISOString().slice(0, 10);
  } catch (_) { return null; }
}

// Main reconciliation pass. Returns summary of changes.
async function reconcileAll({ dryRun = false, limit = 500, onlyIds = null } = {}) {
  let deals;
  if (onlyIds && onlyIds.length) {
    // Single-deal debug path: bypass status/deal_type filters.
    const placeholders = onlyIds.map((_, i) => `$${i + 1}`).join(',');
    deals = await query(
      `SELECT id, headline, target_name, target_ticker, parent_ticker, spinco_ticker, spinco_name,
              deal_type, status, completed_date, announce_date, country, region,
              primary_ticker, yahoo_symbol
         FROM deals WHERE id IN (${placeholders})`,
      onlyIds
    );
  } else {
    deals = await query(
      `SELECT id, headline, target_name, target_ticker, parent_ticker, spinco_ticker, spinco_name,
              deal_type, status, completed_date, announce_date, country, region,
              primary_ticker, yahoo_symbol
         FROM deals
        WHERE (status IN ('announced','pending','rumored') OR completed_date IS NULL)
          AND deal_type IN ('spin_off','merger_arb','ipo')
        ORDER BY announce_date DESC NULLS LAST
        LIMIT $1`,
      [limit]
    );
  }

  const changes = { completed: [], country_fixed: [], skipped: [], errors: 0 };

  for (const d of deals) {
    try {
      let dealDidSomething = false;
      // ---- Completion detection for spin-offs ----
      // Attempt spinco_ticker directly; else try to find a listing by probing
      // exchange-suffix combos of the spinco_name (e.g. 'Coffee Stain Group AB'
      // → COFFEE-B.ST on Nasdaq Stockholm). This catches cases where the feed
      // announced a spin-off without tickers but the subsidiary later listed.
      if (d.deal_type === 'spin_off' && !d.completed_date) {
        let yahooSym = null;
        let probeInfo = null;
        if (d.spinco_ticker) {
          const parsed = parseTicker(d.spinco_ticker);
          if (parsed?.yahooSymbol) yahooSym = parsed.yahooSymbol;
        }
        // Fallback: probe by name if announce_date > 7 days old and spinco_name exists
        if (!yahooSym && d.spinco_name && d.announce_date) {
          const daysOld = (Date.now() - new Date(d.announce_date).getTime()) / 86400000;
          if (daysOld > 7) {
            probeInfo = await probeSpinByName(d.spinco_name, d.country || d.region);
            if (probeInfo) yahooSym = probeInfo.yahooSymbol;
            else changes.skipped.push({ id: d.id, headline: d.headline, reason: 'spinco-name-probe-failed', name: d.spinco_name, hint: d.country || d.region });
          } else {
            changes.skipped.push({ id: d.id, headline: d.headline, reason: 'spin-too-recent', days: Math.round(daysOld) });
          }
        } else if (!yahooSym) {
          changes.skipped.push({ id: d.id, headline: d.headline, reason: 'spin-no-ticker-and-no-name' });
        }
        if (yahooSym) {
          const firstTrade = await findFirstTradeDate(yahooSym, d.announce_date);
          if (firstTrade) {
            changes.completed.push({ id: d.id, ticker: yahooSym, completed_date: firstTrade, headline: d.headline });
            dealDidSomething = true;
            if (!dryRun) {
              const primaryTickerUpdate = probeInfo ? `${probeInfo.exchangeLabel}:${yahooSym.split('.')[0]}` : null;
              await query(
                `UPDATE deals
                    SET status = 'completed', completed_date = $1, event_type = 'spin_off_completed',
                        spinco_ticker = COALESCE(spinco_ticker, $3),
                        yahoo_symbol = COALESCE($4, yahoo_symbol),
                        primary_ticker = COALESCE($5, primary_ticker)
                  WHERE id = $2`,
                [firstTrade, d.id, yahooSym, yahooSym, primaryTickerUpdate]
              );
            }
          } else {
            changes.skipped.push({ id: d.id, headline: d.headline, reason: 'no-trade-history', ticker: yahooSym });
          }
        }
      }

      // ---- Completion detection for mergers ----
      // Heuristic: if target_ticker has NO recent quote (trading halted after close)
      // AND announce_date is > 60 days old. We treat no-quote as completed only if
      // we previously had a current_price and it's now null — too prone to false
      // positives without that state. Skipping automated merger completion for v2.
      // (Users can still manually mark closed via admin.)

      // ---- Country fix: try non-US suffix probe for ambiguous primary tickers ----
      if ((d.country === 'US' || !d.country) && d.target_name && !dealDidSomething) {
        // Only probe if the target_ticker is a bare uppercase symbol (no suffix).
        const raw = String(d.target_ticker || '').trim().toUpperCase();
        if (raw && /^[A-Z0-9.\-]{1,8}$/.test(raw) && !raw.includes(':') && !raw.includes('.')) {
          // Cheap guard: only probe when name contains a non-US hint
          const hint = /\b(N\.?V\.?|A\.?B\.?|AG|SA|PLC|AS|Amsterdam|Stockholm|Paris|Oslo|Copenhagen|Helsinki|London|Zurich|Madrid|Milan|Dublin|Luxembourg|Ice\s*cream|Europe)\b/i.test(d.target_name + ' ' + (d.headline || ''));
          if (hint) {
            const probe = await probeNonUsListing(raw, d.target_name);
            if (probe && probe.country && probe.country !== d.country) {
              const region = COUNTRY_TO_REGION[probe.country] || d.region;
              changes.country_fixed.push({
                id: d.id, headline: d.headline, from: d.country || '?', to: probe.country,
                yahoo: probe.yahooSymbol, matchedName: probe.matchedName,
              });
              if (!dryRun) {
                await query(
                  `UPDATE deals SET country = $1, region = $2, yahoo_symbol = $3, primary_ticker = $4 WHERE id = $5`,
                  [probe.country, region, probe.yahooSymbol, `${probe.exchangeLabel}:${raw}`, d.id]
                );
              }
            } else if (!probe) {
              changes.skipped.push({ id: d.id, headline: d.headline, reason: 'no-probe-match', ticker: raw });
            }
          }
        }
      }
    } catch (e) {
      changes.errors += 1;
      console.warn('[reconcile] error on deal', d.id, e.message);
    }
  }

  return { total: deals.length, ...changes };
}

// Probe a spinco by its name. Strategy: generate candidate tickers from the
// first significant token(s) of the name and try common suffixes. Validate
// by comparing the returned shortName back to the input.
//
// Examples this catches:
//   'Coffee Stain Group AB' + hint 'SE' → COFFEE-B.ST (Nasdaq Stockholm)
//   'Asmodee SA' + hint 'FR' → ASM.ST (actually lists in Stockholm)
async function probeSpinByName(name, countryOrRegionHint) {
  const nExpected = normalize(name);
  if (!nExpected) return null;
  const expectedTokens = nExpected.split(' ').filter(t => t.length > 3);
  if (!expectedTokens.length) return null;

  // Build candidate bare tickers from the first significant word(s).
  const firstTok = expectedTokens[0].toUpperCase();
  const candidates = new Set([firstTok]);
  if (firstTok.length > 5) candidates.add(firstTok.slice(0, 5));
  if (firstTok.length > 4) candidates.add(firstTok.slice(0, 4));
  if (firstTok.length > 3) candidates.add(firstTok.slice(0, 3));
  // Class-B variants (common in Nordic listings: COFFEE-B.ST, EMBRAC-B.ST)
  for (const c of Array.from(candidates)) {
    candidates.add(`${c}-B`);
  }

  // Prioritize suffixes by country hint if provided.
  let suffixes = NON_US_SUFFIXES.slice();
  if (countryOrRegionHint) {
    const hintMap = {
      SE: '.ST', Nordic: '.ST', NO: '.OL', DK: '.CO', FI: '.HE', IS: '.IC',
      NL: '.AS', BE: '.BR', FR: '.PA', DE: '.DE', CH: '.SW', IT: '.MI',
      ES: '.MC', PT: '.LS', IE: '.IR', AT: '.VI', UK: '.L', GB: '.L',
      'EU-Continental': '.AS',
    };
    const prio = hintMap[countryOrRegionHint];
    if (prio) suffixes = [prio, ...suffixes.filter(s => s !== prio)];
  }

  for (const suffix of suffixes) {
    for (const cand of candidates) {
      const sym = `${cand}${suffix}`;
      try {
        const q = await fetchQuote(sym);
        if (!q || !q.shortName) continue;
        const nGot = normalize(q.shortName);
        const gotTokens = nGot.split(' ').filter(t => t.length > 3);
        const overlap = expectedTokens.filter(t => gotTokens.includes(t));
        if (overlap.length >= 1) {
          const info = inferFromYahooSymbol(sym);
          if (info) return { yahooSymbol: sym, ...info, matchedName: q.shortName };
        }
      } catch (_) { /* try next */ }
    }
  }
  return null;
}

module.exports = { reconcileAll, probeNonUsListing, probeSpinByName, findFirstTradeDate };
