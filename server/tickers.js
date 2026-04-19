// Ticker normalization + country mapping.
//
// Gemini emits tickers in mixed formats: "NYSE:ABC", "LSE:XYZ", "ABC", "ABC.L", etc.
// We normalize to two things:
//   - yahooSymbol:   the symbol to pass to yahoo-finance2 (e.g. "ABC", "XYZ.L", "ABC.ST")
//   - country:       ISO-2 country code (US, GB, SE, DK, NO, FI, IS, CH, DE, FR, NL, IT, ES, BE, IE, PL, AT, PT)
//   - exchange:      display label (e.g. "NYSE", "LSE", "Nasdaq Stockholm")
//
// Only listed markets we actually cover. Anything else → null, classified as 'Other'.

const EXCHANGES = {
  // -- United States --
  'NYSE':     { country: 'US', yahooSuffix: '',     label: 'NYSE'   },
  'NASDAQ':   { country: 'US', yahooSuffix: '',     label: 'Nasdaq' },
  'NYSEARCA': { country: 'US', yahooSuffix: '',     label: 'NYSE Arca' },
  'NYSEAMERICAN': { country: 'US', yahooSuffix: '', label: 'NYSE American' },
  'AMEX':     { country: 'US', yahooSuffix: '',     label: 'NYSE American' },
  'OTC':      { country: 'US', yahooSuffix: '',     label: 'OTC' },

  // -- United Kingdom --
  'LSE':      { country: 'GB', yahooSuffix: '.L',   label: 'LSE'   },
  'LON':      { country: 'GB', yahooSuffix: '.L',   label: 'LSE'   },
  'AIM':      { country: 'GB', yahooSuffix: '.L',   label: 'AIM'   },

  // -- Nordics --
  'STO':      { country: 'SE', yahooSuffix: '.ST',  label: 'Nasdaq Stockholm' },
  'OMX':      { country: 'SE', yahooSuffix: '.ST',  label: 'Nasdaq Stockholm' },
  'STOCKHOLM':{ country: 'SE', yahooSuffix: '.ST',  label: 'Nasdaq Stockholm' },
  'CPH':      { country: 'DK', yahooSuffix: '.CO',  label: 'Nasdaq Copenhagen' },
  'COPENHAGEN':{country: 'DK', yahooSuffix: '.CO',  label: 'Nasdaq Copenhagen' },
  'HEL':      { country: 'FI', yahooSuffix: '.HE',  label: 'Nasdaq Helsinki'  },
  'HELSINKI': { country: 'FI', yahooSuffix: '.HE',  label: 'Nasdaq Helsinki'  },
  'OSL':      { country: 'NO', yahooSuffix: '.OL',  label: 'Oslo B\u00f8rs'    },
  'OSLO':     { country: 'NO', yahooSuffix: '.OL',  label: 'Oslo B\u00f8rs'    },
  'ICE':      { country: 'IS', yahooSuffix: '.IC',  label: 'Nasdaq Iceland'   },

  // -- Switzerland --
  'SIX':      { country: 'CH', yahooSuffix: '.SW',  label: 'SIX'   },
  'SWX':      { country: 'CH', yahooSuffix: '.SW',  label: 'SIX'   },
  'VTX':      { country: 'CH', yahooSuffix: '.VX',  label: 'SIX'   },

  // -- Germany --
  'ETR':      { country: 'DE', yahooSuffix: '.DE',  label: 'XETRA' },
  'XETRA':    { country: 'DE', yahooSuffix: '.DE',  label: 'XETRA' },
  'FRA':      { country: 'DE', yahooSuffix: '.F',   label: 'Frankfurt' },
  'FWB':      { country: 'DE', yahooSuffix: '.F',   label: 'Frankfurt' },
  'GR':       { country: 'DE', yahooSuffix: '.DE',  label: 'XETRA' },

  // -- France --
  'PAR':      { country: 'FR', yahooSuffix: '.PA',  label: 'Euronext Paris' },
  'EPA':      { country: 'FR', yahooSuffix: '.PA',  label: 'Euronext Paris' },

  // -- Netherlands --
  'AMS':      { country: 'NL', yahooSuffix: '.AS',  label: 'Euronext Amsterdam' },
  'AEX':      { country: 'NL', yahooSuffix: '.AS',  label: 'Euronext Amsterdam' },

  // -- Italy --
  'BIT':      { country: 'IT', yahooSuffix: '.MI',  label: 'Borsa Italiana' },
  'MIL':      { country: 'IT', yahooSuffix: '.MI',  label: 'Borsa Italiana' },

  // -- Spain --
  'MCE':      { country: 'ES', yahooSuffix: '.MC',  label: 'BME Madrid' },
  'BME':      { country: 'ES', yahooSuffix: '.MC',  label: 'BME Madrid' },

  // -- Belgium --
  'BRU':      { country: 'BE', yahooSuffix: '.BR',  label: 'Euronext Brussels' },
  'EBR':      { country: 'BE', yahooSuffix: '.BR',  label: 'Euronext Brussels' },

  // -- Ireland --
  'ISE':      { country: 'IE', yahooSuffix: '.IR',  label: 'Euronext Dublin' },
  'DUB':      { country: 'IE', yahooSuffix: '.IR',  label: 'Euronext Dublin' },

  // -- Portugal --
  'ELI':      { country: 'PT', yahooSuffix: '.LS',  label: 'Euronext Lisbon' },

  // -- Austria --
  'VIE':      { country: 'AT', yahooSuffix: '.VI',  label: 'Wiener B\u00f6rse' },
  'WBO':      { country: 'AT', yahooSuffix: '.VI',  label: 'Wiener B\u00f6rse' },

  // -- Poland --
  'WSE':      { country: 'PL', yahooSuffix: '.WA',  label: 'GPW Warsaw' },
  'WAR':      { country: 'PL', yahooSuffix: '.WA',  label: 'GPW Warsaw' },
};

// Yahoo suffix → country reverse lookup (for symbols that already include a suffix).
const SUFFIX_TO_COUNTRY = {};
for (const [_, v] of Object.entries(EXCHANGES)) {
  if (v.yahooSuffix) SUFFIX_TO_COUNTRY[v.yahooSuffix] = { country: v.country, label: v.label };
}

const COUNTRY_NAMES = {
  US: 'United States', GB: 'United Kingdom', SE: 'Sweden', DK: 'Denmark',
  NO: 'Norway', FI: 'Finland', IS: 'Iceland', CH: 'Switzerland', DE: 'Germany',
  FR: 'France', NL: 'Netherlands', IT: 'Italy', ES: 'Spain', BE: 'Belgium',
  IE: 'Ireland', PT: 'Portugal', AT: 'Austria', PL: 'Poland',
};

// Granular region groupings stored on each deal row. The UI filter expands
// these hierarchically (e.g. selecting 'Europe' matches Nordic/UK/Switzerland/EU-Continental).
// See REGION_HIERARCHY below for the expansion logic.
const COUNTRY_TO_REGION = {
  US: 'US',
  CA: 'Canada',
  GB: 'UK',
  SE: 'Nordic', DK: 'Nordic', NO: 'Nordic', FI: 'Nordic', IS: 'Nordic',
  CH: 'Switzerland',
  DE: 'EU-Continental', FR: 'EU-Continental', NL: 'EU-Continental', IT: 'EU-Continental',
  ES: 'EU-Continental', BE: 'EU-Continental', IE: 'EU-Continental', PT: 'EU-Continental',
  AT: 'EU-Continental', PL: 'EU-Continental', LU: 'EU-Continental', CZ: 'EU-Continental',
  HU: 'EU-Continental', GR: 'EU-Continental',
};

// Nested region hierarchy used by the API filter.
// When a user selects 'Europe', we match any of [UK, Nordic, Switzerland, EU-Continental, Europe].
// When they select 'Nordic', we match only Nordic rows.
const REGION_HIERARCHY = {
  'US':             ['US'],
  'Canada':         ['Canada'],
  'Americas':       ['US', 'Canada'],
  'UK':             ['UK'],
  'Nordic':         ['Nordic'],
  'Switzerland':    ['Switzerland'],
  'EU-Continental': ['EU-Continental'],
  // "Europe" broadly includes UK + Nordic + Switzerland + EU-Continental,
  // plus any legacy rows tagged simply 'Europe'.
  'Europe':         ['UK', 'Nordic', 'Switzerland', 'EU-Continental', 'Europe'],
  'Global':         ['Global'],
};

// Flat list for the UI dropdown, ordered by relevance for special-sits desks.
const UI_REGIONS = ['US', 'Canada', 'Americas', 'UK', 'Nordic', 'EU-Continental', 'Switzerland', 'Europe', 'Global'];

// Parse a raw ticker string emitted by Gemini (or a human) into a normalized form.
// Returns { exchange, symbol, yahooSymbol, country, countryName, region, label } or null.
function parseTicker(raw) {
  if (!raw || typeof raw !== 'string') return null;
  let s = raw.trim().toUpperCase();
  if (!s || s === '?' || s === 'NULL' || s === 'N/A' || s === 'TBD') return null;

  // Form 1: "EXCHANGE:SYMBOL"
  let m = s.match(/^([A-Z][A-Z0-9]*):([A-Z0-9.\-]+)$/);
  if (m) {
    const ex = EXCHANGES[m[1]];
    if (ex) {
      const symbol = m[2].replace(/\./g, '-'); // Yahoo uses BRK-B for BRK.B
      const yahooSymbol = ex.yahooSuffix ? `${symbol}${ex.yahooSuffix}` : symbol;
      return {
        exchange: ex.label,
        symbol,
        yahooSymbol,
        country: ex.country,
        countryName: COUNTRY_NAMES[ex.country] || ex.country,
        region: COUNTRY_TO_REGION[ex.country] || 'Other',
        label: `${ex.label}:${symbol}`,
      };
    }
    // Unknown exchange prefix \u2014 fall through to bare-symbol handling
    s = m[2];
  }

  // Form 2: "SYMBOL.SUFFIX" (Yahoo style, e.g. "ABC.L")
  m = s.match(/^([A-Z0-9\-]+)(\.[A-Z]{1,3})$/);
  if (m) {
    const meta = SUFFIX_TO_COUNTRY[m[2]];
    if (meta) {
      return {
        exchange: meta.label,
        symbol: m[1],
        yahooSymbol: s,
        country: meta.country,
        countryName: COUNTRY_NAMES[meta.country] || meta.country,
        region: COUNTRY_TO_REGION[meta.country] || 'Other',
        label: `${meta.label}:${m[1]}`,
      };
    }
  }

  // Form 3: bare US-style symbol ("ABC", "BRK-B", "BRK.B")
  if (/^[A-Z][A-Z0-9]{0,5}(?:[.\-][A-Z])?$/.test(s)) {
    const symbol = s.replace(/\./g, '-');
    return {
      exchange: 'NYSE/Nasdaq',
      symbol,
      yahooSymbol: symbol,
      country: 'US',
      countryName: 'United States',
      region: 'US',
      label: symbol,
    };
  }

  return null;
}

// Pick the primary ticker for a deal.
// Rule: the investable security, i.e. what you'd actually buy/short.
//   merger_arb:       target (what you buy to collect the spread)
//   spin_off:         parent (pre-distribution) \u2014 spinco usually hasn't traded yet
//   going_private:    target (listed co being taken out)
//   tender:           target
//   activist:         target (the company being targeted)
//   ipo / spac:       the issuer/newco (acquirer_ticker if present, else target_ticker)
//   rights/buyback/share_class/liquidation: target/issuer (target_ticker)
//   other:            first non-null in order: target > acquirer > spinco > parent
function pickPrimaryTicker(deal) {
  const order = {
    merger_arb:    ['target_ticker', 'acquirer_ticker'],
    spin_off:      ['parent_ticker', 'spinco_ticker'],
    going_private: ['target_ticker'],
    tender:        ['target_ticker', 'acquirer_ticker'],
    activist:      ['target_ticker'],
    ipo:           ['target_ticker', 'acquirer_ticker'],
    spac:          ['acquirer_ticker', 'target_ticker'],
    rights:        ['target_ticker'],
    buyback:       ['target_ticker'],
    share_class:   ['target_ticker'],
    liquidation:   ['target_ticker'],
    other:         ['target_ticker', 'acquirer_ticker', 'spinco_ticker', 'parent_ticker'],
  };
  const fields = order[deal.deal_type] || order.other;
  for (const f of fields) {
    const t = deal[f];
    if (t) {
      const parsed = parseTicker(t);
      if (parsed) return parsed;
    }
  }
  return null;
}

// Bucket a market cap (USD) into a size tier.
function marketCapBucket(usd) {
  if (usd == null) return null;
  const n = Number(usd);
  if (n >= 200e9) return 'mega';      // >$200B
  if (n >= 10e9)  return 'large';     // $10B-$200B
  if (n >= 2e9)   return 'mid';       // $2B-$10B
  if (n >= 300e6) return 'small';     // $300M-$2B
  if (n >= 50e6)  return 'micro';     // $50M-$300M
  return 'nano';                       // <$50M
}

// Bucket deal value (USD) into a size tier.
function dealSizeBucket(usd) {
  if (usd == null) return null;
  const n = Number(usd);
  if (n >= 10e9) return 'mega';       // >$10B
  if (n >= 1e9)  return 'large';      // $1B-$10B
  if (n >= 100e6) return 'mid';       // $100M-$1B
  return 'small';                      // <$100M
}

// Given a Yahoo symbol like 'MICC.AS', 'EMBRAC-B.ST', 'MICC' (bare),
// return the inferred { country, exchangeLabel } from the suffix. Returns null if bare.
function inferFromYahooSymbol(yahooSymbol) {
  if (!yahooSymbol || typeof yahooSymbol !== 'string') return null;
  const m = yahooSymbol.match(/\.(AS|L|ST|CO|HE|OL|IC|SW|VX|DE|F|PA|MI|MC|BR|IR|LS|VI|WA|TO|V|HK|SI|AX)$/i);
  if (!m) return null;
  const info = SUFFIX_TO_COUNTRY[`.${m[1].toUpperCase()}`];
  if (!info) return null;
  return { country: info.country, exchangeLabel: info.label };
}

module.exports = {
  parseTicker,
  pickPrimaryTicker,
  marketCapBucket,
  dealSizeBucket,
  inferFromYahooSymbol,
  COUNTRY_NAMES,
  COUNTRY_TO_REGION,
  REGION_HIERARCHY,
  UI_REGIONS,
  EXCHANGES,
};
