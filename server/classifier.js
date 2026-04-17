// Gemini 2.0 Flash classifier for special situations.
// Takes a raw item (headline + body) and returns either
// { is_special_situation: false } or a structured deal record.

const SYSTEM_PROMPT = `You are a special situations research analyst. Given a headline and body text
from a corporate disclosure, filing, or news item, decide whether it describes a SPECIAL
SITUATION investment opportunity, and if so extract structured deal data.

SPECIAL SITUATIONS include:
- spin_off          : parent company separating a subsidiary into a new listed entity
- merger_arb        : an announced acquisition where one listed company is buying another
- ipo               : initial public offering, direct listing, or listing intention
- spac              : SPAC business combination, extension, redemption, or launch
- tender            : tender offer, Dutch auction, mandatory offer
- buyback           : significant share repurchase program (only if >=5% of shares)
- rights            : rights issue or rights offering
- liquidation       : wind-down, liquidation distribution
- going_private     : management buyout, take-private transaction, SC 13E3
- activist          : activist investor campaign, board contest, 13D filing with intent
- share_class       : share class collapse, dual-class unification
- other             : any other well-defined corporate event with catalyst

DO NOT classify as special situation:
- Routine earnings, guidance, dividends (regular), analyst reports
- Small share buybacks under 5%
- Executive appointments, routine governance
- Product launches, partnerships, minor operational news

Return STRICT JSON (no prose, no markdown fences) with this exact shape:

If NOT a special situation:
{"is_special_situation": false, "reason": "brief reason"}

If it IS a special situation:
{
  "is_special_situation": true,
  "deal_type": "spin_off|merger_arb|ipo|spac|tender|buyback|rights|liquidation|going_private|activist|share_class|other",
  "status": "rumored|announced|pending|closed|terminated",
  "region": "US|UK|EU|Nordic|Switzerland|Global",
  "headline": "short headline (max 140 chars)",
  "summary": "2-3 sentence plain-English summary",
  "thesis": "why this is interesting as an investment (1-2 sentences) or null",
  "risks": "key deal risks (1-2 sentences) or null",
  "acquirer_name": "or null",
  "acquirer_ticker": "e.g. NYSE:ABC or LSE:XYZ or null",
  "target_name": "or null",
  "target_ticker": "or null",
  "parent_name": "for spin-offs — the existing parent, or null",
  "parent_ticker": "or null",
  "spinco_name": "for spin-offs — the new entity, or null",
  "spinco_ticker": "or null",
  "deal_value_usd": number in USD or null,
  "consideration": "e.g. 'all cash', '0.5x shares + $10', 'mixed' or null",
  "offer_price": number or null,
  "announce_date": "YYYY-MM-DD or null",
  "expected_close_date": "YYYY-MM-DD or null",
  "record_date": "YYYY-MM-DD or null",
  "ex_date": "YYYY-MM-DD or null"
}`;

async function classify({ headline, body, source }) {
  const key = process.env.GEMINI_API_KEY;
  if (!key) {
    console.warn('[classifier] GEMINI_API_KEY not set — skipping classification');
    return { is_special_situation: false, reason: 'no api key' };
  }

  const userText = `SOURCE: ${source}\nHEADLINE: ${headline || ''}\n\nBODY:\n${(body || '').slice(0, 8000)}`;

  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${key}`;
  const payload = {
    system_instruction: { parts: [{ text: SYSTEM_PROMPT }] },
    contents: [{ role: 'user', parts: [{ text: userText }] }],
    generationConfig: {
      response_mime_type: 'application/json',
      temperature: 0.1,
      maxOutputTokens: 1024,
    },
  };

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const txt = await res.text();
    console.error('[classifier] gemini error', res.status, txt.slice(0, 200));
    return { is_special_situation: false, reason: 'classifier error' };
  }
  const data = await res.json();
  const text = data?.candidates?.[0]?.content?.parts?.[0]?.text || '';
  try {
    return JSON.parse(text);
  } catch (e) {
    console.error('[classifier] parse failed', text.slice(0, 200));
    return { is_special_situation: false, reason: 'parse error' };
  }
}

module.exports = { classify };
