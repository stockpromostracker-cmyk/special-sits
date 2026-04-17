// Gemini 2.0 Flash classifier for special situations.
// Takes a raw item (headline + body) and returns either
// { is_special_situation: false } or a structured deal record.

const SYSTEM_PROMPT = `You are a special situations research analyst. Given a headline and body text
from a corporate disclosure, filing, or news item, decide whether it describes a SPECIAL
SITUATION investment opportunity, and if so extract structured deal data.

SPECIAL SITUATIONS include:
- spin_off          : parent company separating a subsidiary into a NEW LISTED entity whose shares will be distributed to existing shareholders (or sold via IPO with shares going to parent holders)
- merger_arb        : announced acquisition where BOTH acquirer and target are PUBLICLY LISTED companies, AND the target's stock can be bought by investors to capture the spread to the offer price. MUST have an identifiable public target ticker. Acquisitions of private subsidiaries, private companies, or asset purchases are NOT merger_arb — use 'other' for those.
- ipo               : initial public offering, direct listing, or explicit listing intention
- spac              : SPAC business combination, extension, redemption, or de-SPAC launch
- tender            : tender offer (third-party or issuer), Dutch auction, mandatory offer — the offer is live and investors can tender shares
- buyback           : significant share repurchase program (ONLY if >=5% of shares outstanding and announced as a program, not a small incremental buy)
- rights            : rights issue or rights offering to existing holders (not a routine secondary offering)
- liquidation       : formal wind-down, liquidation distribution to shareholders
- going_private     : management buyout, take-private transaction, SC 13E3 filing, PE take-out of a listed company
- activist          : activist investor campaign with named activist + identified target, board/proxy contest, Schedule 13D filed with stated intent (not passive 13G)
- share_class       : share class collapse, dual-class unification, recap
- other             : any other well-defined corporate event with a catalyst investors can position around

CRITICAL DISAMBIGUATION — merger_arb vs other:
- Public company A buys public company B for cash/stock → merger_arb (target ticker = B)
- Public company A buys PRIVATE company C (or a division of another company) → 'other' (not merger_arb, because there is no tradable target security)
- Public company A buys a subsidiary / business unit / assets from public company D → 'other'
- Parent spins OUT a subsidiary to its own shareholders → spin_off (not merger_arb)
If you cannot identify a publicly-listed TARGET with a ticker, it is NOT merger_arb.

DO NOT classify as special situation:
- Routine earnings, guidance, regular dividends, analyst reports, price targets
- Small share buybacks under 5%
- Executive appointments, routine governance (use 'activist' only if there's a contest)
- Product launches, partnerships, minor operational news, clinical trial data
- Secondary offerings, follow-on equity raises (unless structured as rights issue)
- Rumors / speculation without named parties
- Already-closed deals from years ago being referenced retrospectively

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

  const modelName = process.env.GEMINI_MODEL || 'gemini-2.5-flash';
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${modelName}:generateContent?key=${key}`;
  const payload = {
    system_instruction: { parts: [{ text: SYSTEM_PROMPT }] },
    contents: [{ role: 'user', parts: [{ text: userText }] }],
    generationConfig: {
      response_mime_type: 'application/json',
      temperature: 0.1,
      maxOutputTokens: 2048,
      // Disable thinking for 2.5 Flash — we just need a structured JSON classification.
      thinkingConfig: { thinkingBudget: 0 },
    },
  };

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const txt = await res.text();
    console.error('[classifier] gemini error', res.status, txt.slice(0, 500));
    return { is_special_situation: false, reason: `classifier error ${res.status}: ${txt.slice(0, 200)}` };
  }
  const data = await res.json();
  // Extract text from all parts (Gemini 2.5 may split thinking + answer across parts)
  const parts = data?.candidates?.[0]?.content?.parts || [];
  const text = parts.map(p => p?.text || '').join('').trim();
  if (!text) {
    const finish = data?.candidates?.[0]?.finishReason;
    const block = data?.promptFeedback?.blockReason;
    console.error('[classifier] empty response', 'finish=', finish, 'block=', block);
    return { is_special_situation: false, reason: `empty response (finish=${finish || 'none'})` };
  }
  try {
    // Strip any accidental markdown code fences
    const cleaned = text.replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/i, '').trim();
    return JSON.parse(cleaned);
  } catch (e) {
    console.error('[classifier] parse failed', text.slice(0, 300));
    return { is_special_situation: false, reason: `parse error: ${text.slice(0, 120)}` };
  }
}

module.exports = { classify };
