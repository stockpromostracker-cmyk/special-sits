// SpecialSits frontend — hash router, screener, detail drawer, admin.

const view = document.getElementById('view');
const navLinks = document.querySelectorAll('.navlink');

const DEAL_TYPES = [
  ['merger_arb', 'Merger arb'],
  ['spin_off', 'Spin-off'],
  ['ipo', 'IPO'],
  ['spac', 'SPAC'],
  ['tender', 'Tender'],
  ['buyback', 'Buyback'],
  ['rights', 'Rights issue'],
  ['liquidation', 'Liquidation'],
  ['going_private', 'Going private'],
  ['activist', 'Activist'],
  ['share_class', 'Share class'],
  ['other', 'Other'],
];
const STATUSES = ['rumored', 'announced', 'pending', 'closed', 'terminated'];
// Region filter options. 'Europe' is a meta-region that expands on the server
// to match UK + Nordic + Switzerland + EU-Continental. Select a narrower bucket
// (e.g. 'Nordic') to drill in.
const REGIONS = ['US', 'Canada', 'Americas', 'UK', 'Nordic', 'EU-Continental', 'Switzerland', 'Europe', 'Global'];
const COUNTRIES = [
  ['US','🇺🇸 United States'], ['GB','🇬🇧 United Kingdom'],
  ['DE','🇩🇪 Germany'],       ['FR','🇫🇷 France'],
  ['NL','🇳🇱 Netherlands'],   ['CH','🇨🇭 Switzerland'],
  ['SE','🇸🇪 Sweden'],        ['DK','🇩🇰 Denmark'],
  ['NO','🇳🇴 Norway'],        ['FI','🇫🇮 Finland'],
  ['IT','🇮🇹 Italy'],         ['ES','🇪🇸 Spain'],
  ['BE','🇧🇪 Belgium'],       ['IE','🇮🇪 Ireland'],
  ['PT','🇵🇹 Portugal'],      ['AT','🇦🇹 Austria'],
  ['PL','🇵🇱 Poland'],        ['IS','🇮🇸 Iceland'],
];
const MCAP_BUCKETS = [
  ['mega',  'Mega ≥$200B'],
  ['large', 'Large $10-200B'],
  ['mid',   'Mid $2-10B'],
  ['small', 'Small $300M-2B'],
  ['micro', 'Micro $50-300M'],
  ['nano',  'Nano <$50M'],
];
const DEAL_SIZE_BUCKETS = [
  ['mega',  'Mega ≥$10B'],
  ['large', 'Large $1-10B'],
  ['mid',   'Mid $100M-1B'],
  ['small', 'Small <$100M'],
];
const INSIDER_SIGNALS = [
  ['any',       'Any skin-in-the-game'],
  ['cluster',   '🟢 Cluster insider buying'],
  ['mgmt_spin', '👤 Mgmt moves to SpinCo'],
  ['rollover',  '💼 Mgmt/founder rollover'],
  ['activist',  '⚔️ Activist on register'],
];

// Authoritative event types (regulator-first schema)
const EVENT_TYPES = [
  ['spin_off_pending',   'Spin-off — pending'],
  ['spin_off_completed', 'Spin-off — completed'],
  ['ipo_pending',        'IPO — pending'],
  ['ipo_recent',         'IPO — recent'],
  ['merger_pending',     'Merger — pending'],
  ['merger_completed',   'Merger — completed'],
  ['demerger_pending',   'Demerger — pending'],
];
// Trust tier badges shown on screener rows
const TIER_META = {
  official:   { label: 'Official',   icon: '✅', tooltip: 'Primary source: SEC / LSE RNS / MAR regulator release', cls: 'tier-official' },
  aggregator: { label: 'Aggregator', icon: '📊', tooltip: 'Secondary source: stockanalysis.com', cls: 'tier-aggregator' },
  news:       { label: 'News',       icon: '📰', tooltip: 'News-derived only — lower confidence', cls: 'tier-news' },
};
const TIMEFRAMES = [
  ['',         'All deals'],
  ['upcoming', 'Upcoming (≤ 90d)'],
  ['recent',   'Recent (≤ 90d)'],
];

let state = { deals: [], stats: null, filters: {}, admin: null };

// ---- Router ---------------------------------------------------------------
function route() {
  const hash = location.hash.replace(/^#/, '') || '/';
  navLinks.forEach(a => a.classList.remove('active'));
  if (hash.startsWith('/admin')) {
    document.querySelector('[data-nav="admin"]')?.classList.add('active');
    renderAdmin();
  } else {
    document.querySelector('[data-nav="screener"]')?.classList.add('active');
    renderScreener();
  }
}
window.addEventListener('hashchange', route);

// ---- API helpers ----------------------------------------------------------
async function api(path, opts = {}) {
  const res = await fetch(path, {
    ...opts,
    headers: { 'content-type': 'application/json', ...(opts.headers || {}) },
  });
  if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || res.statusText);
  return res.json();
}

// ---- Screener -------------------------------------------------------------
async function renderScreener() {
  view.innerHTML = `
    <div class="kpis" id="kpis">${skeletonKpis()}</div>
    <div class="timeframe-tabs" id="timeframe-tabs">
      ${TIMEFRAMES.map(([v, l]) => `<button class="tf-tab ${v==='' ? 'active' : ''}" data-tf="${v}">${l}</button>`).join('')}
    </div>
    <div class="filters">
      <input id="f-q" type="search" placeholder="Search tickers, companies, headlines…" />
      <select id="f-event"><option value="">All events</option>${EVENT_TYPES.map(([v,l]) => `<option value="${v}">${l}</option>`).join('')}</select>
      <select id="f-tier"><option value="">Any source</option><option value="official">✅ Official only</option><option value="aggregator">📊 Aggregator+</option></select>
      <select id="f-type"><option value="">All types</option>${DEAL_TYPES.map(([v,l]) => `<option value="${v}">${l}</option>`).join('')}</select>
      <select id="f-status"><option value="">All statuses</option>${STATUSES.map(s => `<option value="${s}">${cap(s)}</option>`).join('')}</select>
      <select id="f-region"><option value="">All regions</option>${REGIONS.map(r => `<option value="${r}">${r}</option>`).join('')}</select>
      <select id="f-country"><option value="">All countries</option>${COUNTRIES.map(([v,l]) => `<option value="${v}">${l}</option>`).join('')}</select>
      <select id="f-mcap"><option value="">Any market cap</option>${MCAP_BUCKETS.map(([v,l]) => `<option value="${v}">${l}</option>`).join('')}</select>
      <select id="f-dsize"><option value="">Any deal size</option>${DEAL_SIZE_BUCKETS.map(([v,l]) => `<option value="${v}">${l}</option>`).join('')}</select>
      <select id="f-insider"><option value="">Any insider signal</option>${INSIDER_SIGNALS.map(([v,l]) => `<option value="${v}">${l}</option>`).join('')}</select>
      <select id="f-arb" title="Merger-arb filters — only affect rows with an expected-close date or an offer price"><option value="">Any arb</option><option value="below_offer">Trading below offer</option><option value="tight_spread">Tight spread (<3%)</option><option value="closing_soon">Closing &lt; 90d</option><option value="closing_soon_below">Closing &lt; 90d &amp; below offer</option></select>
      <label class="filter-check" title="By default, SPAC shells are hidden (they clog the IPO feed with no-price rows)"><input id="f-spacs" type="checkbox" /> Show SPACs</label>
      <span class="spacer"></span>
      <button class="btn-ghost btn" id="f-reset">Reset</button>
    </div>
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th>Event</th>
          <th class="hide-sm">Source</th>
          <th class="hide-sm">Country</th>
          <th>Headline / parties</th>
          <th>Ticker</th>
          <th class="td-right" id="th-date">Date</th>
          <th class="td-right hide-sm">Mkt cap</th>
          <th class="td-right">Return</th>
          <th class="hide-sm">Skin</th>
        </tr></thead>
        <tbody id="deals-body"><tr><td colspan="9" class="loading">Loading deals…</td></tr></tbody>
      </table>
    </div>
    <div class="drawer-backdrop" id="drawer-backdrop"></div>
    <div class="drawer" id="drawer"></div>
  `;

  document.getElementById('f-q').addEventListener('input', debounce(applyFilters, 250));
  document.getElementById('f-spacs').addEventListener('change', applyFilters);
  ['f-type','f-status','f-region','f-country','f-mcap','f-dsize','f-insider','f-event','f-tier','f-arb'].forEach(id =>
    document.getElementById(id).addEventListener('change', applyFilters));
  document.getElementById('f-reset').addEventListener('click', () => {
    ['f-q','f-type','f-status','f-region','f-country','f-mcap','f-dsize','f-insider','f-event','f-tier','f-arb']
      .forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
    document.getElementById('f-spacs').checked = false;
    state.filters.timeframe = '';
    document.querySelectorAll('.tf-tab').forEach(b => b.classList.toggle('active', b.dataset.tf === ''));
    applyFilters();
  });
  document.querySelectorAll('.tf-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.tf-tab').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      state.filters.timeframe = btn.dataset.tf;
      // Update the Date column header label to match the selected timeframe
      const th = document.getElementById('th-date');
      if (th) th.textContent = btn.dataset.tf === 'upcoming' ? 'In' : btn.dataset.tf === 'recent' ? 'Ago' : 'Date';
      loadDeals();
    });
  });
  document.getElementById('drawer-backdrop').addEventListener('click', closeDrawer);

  await Promise.all([loadStats(), loadDeals()]);
}

async function loadStats() {
  try {
    const s = await api('/api/stats');
    state.stats = s;
    const fmt = (n) => (n || 0).toLocaleString();
    const el = document.getElementById('kpis');
    if (!el) return;
    el.innerHTML = `
      ${kpi('Total deals', fmt(s.total))}
      ${kpi('Upcoming ≤90d', fmt(s.upcoming_90d), 'kpi-upcoming')}
      ${kpi('Recent ≤90d', fmt(s.recent_90d), 'kpi-recent')}
      ${kpi('✅ Official', fmt(s.tier_official), 'kpi-official')}
      ${kpi('📊 Aggregator', fmt(s.tier_aggregator), 'kpi-aggregator')}
      ${kpi('Spin-offs', fmt(s.spin_off))}
      ${kpi('IPOs', fmt(s.ipo))}
      ${kpi('Mergers', fmt(s.merger_arb))}
    `;
  } catch (e) {
    document.getElementById('kpis').innerHTML = `<div class="kpi">Could not load stats</div>`;
  }
}
function kpi(label, value, extraClass) {
  return `<div class="kpi ${extraClass || ''}"><div class="kpi-label">${label}</div><div class="kpi-value">${value}</div></div>`;
}
function skeletonKpis() {
  return Array(8).fill(0).map(() =>
    `<div class="kpi"><div class="kpi-label">&nbsp;</div><div class="skel" style="height:22px;width:60%;margin-top:4px"></div></div>`
  ).join('');
}

async function loadDeals() {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(state.filters)) if (v) params.set(k, v);
  try {
    const deals = await api('/api/deals?' + params.toString());
    state.deals = deals;
    renderDealsRows();
  } catch (e) {
    document.getElementById('deals-body').innerHTML =
      `<tr><td colspan="9" class="empty">Could not load deals: ${esc(e.message)}</td></tr>`;
  }
}

function renderDealsRows() {
  const tbody = document.getElementById('deals-body');
  if (!state.deals.length) {
    tbody.innerHTML = `<tr><td colspan="9" class="empty">
      <h3>No deals match</h3>
      <div>Try clearing filters or selecting another timeframe. If the database is empty, run the authoritative ingest from the Admin page.</div>
    </td></tr>`;
    return;
  }
  tbody.innerHTML = state.deals.map(d => `
    <tr data-id="${d.id}">
      <td>${eventCell(d)}</td>
      <td class="hide-sm">${trustBadge(d)}</td>
      <td class="hide-sm">${countryBadge(d)}</td>
      <td class="td-headline">
        <div>${esc(d.headline || '')}</div>
        <div class="sub">${esc(dealParties(d))}</div>
      </td>
      <td class="td-tickers">${primaryTickerCell(d)}</td>
      <td class="td-right mono">${dateCell(d)}</td>
      <td class="td-right hide-sm mono">${fmtMcap(d.market_cap_usd)}</td>
      <td class="td-right mono">${returnCell(d)}</td>
      <td class="hide-sm">${skinCell(d)}</td>
    </tr>
  `).join('');
  tbody.querySelectorAll('tr').forEach(tr => {
    tr.addEventListener('click', () => openDrawer(tr.dataset.id));
  });
}

// Event cell: event_type badge (preferred) or falls back to deal_type badge
function eventCell(d) {
  if (d.event_type) {
    const label = EVENT_TYPES.find(([v]) => v === d.event_type)?.[1] || d.event_type;
    const family = d.event_type.split('_')[0]; // spin | ipo | merger | demerger
    return `<span class="badge badge-event badge-event-${family}">${esc(label)}</span>`;
  }
  return `<span class="badge badge-type-${d.deal_type}">${labelType(d.deal_type)}</span>`;
}

// Trust badge: official / aggregator / news
function trustBadge(d) {
  const tier = d.data_source_tier || 'news';
  const meta = TIER_META[tier];
  if (!meta) return '<span style="color:var(--muted)">—</span>';
  return `<span class="trust-badge ${meta.cls}" title="${esc(meta.tooltip)}"><span class="ic">${meta.icon}</span><span class="lb">${esc(meta.label)}</span></span>`;
}

// Date cell: shows days-to or days-since depending on the row. Falls back to announce_date.
function dateCell(d) {
  if (d.days_to_event != null && d.days_to_event >= 0) {
    const cls = d.days_to_event <= 14 ? 'date-chip-urgent' : d.days_to_event <= 45 ? 'date-chip-soon' : 'date-chip';
    return `<span class="${cls}">in ${d.days_to_event}d</span>`;
  }
  if (d.days_since_event != null && d.days_since_event >= 0) {
    const cls = d.days_since_event <= 30 ? 'date-chip-fresh' : 'date-chip';
    return `<span class="${cls}">${d.days_since_event}d ago</span>`;
  }
  if (d.completed_date) return esc(String(d.completed_date).slice(0,10));
  if (d.announce_date)  return esc(String(d.announce_date).slice(0,10));
  if (d.filing_date)    return esc(String(d.filing_date).slice(0,10));
  return '—';
}

function applyFilters() {
  state.filters = {
    q: document.getElementById('f-q').value.trim(),
    type: document.getElementById('f-type').value,
    status: document.getElementById('f-status').value,
    region: document.getElementById('f-region').value,
    country: document.getElementById('f-country').value,
    market_cap_bucket: document.getElementById('f-mcap').value,
    deal_size_bucket: document.getElementById('f-dsize').value,
    insider_signal: document.getElementById('f-insider').value,
    event_type: document.getElementById('f-event').value,
    data_source_tier: document.getElementById('f-tier').value,
    arb_filter: document.getElementById('f-arb')?.value || '',
    timeframe: state.filters.timeframe || '',
    include_spacs: document.getElementById('f-spacs').checked ? '1' : '',
  };
  loadDeals();
}

// ---- Drawer ---------------------------------------------------------------
async function openDrawer(id) {
  const drawer = document.getElementById('drawer');
  const backdrop = document.getElementById('drawer-backdrop');
  drawer.classList.add('open'); backdrop.classList.add('open');
  drawer.innerHTML = `<div class="drawer-head"><h2>Loading…</h2><button class="drawer-close">&times;</button></div>`;
  drawer.querySelector('.drawer-close').addEventListener('click', closeDrawer);

  try {
    const [d, inc, rel, own] = await Promise.all([
      api(`/api/deals/${id}`),
      api(`/api/deals/${id}/incentives`).catch(() => null),
      api(`/api/deals/${id}/related`).catch(() => null),
      api(`/api/deals/${id}/ownership`).catch(() => null),
    ]);
    const kd = d.key_dates && typeof d.key_dates === 'object' ? d.key_dates : {};
    drawer.innerHTML = `
      <div class="drawer-head">
        <h2>${esc(d.headline || '')}</h2>
        <button class="drawer-close" aria-label="Close">&times;</button>
      </div>
      <div class="drawer-body">
        <div class="drawer-meta">
          ${d.event_type ? `<span class="badge badge-event badge-event-${d.event_type.split('_')[0]}">${esc(d.event_label || d.event_type)}</span>` : `<span class="badge badge-type-${d.deal_type}">${labelType(d.deal_type)}</span>`}
          <span class="badge badge-status-${d.status || 'announced'}">${cap(d.status || 'announced')}</span>
          ${d.region ? `<span class="badge badge-region">${esc(d.region)}</span>` : ''}
          ${trustBadge(d)}
          ${d.days_to_event != null ? `<span class="date-chip-urgent">in ${d.days_to_event}d</span>` :
            d.days_since_event != null ? `<span class="date-chip-fresh">${d.days_since_event}d ago</span>` : ''}
        </div>
        ${d.source_filing_url ? `<div class="primary-source"><span class="src-label">Primary source:</span> <a href="${esc(d.source_filing_url)}" target="_blank" rel="noopener">${esc(d.primary_source || 'Filing')} ↗</a> ${externalChartLinks(d)}</div>` : ''}

        ${renderChartSection(d)}

        ${d.summary ? `<div class="section"><h3>Summary</h3><p>${esc(d.summary)}</p></div>` : ''}
        ${d.thesis ? `<div class="section"><h3>Thesis</h3><p>${esc(d.thesis)}</p></div>` : ''}
        ${d.risks ? `<div class="section"><h3>Risks</h3><p>${esc(d.risks)}</p></div>` : ''}

        <div class="section">
          <h3>Market snapshot</h3>
          <dl class="kv">
            ${row('Primary ticker', d.primary_ticker)}
            ${row('Country', d.country ? `${COUNTRY_FLAG[d.country] || ''} ${d.country}` : null)}
            ${row('Sector', d.sector)}
            ${row('Industry', d.industry)}
            ${row('Market cap', fmtMcap(d.market_cap_usd))}
            ${row('Announce price', d.announce_price != null ? `$${Number(d.announce_price).toFixed(2)}` : null)}
            ${row('Current price', d.current_price != null ? `$${Number(d.current_price).toFixed(2)}` : null)}
            ${(() => {
              const isSpin = (d.deal_type === 'spin_off') || (d.event_type && d.event_type.startsWith('spin'));
              const isMerger = (d.deal_type === 'merger_arb') || (d.event_type && d.event_type.startsWith('merger'));
              // Helper: emit a <dt>/<dd> with raw HTML (bypasses esc() in row())
              const rawRow = (label, html) => `<dt>${label}</dt><dd>${html != null ? html : '—'}</dd>`;

              if (isSpin && (d.parent_return_pct != null || d.spinco_return_pct != null)) {
                const pr = d.parent_return_pct, sr = d.spinco_return_pct;
                const pLabel = `Parent return <span class="kv-hint" title="RemainCo performance since ex-date">ⓘ</span>`;
                const sLabel = `SpinCo return <span class="kv-hint" title="New entity performance since first trade">ⓘ</span>`;
                return rawRow(pLabel, pr != null ? `<span class="${returnClass(pr)}">${fmtReturn(pr)}</span>` : null)
                     + rawRow(sLabel, sr != null ? `<span class="${returnClass(sr)}">${fmtReturn(sr)}</span>` : null);
              }

              // For mergers, suppress the misleading "return since announce" here.
              // The dedicated "Merger arb" section below shows spread-to-deal,
              // unaffected price, and bid premium — the metrics that actually matter.
              if (isMerger) return '';

              const ap = d.announce_price, cp = d.current_price;
              if (ap == null || cp == null) return '';
              const r = ((cp-ap)/ap)*100;
              return rawRow('Return since announce', `<span class="${returnClass(r)}">${fmtReturn(r)}</span>`);
            })()}
            ${row('Refreshed', d.market_refreshed_at ? String(d.market_refreshed_at).slice(0,16) : null)}
          </dl>
        </div>

        ${renderIncentiveSection(inc)}

        ${(() => {
          const isMerger = (d.deal_type === 'merger_arb') || (d.event_type && d.event_type.startsWith('merger'));
          if (!isMerger) {
            return `<div class="section"><h3>Deal terms</h3><dl class="kv">
              ${row('Consideration', d.consideration)}
              ${row('Offer price', d.offer_price != null ? `$${Number(d.offer_price).toFixed(2)}` : null)}
              ${row('Deal value (USD)', d.deal_value_usd ? fmtM(d.deal_value_usd) + 'M' : null)}
              ${row('Current spread', d.spread_pct != null ? d.spread_pct + '%' : null)}
            </dl></div>`;
          }
          // ---- Merger arb section — the metrics that actually matter ----
          const rawRow = (label, html) => `<dt>${label}</dt><dd>${html != null ? html : '—'}</dd>`;
          const offer = d.offer_price;
          const curr  = d.current_price;
          const unaff = d.unaffected_price;
          const spread = d.spread_to_deal_pct;
          const premium = (offer != null && unaff != null && unaff > 0) ? ((offer - unaff) / unaff) * 100 : null;
          const spreadHtml = spread != null
            ? `<span class="${spread >= 0 ? 'ret-pos' : 'ret-neg'}">${fmtReturn(spread)}</span> <span class="mute">← positive = trading below offer</span>`
            : null;
          const premiumHtml = premium != null
            ? `<span class="${returnClass(premium)}">${fmtReturn(premium)}</span> <span class="mute">over unaffected</span>`
            : null;
          const annSrcLabel = {
            sec_8k_101: 'from 8-K Item 1.01',
            sec_defa14a: 'from DEFA14A',
            sec_prem14a: 'from PREM14A',
            filing_date: 'proxy filing date (approx.)',
          }[d.announce_date_source] || '';
          return `<div class="section">
            <h3>Merger arb</h3>
            <dl class="kv">
              ${row('Consideration', d.consideration)}
              ${row('Offer price', offer != null ? `$${Number(offer).toFixed(2)}` : null)}
              ${row('Unaffected price', unaff != null ? `$${Number(unaff).toFixed(2)} (1 day pre-announce)` : null)}
              ${row('Current price', curr != null ? `$${Number(curr).toFixed(2)}` : null)}
              ${rawRow('Spread to deal', spreadHtml)}
              ${rawRow('Bid premium', premiumHtml)}
              ${row('Announce date', d.announce_date ? `${d.announce_date}${annSrcLabel ? ' — ' + annSrcLabel : ''}` : null)}
              ${row('Deal value (USD)', d.deal_value_usd ? fmtM(d.deal_value_usd) + 'M' : null)}
            </dl>
          </div>`;
        })()}

        <div class="section">
          <h3>Parties</h3>
          <dl class="kv">
            ${row('Acquirer', nameTicker(d.acquirer_name, d.acquirer_ticker))}
            ${row('Target', nameTicker(d.target_name, d.target_ticker))}
            ${row('Parent', nameTicker(d.parent_name, d.parent_ticker))}
            ${row('SpinCo', nameTicker(d.spinco_name, d.spinco_ticker))}
          </dl>
        </div>

        ${renderRelationshipGraph(rel, d)}

        ${renderOwnershipCompSection(d, own)}

        <div class="section">
          <h3>Timeline</h3>
          <dl class="kv">
            ${row('Filed', d.filing_date || kd.filing_date)}
            ${row('Announced', d.announce_date)}
            ${row('Record date', d.record_date || kd.record_date)}
            ${row('Ex-date', d.ex_date || kd.ex_date)}
            ${row('First trade date', kd.first_trade_date)}
            ${row('Effective date', kd.effective_date)}
            ${row('Expected close', d.expected_close_date || kd.expected_close_date)}
            ${row('Completed', d.completed_date || kd.completed_date)}
            ${row('First seen', d.first_seen_at)}
            ${row('Last updated', d.updated_at)}
          </dl>
        </div>

        ${renderEventTimeline(d, { groupSameDate: true })}
      </div>
    `;
    drawer.querySelector('.drawer-close').addEventListener('click', closeDrawer);
  } catch (e) {
    drawer.innerHTML = `<div class="drawer-head"><h2>Error</h2><button class="drawer-close">&times;</button></div>
      <div class="drawer-body"><p>${esc(e.message)}</p></div>`;
    drawer.querySelector('.drawer-close').addEventListener('click', closeDrawer);
  }
}
function closeDrawer() {
  document.getElementById('drawer')?.classList.remove('open');
  document.getElementById('drawer-backdrop')?.classList.remove('open');
}

function row(k, v) {
  if (v == null || v === '') return `<dt>${k}</dt><dd>—</dd>`;
  return `<dt>${k}</dt><dd>${esc(String(v))}</dd>`;
}
function nameTicker(name, ticker) {
  if (!name && !ticker) return null;
  if (name && ticker) return `${name} (${ticker})`;
  return name || ticker;
}

// -----------------------------------------------------------------
// Relationship graph — shows Parent ↔ Self ↔ SpinCo chain plus siblings
// (other spin-offs from the same parent). Every node with an id is a
// clickable drawer link; nodes without id are still labeled so the
// user sees the chain.
function renderRelationshipGraph(rel, self) {
  if (!rel) return '';
  const hasParent = rel.parent && (rel.parent.name || rel.parent.ticker);
  const hasSpinco = rel.spinco && (rel.spinco.name || rel.spinco.ticker);
  const siblings = Array.isArray(rel.siblings) ? rel.siblings : [];
  if (!hasParent && !hasSpinco && !siblings.length) return '';

  const nodeHtml = (node, role) => {
    if (!node) return `<div class="rg-node rg-empty">—</div>`;
    const label = nameTicker(node.name, node.ticker) || node.headline || node.ticker || '?';
    const inner = `<span class="rg-role">${role}</span><span class="rg-label">${esc(label)}</span>`;
    if (node.id && node.id !== self.id) {
      return `<a class="rg-node rg-link" href="#" data-open-deal="${node.id}">${inner}</a>`;
    }
    return `<div class="rg-node ${node.id === self.id ? 'rg-self' : ''}">${inner}</div>`;
  };

  const selfNode = { id: self.id, name: self.spinco_name || self.target_name || self.primary_ticker, ticker: self.primary_ticker };
  const chain = `
    <div class="rg-chain">
      ${nodeHtml(rel.parent, 'Parent')}
      <div class="rg-arrow">→</div>
      ${nodeHtml(selfNode, self.deal_type === 'spin_off' ? 'SpinCo' : 'This deal')}
      ${hasSpinco ? `<div class="rg-arrow">→</div>${nodeHtml(rel.spinco, 'SpinCo')}` : ''}
    </div>`;

  const sibHtml = siblings.length
    ? `<div class="rg-siblings">
         <h4>Other spin-offs from the same parent</h4>
         <ul class="rg-sib-list">
           ${siblings.map(s => {
             const lbl = nameTicker(s.name, s.ticker) || s.headline;
             const date = s.completed_date ? ` <span class="rg-date">· ${s.completed_date}</span>` : '';
             return `<li><a href="#" data-open-deal="${s.id}">${esc(lbl)}</a>${date}</li>`;
           }).join('')}
         </ul>
       </div>`
    : '';

  return `
    <div class="section">
      <h3>Relationship</h3>
      ${chain}
      ${sibHtml}
    </div>`;
}

// Wire up clicks on relationship graph links + sibling list to re-open drawer.
// Uses event delegation on document so the handler covers fresh markup.
if (!window.__rgGraphWired) {
  window.__rgGraphWired = true;
  document.addEventListener('click', (e) => {
    const a = e.target.closest('[data-open-deal]');
    if (!a) return;
    e.preventDefault();
    const id = parseInt(a.getAttribute('data-open-deal'), 10);
    if (id) openDrawer(id);
  });
}

// -----------------------------------------------------------------
// Compensation & ownership — combined DATA + link-outs.
// Data shown: top insider holders (any market we ingest from) and
// CEO/NEO compensation (US issuers with SEC XBRL Pay-vs-Performance).
// Link-outs shown: DEF 14A, 13D/G, Form 3/4 SEC searches (US only).
function renderOwnershipCompSection(d, own) {
  const linksHtml = renderOfficialDocLinksInner(d);
  const ownershipHtml = renderOwnershipTable(own);
  const compHtml = renderCompensationBlock(own);

  // If absolutely nothing to show (non-US, no insider history), bail.
  if (!linksHtml && !ownershipHtml && !compHtml) return '';

  return `<div class="section">
    <h3>Compensation &amp; ownership</h3>
    <p class="hint-text">Skin-in-the-game: who owns it, how they're paid.</p>
    ${compHtml}
    ${ownershipHtml}
    ${linksHtml}
  </div>`;
}

function renderOwnershipTable(own) {
  if (!own || !own.ownership || !own.ownership.holders || !own.ownership.holders.length) {
    // For non-US issuers or deals with no insider history, show explanatory copy.
    if (own && own.ownership && own.ownership.notes && own.ownership.notes.length) {
      return `<p class="hint-text" style="margin:6px 0 12px">Top holders: <em>${esc(own.ownership.notes[0])}</em></p>`;
    }
    return '';
  }
  const h = own.ownership.holders;
  const rows = h.map(r => {
    const title = [r.insider_title, r.is_ten_percent_owner ? '10%+ owner' : null].filter(Boolean).join(' • ');
    const activity = r.buy_count_180d || r.sell_count_180d
      ? `${r.buy_count_180d ? `<span class="tx-buy">${r.buy_count_180d} buy</span>` : ''}${r.buy_count_180d && r.sell_count_180d ? ' / ' : ''}${r.sell_count_180d ? `<span class="tx-sell">${r.sell_count_180d} sell</span>` : ''}`
      : '<span style="color:var(--muted)">—</span>';
    const netShares = r.net_shares ? (r.net_shares > 0 ? `+${fmtCompact(r.net_shares)}` : fmtCompact(r.net_shares)) : '—';
    const netCls = r.net_shares > 0 ? 'tx-buy' : r.net_shares < 0 ? 'tx-sell' : '';
    return `<tr>
      <td>${esc(r.insider_name)}</td>
      <td style="color:var(--muted);font-size:12px">${esc(title || '—')}</td>
      <td class="td-right ${netCls}">${netShares}</td>
      <td style="font-size:12px">${activity}</td>
      <td style="color:var(--muted);font-size:12px">${esc(r.last_date || '—')}</td>
    </tr>`;
  }).join('');
  return `<h4 style="margin:12px 0 6px;font-size:13px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:0.05em">Top insiders (3y net)</h4>
    <div class="insider-tx-wrap"><table class="insider-tx">
      <thead><tr><th>Insider</th><th>Title</th><th class="td-right">Net shares (3y)</th><th>180d activity</th><th>Last</th></tr></thead>
      <tbody>${rows}</tbody>
    </table></div>`;
}

function renderCompensationBlock(own) {
  if (!own || !own.compensation || !own.compensation.items || !own.compensation.items.length) return '';
  const items = own.compensation.items;
  // Build a per-concept mini-table of the two most recent fiscal years.
  const allYears = new Set();
  for (const it of items) for (const y of it.years) allYears.add(y.fy);
  const fyList = [...allYears].sort((a, b) => b - a).slice(0, 2);
  if (!fyList.length) return '';
  const header = `<tr><th>Metric</th>${fyList.map(fy => `<th class="td-right">FY${fy}</th>`).join('')}</tr>`;
  const rows = items.map(it => {
    const cells = fyList.map(fy => {
      const y = it.years.find(x => x.fy === fy);
      return `<td class="td-right">${y ? fmtUsdCompact(y.val) : '—'}</td>`;
    }).join('');
    return `<tr><td>${esc(it.label)}</td>${cells}</tr>`;
  }).join('');
  return `<h4 style="margin:12px 0 6px;font-size:13px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:0.05em">Executive compensation (SEC Pay-vs-Performance)</h4>
    <div class="insider-tx-wrap"><table class="insider-tx">
      <thead>${header}</thead>
      <tbody>${rows}</tbody>
    </table></div>`;
}

function fmtCompact(n) {
  if (n == null || !isFinite(n)) return '—';
  const abs = Math.abs(n);
  if (abs >= 1e9) return (n/1e9).toFixed(2) + 'B';
  if (abs >= 1e6) return (n/1e6).toFixed(2) + 'M';
  if (abs >= 1e3) return (n/1e3).toFixed(1) + 'K';
  return String(Math.round(n));
}

function renderOfficialDocLinksInner(d) {
  const ticker = d.primary_ticker || d.target_ticker;
  const parentTicker = d.parent_ticker;
  const cik = d.source_cik;
  // Build SEC EDGAR-browse URLs by CIK (only for US issuers with a CIK we know)
  const usIssuer = d.country === 'US' || (cik && /^\d+$/.test(String(cik)));
  const links = [];
  if (cik) {
    const cikPlain = String(cik).replace(/^0+/, '');
    links.push({
      label: 'DEF 14A (proxy — executive compensation)',
      url: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cikPlain}&type=DEF+14A&dateb=&owner=include&count=10`,
      hint: 'Annual proxy: CEO/CFO comp, equity grants, golden parachutes',
    });
    links.push({
      label: 'Schedule 13D / 13G (5%+ beneficial owners)',
      url: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cikPlain}&type=SC+13&dateb=&owner=include&count=20`,
      hint: 'Activist or strategic investor stakes',
    });
    links.push({
      label: 'Form 4 insider transactions',
      url: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cikPlain}&type=4&dateb=&owner=include&count=40`,
      hint: 'Every insider trade by officers & directors',
    });
    links.push({
      label: 'Form 3 initial ownership (post-spin / post-IPO)',
      url: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cikPlain}&type=3&dateb=&owner=include&count=20`,
      hint: 'Who owned what on day one',
    });
  }
  // Parent-side links
  if (parentTicker && d.deal_type === 'spin_off' && usIssuer) {
    links.push({
      label: `Parent ${esc(parentTicker)} — DEF 14A`,
      url: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${encodeURIComponent(parentTicker)}&type=DEF+14A&dateb=&owner=include&count=10`,
      hint: 'Compare parent vs spinco exec incentives',
    });
  }
  if (!links.length) return '';
  return `
      <h4 style="margin:12px 0 6px;font-size:13px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:0.05em">Source filings</h4>
      <ul class="doc-links">
        ${links.map(l => `<li><a href="${esc(l.url)}" target="_blank" rel="noopener"><span class="doc-link-label">${l.label} ↗</span><span class="doc-link-hint">${esc(l.hint)}</span></a></li>`).join('')}
      </ul>`;
}

function renderIncentiveSection(inc) {
  if (!inc || !inc.rollup) return '';
  const r = inc.rollup || {};
  const tx = Array.isArray(inc.transactions) ? inc.transactions : [];
  const badges = Array.isArray(inc.incentive_badges) ? inc.incentive_badges : [];

  const hasAnySignal = badges.length
    || r.insider_buy_count_6m
    || r.cluster_buying
    || r.activist_on_register
    || r.mgmt_moves_to_spinco
    || r.founder_rollover
    || r.incentive_notes
    || tx.length;
  if (!hasAnySignal) return '';

  const badgeStrip = badges.length
    ? `<div class="skin-badges skin-badges-lg">${badges.map(b =>
        `<span class="skin-badge skin-badge-lg" title="${esc(b.tooltip || '')}"><span class="ic">${b.icon || '•'}</span><span class="lb">${esc(b.label || '')}</span></span>`
      ).join('')}</div>`
    : '';

  const kv = `
    <dl class="kv">
      ${row('Cluster buying (≥3 insiders, 180d)', r.cluster_buying ? 'Yes' : null)}
      ${row('Insider buys 180d', r.insider_buy_count_6m ? `${r.insider_buy_count_6m} tx · ${fmtUsdCompact(r.insider_buy_usd_6m)}` : null)}
      ${row('Insider sells 180d', r.insider_sell_usd_6m ? fmtUsdCompact(r.insider_sell_usd_6m) : null)}
      ${row('Net insider 180d', r.insider_net_usd_6m != null ? fmtUsdCompact(r.insider_net_usd_6m) : null)}
      ${row('Avg insider buy price', r.avg_insider_buy_price ? `$${Number(r.avg_insider_buy_price).toFixed(2)}` : null)}
      ${row('Trading below insider $', r.trading_below_insider_price ? 'Yes' : null)}
      ${row('Mgmt moves to SpinCo', r.mgmt_moves_to_spinco ? 'Yes' : null)}
      ${row('Mgmt retention %', r.mgmt_retention_pct != null ? `${r.mgmt_retention_pct}%` : null)}
      ${row('Sponsor promote %', r.sponsor_promote_pct != null ? `${r.sponsor_promote_pct}%` : null)}
      ${row('Founder rollover', r.founder_rollover ? 'Yes' : null)}
      ${row('Bidder stake pre-deal', r.bidder_stake_pre_deal != null ? `${r.bidder_stake_pre_deal}%` : null)}
      ${row('Activist on register', r.activist_on_register ? 'Yes' : null)}
      ${row('Refreshed', r.insider_refreshed_at ? String(r.insider_refreshed_at).slice(0,16) : null)}
    </dl>`;

  const notes = r.incentive_notes
    ? `<p class="incentive-notes">${esc(r.incentive_notes)}</p>`
    : '';

  const txTable = tx.length
    ? `<div class="insider-tx-wrap">
         <table class="insider-tx">
           <thead><tr>
             <th>Date</th><th>Insider</th><th>Title</th><th>Type</th>
             <th class="td-right">Shares</th><th class="td-right">Price</th><th class="td-right">Value</th><th>Src</th>
           </tr></thead>
           <tbody>
             ${tx.slice(0, 25).map(t => {
               const type = t.is_ten_percent_owner ? 'Stake' : (t.is_buy === 1 || t.is_buy === true ? 'Buy' : (t.is_buy === 0 || t.is_buy === false ? 'Sell' : '—'));
               const cls = type === 'Buy' ? 'tx-buy' : type === 'Sell' ? 'tx-sell' : type === 'Stake' ? 'tx-stake' : '';
               const shares = t.shares ? Number(t.shares).toLocaleString(undefined, { maximumFractionDigits: 0 }) : '—';
               const price = t.price_per_share ? `$${Number(t.price_per_share).toFixed(2)}` : '—';
               const value = t.value_usd ? fmtUsdCompact(t.value_usd) : '—';
               const src = t.source_url ? `<a href="${esc(t.source_url)}" target="_blank" rel="noopener">↗</a>` : '';
               return `<tr class="insider-tx-row">
                 <td class="mono">${esc(String(t.transaction_date || '').slice(0,10))}</td>
                 <td>${esc(t.insider_name || '—')}</td>
                 <td>${esc(t.insider_title || '')}</td>
                 <td><span class="tx-badge ${cls}">${type}</span></td>
                 <td class="td-right mono">${shares}</td>
                 <td class="td-right mono">${price}</td>
                 <td class="td-right mono">${value}</td>
                 <td>${src}</td>
               </tr>`;
             }).join('')}
           </tbody>
         </table>
       </div>`
    : '<p style="color:var(--muted);margin:0">No recent insider transactions matched.</p>';

  return `
    <div class="section incentive-section">
      <h3>Incentive layer</h3>
      ${badgeStrip}
      ${kv}
      ${notes}
      <h4 class="incentive-sub">Recent insider transactions</h4>
      ${txTable}
    </div>`;
}

// ---- Chart helpers -------------------------------------------------------
// Convert any of these inputs to a TradingView symbol:
//   'Nasdaq Stockholm:EMBRAC-B'  -> OMXSTO:EMBRAC_B
//   'NYSE:BIRD'                  -> NYSE:BIRD
//   'STO:EMBRAC-B'               -> OMXSTO:EMBRAC_B   (short-form exchange alias)
//   'COFFEE-B.ST'                -> OMXSTO:COFFEE_B   (Yahoo-suffix form)
//   'MICC.AS'                    -> EURONEXT:MICC
// Yahoo suffix → TradingView exchange prefix. Euronext uses city-specific codes
// on TradingView (ENXTAM Amsterdam, EURONEXT Paris, ENXTBR Brussels, etc.).
const TV_SUFFIX_MAP = {
  '.ST': 'OMXSTO', '.CO': 'OMXCOP', '.HE': 'OMXHEX', '.OL': 'OSL', '.IC': 'OMXICE',
  '.L': 'LSE',
  '.AS': 'EURONEXT', '.PA': 'EURONEXT', '.BR': 'EURONEXT', '.LS': 'EURONEXT',
  '.IR': 'EURONEXT', '.DE': 'XETR', '.SW': 'SIX', '.MI': 'MIL', '.MC': 'BME',
  '.VI': 'WBAG', '.WA': 'GPW', '.TO': 'TSX', '.V': 'TSXV',
};
const TV_EXCHANGE_MAP = {
  'NYSE': 'NYSE', 'Nasdaq': 'NASDAQ', 'NASDAQ': 'NASDAQ',
  'NYSE Arca': 'NYSE', 'NYSE American': 'AMEX', 'AMEX': 'AMEX',
  'LSE': 'LSE', 'AIM': 'LSE', 'L': 'LSE',
  'Nasdaq Stockholm': 'OMXSTO', 'STO': 'OMXSTO', 'OMXSTO': 'OMXSTO',
  'Nasdaq Copenhagen': 'OMXCOP', 'CPH': 'OMXCOP', 'OMXCOP': 'OMXCOP',
  'Nasdaq Helsinki': 'OMXHEX', 'HEL': 'OMXHEX', 'OMXHEX': 'OMXHEX',
  'Oslo B\u00f8rs': 'OSL', 'Oslo Bors': 'OSL', 'OSL': 'OSL',
  'Nasdaq Iceland': 'OMXICE', 'OMXICE': 'OMXICE',
  'SIX': 'SIX', 'XETRA': 'XETR', 'XETR': 'XETR', 'Frankfurt': 'FWB', 'FWB': 'FWB',
  'Euronext Paris': 'EURONEXT', 'Euronext Amsterdam': 'EURONEXT',
  'Euronext Brussels': 'EURONEXT', 'Euronext Dublin': 'EURONEXT', 'Euronext Lisbon': 'EURONEXT',
  'EURONEXT': 'EURONEXT',
  'Borsa Italiana': 'MIL', 'MIL': 'MIL',
  'BME Madrid': 'BME', 'BME': 'BME',
  'Wiener B\u00f6rse': 'WBAG', 'WBAG': 'WBAG',
  'GPW Warsaw': 'GPW', 'GPW': 'GPW',
  'TSX': 'TSX', 'TSXV': 'TSXV',
};

function tvFromYahoo(yahooSymbol) {
  if (!yahooSymbol) return null;
  const m = String(yahooSymbol).match(/^([A-Z0-9\-]+)(\.[A-Z]+)?$/i);
  if (!m) return null;
  const [, base, suffix] = m;
  const sym = base.replace(/-/g, '_');
  if (!suffix) return `NASDAQ:${sym}`; // bare US ticker — assume Nasdaq (works for most; TV auto-resolves)
  const tvEx = TV_SUFFIX_MAP[suffix.toUpperCase()];
  if (!tvEx) return null;
  return `${tvEx}:${sym}`;
}

function tvSymbol(primaryTicker, yahooSymbol) {
  // Priority 1: primary_ticker with an EXCHANGE:SYMBOL form and known exchange.
  if (primaryTicker && String(primaryTicker).includes(':')) {
    const [exchange, sym] = String(primaryTicker).split(':');
    if (exchange && sym) {
      const tvEx = TV_EXCHANGE_MAP[exchange] || TV_EXCHANGE_MAP[exchange.toUpperCase()];
      if (tvEx) return `${tvEx}:${sym.replace(/-/g, '_')}`;
    }
  }
  // Priority 2: yahoo_symbol with suffix — convert via TV_SUFFIX_MAP.
  const fromYahoo = tvFromYahoo(yahooSymbol || primaryTicker);
  if (fromYahoo) return fromYahoo;
  // Priority 3: fallback — if primary_ticker has no colon (e.g. bare 'BIRD'), return as-is.
  if (primaryTicker && !String(primaryTicker).includes(':')) {
    return `NASDAQ:${String(primaryTicker).replace(/-/g, '_')}`;
  }
  return null;
}

// Render a compact TradingView mini-chart iframe.
function tvMiniChart(tvSym, title) {
  if (!tvSym) return '';
  const cfg = {
    symbol: tvSym,
    width: '100%',
    height: 220,
    dateRange: '12M',
    colorTheme: 'dark',
    isTransparent: true,
    autosize: false,
    trendLineColor: 'rgba(41, 98, 255, 1)',
    underLineColor: 'rgba(41, 98, 255, 0.3)',
    underLineBottomColor: 'rgba(41, 98, 255, 0)',
    locale: 'en',
  };
  // TradingView embed-widget-mini-symbol-overview takes JSON config via <script>.
  // Use srcdoc to fully isolate the widget in its own iframe (no top-window JS pollution).
  const src = `<div class="tradingview-widget-container"><div class="tradingview-widget-container__widget"></div></div><script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-mini-symbol-overview.js" async>${JSON.stringify(cfg)}</script>`;
  const srcdoc = `<html><body style="margin:0;padding:0;background:transparent">${src}</body></html>`;
  return `<div class="chart-wrap">
    <div class="chart-title">${esc(title || tvSym)}</div>
    <iframe class="tv-mini" sandbox="allow-scripts allow-same-origin allow-popups" srcdoc='${srcdoc.replace(/'/g, "&#39;")}' loading="lazy" style="width:100%;height:220px;border:0;border-radius:8px;"></iframe>
  </div>`;
}

// Small pill-link helpers: open the ticker on Yahoo / TradingView / native exchange.
function externalChartLinks(d) {
  if (!d) return '';
  const y = d.yahoo_symbol;
  const tv = tvSymbol(d.primary_ticker, d.yahoo_symbol);
  const links = [];
  if (y)  links.push(`<a class="chip chip-link" href="https://finance.yahoo.com/quote/${encodeURIComponent(y)}" target="_blank" rel="noopener">Yahoo ↗</a>`);
  if (tv) links.push(`<a class="chip chip-link" href="https://www.tradingview.com/symbols/${encodeURIComponent(tv.replace(':', '-'))}/" target="_blank" rel="noopener">TradingView ↗</a>`);
  return links.length ? `<span class="ext-links">• ${links.join(' ')}</span>` : '';
}

// Embed chart(s) for the drawer. For spin-offs, show parent + spinco side-by-side.
//
// GUARD: only render a TradingView embed when we have reasonable confidence
// the symbol actually trades. Pre-listing spin-off tickers (VSNT, VGNT,
// STRG, etc.) fail TV resolution and show the ugly 'Invalid symbol'
// placeholder. Rule of thumb: show a chart only when EITHER
//   - a current_price exists (Yahoo resolved the ticker), OR
//   - status is completed/closed (deal has printed, ticker is live), OR
//   - the chart is for the PARENT side of a spin-off (always live)
// Otherwise fall back to a text link-out to Yahoo/TradingView.
function isChartableTicker(d, kind) {
  // Parent tickers are always live companies.
  if (kind === 'parent') return true;
  // Completed/closed deals: the ticker exists.
  if (['completed', 'closed'].includes(d.status)) return true;
  // Yahoo found it → it trades.
  if (d.current_price != null && d.current_price > 0) return true;
  return false;
}
function renderChartSection(d) {
  const primaryTv = tvSymbol(d.primary_ticker, d.yahoo_symbol);
  const isSpin = (d.deal_type === 'spin_off') || (d.event_type || '').startsWith('spin');
  const charts = [];
  const unchartable = []; // ticker+label pairs we skipped → surface as links
  if (isSpin) {
    if (d.parent_ticker) {
      const pTv = tvSymbol(d.parent_ticker, null);
      if (pTv) charts.push({ sym: pTv, title: `Parent · ${d.parent_name || d.parent_ticker}` });
    }
    if (d.spinco_ticker) {
      const sTv = tvSymbol(d.spinco_ticker, null);
      if (sTv) {
        if (isChartableTicker(d, 'spinco')) {
          charts.push({ sym: sTv, title: `SpinCo · ${d.spinco_name || d.spinco_ticker}` });
        } else {
          unchartable.push({ tv: sTv, ticker: d.spinco_ticker, label: `SpinCo · ${d.spinco_name || d.spinco_ticker}` });
        }
      }
    }
  }
  if (!charts.length && primaryTv) {
    if (isChartableTicker(d, 'primary')) {
      charts.push({ sym: primaryTv, title: d.primary_ticker || primaryTv });
    } else {
      unchartable.push({ tv: primaryTv, ticker: d.primary_ticker, label: d.primary_ticker || primaryTv });
    }
  }
  // If we have neither a chart nor an unchartable link, bail entirely.
  if (!charts.length && !unchartable.length) return '';
  const unchartableHtml = unchartable.length
    ? `<p class="chart-pending">Chart pending first trading day — <span class="chart-pending-links">${
        unchartable.map(u =>
          `<a class="chip chip-link" href="https://www.tradingview.com/symbols/${encodeURIComponent(u.tv.replace(':','-'))}/" target="_blank" rel="noopener">${esc(u.label)} on TradingView ↗</a>`
        ).join(' ')
      }</span></p>`
    : '';
  return `<div class="section chart-section">
    <h3>Price chart</h3>
    ${charts.length ? `<div class="chart-grid${charts.length > 1 ? ' chart-grid-2' : ''}">
      ${charts.map(c => tvMiniChart(c.sym, c.title)).join('')}
    </div>` : ''}
    ${unchartableHtml}
  </div>`;
}

// ---- Unified event timeline ----------------------------------------------
// Merges: filing documents (d.sources), news items (d.news_items), and key
// deal dates (announce_date, filing_date, ex_date, expected_close, completed).
// Sorted newest-first. Each row carries a tier color.
function renderEventTimeline(d, opts) {
  opts = opts || {};
  const groupSameDate = !!opts.groupSameDate;
  const events = [];
  // Build useful fallback URLs that every pseudo-event can fall back to.
  const primaryFilingUrl = d.source_filing_url || null;
  const yahooUrl = d.yahoo_symbol ? `https://finance.yahoo.com/quote/${encodeURIComponent(d.yahoo_symbol)}` : null;
  const yahooHistUrl = d.yahoo_symbol ? `https://finance.yahoo.com/quote/${encodeURIComponent(d.yahoo_symbol)}/history` : null;
  const spincoYahoo = d.spinco_ticker ? `https://finance.yahoo.com/quote/${encodeURIComponent(String(d.spinco_ticker).replace(/^[A-Z]+:/, '').replace(/-/g, '-'))}` : null;

  // Key deal dates as pseudo-events
  if (d.announce_date) events.push({
    date: d.announce_date, tier: 'official', kind: 'announce',
    source: d.announce_date_source || 'announced',
    headline: 'Deal announced',
    summary: `Announce date${d.announce_date_source ? ' — ' + d.announce_date_source.replace(/_/g, ' ') : ''}`,
    url: primaryFilingUrl,
  });
  if (d.filing_date && d.filing_date !== d.announce_date) events.push({
    date: d.filing_date, tier: 'official', kind: 'filing',
    source: d.primary_source || 'filing',
    headline: 'Primary filing',
    summary: null,
    url: primaryFilingUrl,
  });
  const kd = d.key_dates && typeof d.key_dates === 'object' ? d.key_dates : {};
  if (d.ex_date || kd.ex_date) events.push({
    date: d.ex_date || kd.ex_date, tier: 'official', kind: 'ex_date',
    source: 'ex-date',
    headline: 'Ex-date (shares trade without distribution)',
    summary: null,
    url: primaryFilingUrl,
  });
  if (kd.first_trade_date) events.push({
    date: kd.first_trade_date, tier: 'official', kind: 'first_trade',
    source: 'first trade',
    headline: 'First trading day for new entity',
    summary: null,
    url: spincoYahoo || yahooHistUrl,
  });
  if (d.expected_close_date) events.push({
    date: d.expected_close_date, tier: 'upcoming', kind: 'expected_close',
    source: 'expected close',
    headline: 'Expected close date',
    summary: null,
    url: primaryFilingUrl,
  });
  if (d.completed_date) events.push({
    date: d.completed_date, tier: 'official', kind: 'completed',
    source: 'completed',
    headline: d.deal_type === 'spin_off' ? 'Spin-off completed (first trading day)' :
              d.deal_type === 'ipo' ? 'IPO priced / first trading day' : 'Deal completed',
    summary: null,
    url: spincoYahoo || yahooHistUrl,
  });

  // Secondary filings
  if (Array.isArray(d.sources)) {
    for (const s of d.sources) {
      events.push({
        date: s.published_at ? String(s.published_at).slice(0, 10) : null,
        tier: 'official', kind: 'filing',
        source: s.source || 'filing',
        headline: s.headline || '(filing)',
        summary: null,
        url: s.url,
      });
    }
  }

  // News
  if (Array.isArray(d.news_items)) {
    for (const n of d.news_items) {
      events.push({
        date: n.published_at ? String(n.published_at).slice(0, 10) : null,
        tier: 'news', kind: 'news',
        source: n.source || 'news',
        headline: n.headline || '',
        summary: n.summary ? n.summary.slice(0, 200) : null,
        url: n.url,
        matchKind: n.match_kind,
      });
    }
  }

  if (!events.length) return '';

  // Sort newest-first; events without a date sink to the bottom
  events.sort((a, b) => {
    if (!a.date && !b.date) return 0;
    if (!a.date) return 1;
    if (!b.date) return -1;
    return b.date.localeCompare(a.date);
  });

  const tierIcon = { official: '📄', upcoming: '⏳', news: '📰' };
  const tierCls  = { official: 'ev-official', upcoming: 'ev-upcoming', news: 'ev-news' };

  const renderSubEntry = (e) => `
    <div class="event-sub">
      <div class="event-marker">${tierIcon[e.tier] || '•'}</div>
      <div class="event-body">
        <div class="event-meta">
          <span class="event-source">${esc(e.source)}</span>
          ${e.matchKind ? `<span class="event-match mute">via ${esc(e.matchKind.replace('_', ' '))}</span>` : ''}
        </div>
        <div class="event-headline">${esc(e.headline)}</div>
        ${e.summary ? `<div class="event-summary mute">${esc(e.summary)}${e.summary.length >= 200 ? '…' : ''}</div>` : ''}
        ${e.url ? `<a class="event-link" href="${esc(e.url)}" target="_blank" rel="noopener">Open ↗</a>` : ''}
      </div>
    </div>`;

  const renderSingle = (e) => `
    <li class="event-item ${tierCls[e.tier] || ''}">
      <div class="event-marker">${tierIcon[e.tier] || '•'}</div>
      <div class="event-body">
        <div class="event-meta">
          <span class="event-date mono">${e.date ? esc(e.date) : '—'}</span>
          <span class="event-source">${esc(e.source)}</span>
          ${e.matchKind ? `<span class="event-match mute">via ${esc(e.matchKind.replace('_', ' '))}</span>` : ''}
        </div>
        <div class="event-headline">${esc(e.headline)}</div>
        ${e.summary ? `<div class="event-summary mute">${esc(e.summary)}${e.summary.length >= 200 ? '…' : ''}</div>` : ''}
        ${e.url ? `<a class="event-link" href="${esc(e.url)}" target="_blank" rel="noopener">Open ↗</a>` : ''}
      </div>
    </li>`;

  let bodyHtml;
  if (groupSameDate) {
    // Bucket by date (events without date share an empty-key bucket)
    const buckets = [];
    const byDate = new Map();
    for (const e of events) {
      const key = e.date || '__nodate__';
      if (!byDate.has(key)) {
        const b = { date: e.date || null, tier: e.tier, items: [] };
        byDate.set(key, b);
        buckets.push(b);
      }
      const b = byDate.get(key);
      b.items.push(e);
      // Promote tier: official > upcoming > news
      const rank = { official: 3, upcoming: 2, news: 1 };
      if ((rank[e.tier] || 0) > (rank[b.tier] || 0)) b.tier = e.tier;
    }
    bodyHtml = buckets.map(b => {
      if (b.items.length === 1) return renderSingle(b.items[0]);
      return `<li class="event-item tl-date-group ${tierCls[b.tier] || ''}">
        <div class="event-marker">${tierIcon[b.tier] || '•'}</div>
        <div class="event-body">
          <div class="event-meta">
            <span class="event-date mono">${b.date ? esc(b.date) : '—'}</span>
            <span class="event-source">${b.items.length} events on this date</span>
          </div>
          <div class="event-sub-list">
            ${b.items.map(renderSubEntry).join('')}
          </div>
        </div>
      </li>`;
    }).join('');
  } else {
    bodyHtml = events.map(renderSingle).join('');
  }

  return `<div class="section timeline-section">
    <h3>Deal timeline <span class="section-count">${events.length}</span></h3>
    <p class="section-hint">Filings, key dates, and related news merged chronologically. Filings and key dates are source-of-truth; news items are enrichment only.</p>
    <ol class="event-timeline">
      ${bodyHtml}
    </ol>
  </div>`;
}

// News timeline — legacy helper, kept for fallback but no longer used by drawer.
function renderNewsTimeline(items) {
  if (!Array.isArray(items) || !items.length) return '';
  return `
    <div class="section news-timeline-section">
      <h3>News timeline <span class="section-count">${items.length}</span></h3>
      <p class="section-hint">News articles that mention this ticker or issuer name — enrichment only, not source-of-truth.</p>
      <ul class="news-timeline">
        ${items.map(n => `
          <li class="news-item">
            <div class="news-meta">
              <span class="news-source">${esc(n.source || 'news')}</span>
              ${n.published_at ? `<span class="news-date mono">${esc(String(n.published_at).slice(0,10))}</span>` : ''}
              ${n.match_kind ? `<span class="news-match">via ${esc(n.match_kind.replace('_', ' '))}</span>` : ''}
            </div>
            <div class="news-headline">${esc(n.headline || '')}</div>
            ${n.summary ? `<div class="news-summary">${esc((n.summary || '').slice(0, 220))}${n.summary.length > 220 ? '…' : ''}</div>` : ''}
            ${n.url ? `<a class="news-link" href="${esc(n.url)}" target="_blank" rel="noopener">Read ↗</a>` : ''}
          </li>
        `).join('')}
      </ul>
    </div>`;
}

// ---- Admin ----------------------------------------------------------------
async function renderAdmin() {
  if (!state.admin) {
    view.innerHTML = `
      <div class="admin-login">
        <h2>Admin login</h2>
        <input id="admin-pw" type="password" placeholder="Admin password" />
        <button class="btn" id="admin-login-btn">Log in</button>
        <div id="admin-err" style="color:var(--danger);margin-top:8px;font-size:12px"></div>
      </div>`;
    document.getElementById('admin-login-btn').addEventListener('click', async () => {
      const pw = document.getElementById('admin-pw').value;
      try {
        await api('/api/admin/login', { method: 'POST', body: JSON.stringify({ password: pw }) });
        state.admin = pw;
        renderAdmin();
      } catch (e) {
        document.getElementById('admin-err').textContent = e.message;
      }
    });
    return;
  }

  view.innerHTML = `
    <div class="admin-section">
      <h2>Authoritative ingest <span class="pill-primary">Regulator-first</span></h2>
      <p class="admin-hint">Runs SEC EDGAR → LSE/Investegate → Nordic MFN → stockanalysis.com → market data → news linkage. News never creates deals — only enriches existing ones.</p>
      <div class="filters">
        <label class="chk"><input type="checkbox" id="auth-wipe" /> Wipe existing deals first</label>
        <button class="btn" id="run-auth">Run authoritative ingest</button>
        <span id="auth-status" style="color:var(--muted)"></span>
      </div>
    </div>
    <div class="admin-section">
      <h2>Legacy news ingestion</h2>
      <p class="admin-hint">Old news-based pipeline. Retained so existing raw_items keep flowing; the authoritative pipeline will match and attach them as news_items.</p>
      <div class="filters">
        <button class="btn btn-ghost" id="run-ingest">Run legacy news ingest</button>
        <span id="ingest-status" style="color:var(--muted)"></span>
      </div>
    </div>
    <div class="admin-section">
      <h2>Latest raw items (200)</h2>
      <div class="table-wrap">
        <table><thead><tr><th>ID</th><th>Source</th><th>Headline</th><th>Status</th><th>Fetched</th></tr></thead>
        <tbody id="raw-body"><tr><td colspan="5" class="loading">Loading…</td></tr></tbody></table>
      </div>
    </div>
    <div class="admin-section">
      <h2>All deals</h2>
      <div class="table-wrap">
        <table><thead><tr><th>ID</th><th>Type</th><th>Headline</th><th>Status</th><th>Actions</th></tr></thead>
        <tbody id="admin-deals-body"><tr><td colspan="5" class="loading">Loading…</td></tr></tbody></table>
      </div>
    </div>
  `;

  document.getElementById('run-ingest').addEventListener('click', async () => {
    const btn = document.getElementById('run-ingest');
    const status = document.getElementById('ingest-status');
    btn.disabled = true; status.textContent = 'Running… this can take a few minutes.';
    try {
      const res = await fetch('/api/ingest/run', { method: 'POST', headers: { 'x-admin-password': state.admin } });
      if (res.status === 401) {
        const token = prompt('Ingest token (from env)');
        if (!token) { btn.disabled = false; status.textContent = ''; return; }
        const res2 = await fetch('/api/ingest/run', { method: 'POST', headers: { 'x-ingest-token': token } });
        const j2 = await res2.json();
        status.textContent = res2.ok ? `Done — fetched ${j2.fetched}, classified ${j2.classified}` : `Error: ${j2.error || res2.statusText}`;
      } else {
        const j = await res.json();
        status.textContent = res.ok ? `Done — fetched ${j.fetched}, classified ${j.classified}` : `Error: ${j.error || res.statusText}`;
      }
      loadAdminTables();
    } finally { btn.disabled = false; }
  });

  document.getElementById('run-auth').addEventListener('click', async () => {
    const btn = document.getElementById('run-auth');
    const status = document.getElementById('auth-status');
    const wipe = document.getElementById('auth-wipe').checked;
    if (wipe && !confirm('Wipe ALL existing deals before re-ingesting? This cannot be undone.')) return;
    btn.disabled = true;
    status.textContent = 'Queued — running in background. Refresh stats in 2-5 minutes.';
    try {
      const res = await fetch('/api/admin/run-auth-ingest?async=1', {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-admin-password': state.admin },
        body: JSON.stringify({ wipe }),
      });
      const j = await res.json();
      status.textContent = res.ok ? `Started at ${j.started}. Watch server logs for progress.` : `Error: ${j.error || res.statusText}`;
    } catch (e) {
      status.textContent = `Error: ${e.message}`;
    } finally { btn.disabled = false; }
  });

  loadAdminTables();
}

async function loadAdminTables() {
  try {
    const [raw, deals] = await Promise.all([
      fetch('/api/admin/raw', { headers: { 'x-admin-password': state.admin } }).then(r => r.json()),
      api('/api/deals'),
    ]);
    document.getElementById('raw-body').innerHTML = raw.map(r => `
      <tr><td class="mono">${r.id}</td><td>${esc(r.source)}</td>
      <td>${esc((r.headline || '').slice(0, 120))}</td>
      <td><span class="badge badge-region">${esc(r.status)}</span></td>
      <td class="mono">${esc(String(r.fetched_at || '').slice(0,16))}</td></tr>
    `).join('') || `<tr><td colspan="5" class="empty">No raw items yet</td></tr>`;

    document.getElementById('admin-deals-body').innerHTML = deals.map(d => `
      <tr>
        <td class="mono">${d.id}</td>
        <td><span class="badge badge-type-${d.deal_type}">${labelType(d.deal_type)}</span></td>
        <td>${esc((d.headline || '').slice(0, 120))}</td>
        <td><span class="badge badge-status-${d.status || 'announced'}">${cap(d.status || 'announced')}</span></td>
        <td class="inline-actions">
          <button class="btn btn-ghost" data-act="view" data-id="${d.id}">View</button>
          <button class="btn btn-ghost" data-act="delete" data-id="${d.id}" style="color:var(--danger)">Delete</button>
        </td>
      </tr>
    `).join('') || `<tr><td colspan="5" class="empty">No deals yet</td></tr>`;

    document.querySelectorAll('#admin-deals-body button').forEach(b => {
      b.addEventListener('click', async () => {
        const id = b.dataset.id;
        if (b.dataset.act === 'view') { location.hash = '#/'; setTimeout(() => openDrawer(id), 50); }
        if (b.dataset.act === 'delete' && confirm('Delete this deal?')) {
          await fetch(`/api/admin/deals/${id}`, { method: 'DELETE', headers: { 'x-admin-password': state.admin } });
          loadAdminTables();
        }
      });
    });
  } catch (e) {
    console.error(e);
  }
}

// ---- Utils ----------------------------------------------------------------
function esc(s) { return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function cap(s) { return s ? s.charAt(0).toUpperCase() + s.slice(1).replace(/_/g,' ') : ''; }
function labelType(t) { return DEAL_TYPES.find(([v]) => v === t)?.[1] || cap(t); }
function dealParties(d) {
  if (d.deal_type === 'merger_arb' && (d.acquirer_name || d.target_name))
    return `${d.acquirer_name || '?'} → ${d.target_name || '?'}`;
  if (d.deal_type === 'spin_off' && (d.parent_name || d.spinco_name))
    return `${d.parent_name || '?'} spins ${d.spinco_name || '?'}`;
  return d.target_name || d.acquirer_name || d.spinco_name || '';
}
function tickers(d) {
  if (d.deal_type === 'merger_arb')
    return `${d.acquirer_ticker || '?'}<span class="arrow">→</span>${d.target_ticker || '?'}`;
  if (d.deal_type === 'spin_off')
    return `${d.parent_ticker || '?'}<span class="arrow">+</span>${d.spinco_ticker || '?'}`;
  return d.target_ticker || d.acquirer_ticker || d.spinco_ticker || '—';
}
function primaryTickerCell(d) {
  const primary = d.primary_ticker || d.target_ticker || d.acquirer_ticker || d.spinco_ticker;
  if (!primary) return '—';
  if (d.deal_type === 'merger_arb' && d.acquirer_ticker && d.target_ticker) {
    return `<div class="ticker-primary">${esc(primary)}</div><div class="ticker-counter">← ${esc(d.acquirer_ticker)}</div>`;
  }
  if (d.deal_type === 'spin_off' && d.spinco_ticker && d.parent_ticker !== d.spinco_ticker) {
    return `<div class="ticker-primary">${esc(primary)}</div><div class="ticker-counter">+ ${esc(d.spinco_ticker)}</div>`;
  }
  return `<div class="ticker-primary">${esc(primary)}</div>`;
}
function skinCell(d) {
  const badges = Array.isArray(d.incentive_badges) ? d.incentive_badges : [];
  if (!badges.length) return '<span style="color:var(--muted)">—</span>';
  return `<div class="skin-badges">${badges.map(b =>
    `<span class="skin-badge" title="${esc(b.tooltip || b.label || '')}">${b.icon || '•'}</span>`
  ).join('')}</div>`;
}
function fmtUsdCompact(n) {
  if (n == null || !isFinite(n)) return '—';
  const v = Math.abs(Number(n));
  const sign = Number(n) < 0 ? '-' : '';
  if (v >= 1e9) return `${sign}$${(v/1e9).toFixed(1)}B`;
  if (v >= 1e6) return `${sign}$${(v/1e6).toFixed(1)}M`;
  if (v >= 1e3) return `${sign}$${(v/1e3).toFixed(0)}k`;
  return `${sign}$${v.toFixed(0)}`;
}
const COUNTRY_FLAG = { US:'🇺🇸', GB:'🇬🇧', DE:'🇩🇪', FR:'🇫🇷', NL:'🇳🇱', CH:'🇨🇭',
  SE:'🇸🇪', DK:'🇩🇰', NO:'🇳🇴', FI:'🇫🇮', IT:'🇮🇹', ES:'🇪🇸',
  BE:'🇧🇪', IE:'🇮🇪', PT:'🇵🇹', AT:'🇦🇹', PL:'🇵🇱', IS:'🇮🇸' };
function countryBadge(d) {
  if (d.country) {
    const flag = COUNTRY_FLAG[d.country] || '';
    return `<span class="badge badge-country">${flag} ${esc(d.country)}</span>`;
  }
  return d.region ? `<span class="badge badge-region">${esc(d.region)}</span>` : '';
}
function fmtM(n) { return (Number(n) / 1_000_000).toLocaleString(undefined, { maximumFractionDigits: 0 }); }
function fmtMcap(n) {
  if (n == null) return '—';
  const v = Number(n);
  if (v >= 1e12) return `$${(v/1e12).toFixed(1)}T`;
  if (v >= 1e9)  return `$${(v/1e9).toFixed(1)}B`;
  if (v >= 1e6)  return `$${(v/1e6).toFixed(0)}M`;
  return `$${v.toFixed(0)}`;
}
function fmtReturn(r) {
  if (r == null) return '—';
  const n = typeof r === 'number' ? r : Number(r);
  if (!isFinite(n)) return '—';
  const sign = n >= 0 ? '+' : '';
  return `${sign}${n.toFixed(1)}%`;
}
function returnClass(r) {
  if (r == null) return '';
  const n = typeof r === 'number' ? r : Number(r);
  if (!isFinite(n)) return '';
  return n >= 0 ? 'ret-pos' : 'ret-neg';
}
// Renders the Return cell. For spin-offs with split parent/spinco returns, shows
// compact "P +4.2% / S +18.7%" with color coding on each leg. Falls back to
// single return_pct otherwise.
function returnCell(d) {
  const isSpin = (d.deal_type === 'spin_off') || (d.event_type && d.event_type.startsWith('spin'));
  const isMerger = (d.deal_type === 'merger_arb') || (d.event_type && d.event_type.startsWith('merger'));

  if (isSpin) {
    const pr = d.parent_return_pct, sr = d.spinco_return_pct;
    if (pr != null || sr != null) {
      const p = pr != null ? `<span class="${returnClass(pr)}" title="Parent (RemainCo) since ex-date">P ${fmtReturn(pr)}</span>` : '<span class="mute">P —</span>';
      const s = sr != null ? `<span class="${returnClass(sr)}" title="SpinCo since first trade">S ${fmtReturn(sr)}</span>` : '<span class="mute">S —</span>';
      return `${p} <span class="mute">/</span> ${s}`;
    }
  }

  if (isMerger) {
    // For mergers, return_pct is misleading (stale announce_price, no offer context).
    // Show spread-to-deal instead: positive = trading below offer (arb opportunity).
    const sp = d.spread_to_deal_pct;
    if (sp != null) {
      return `<span class="${sp >= 0 ? 'ret-pos' : 'ret-neg'}" title="Spread to deal: (offer − current) / current">${fmtReturn(sp)} spr</span>`;
    }
    // No offer price captured yet — suppress misleading return.
    return '<span class="mute" title="No offer price yet — spread unavailable">—</span>';
  }

  return `<span class="${returnClass(d.return_pct)}">${fmtReturn(d.return_pct)}</span>`;
}
function debounce(fn, ms) { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; }

// ---- boot -----------------------------------------------------------------
route();
