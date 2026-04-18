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
const REGIONS = ['US', 'UK', 'Europe', 'Nordic', 'Switzerland', 'Global'];
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
  ['f-type','f-status','f-region','f-country','f-mcap','f-dsize','f-insider','f-event','f-tier'].forEach(id =>
    document.getElementById(id).addEventListener('change', applyFilters));
  document.getElementById('f-reset').addEventListener('click', () => {
    ['f-q','f-type','f-status','f-region','f-country','f-mcap','f-dsize','f-insider','f-event','f-tier']
      .forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
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
    timeframe: state.filters.timeframe || '',
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
    const [d, inc] = await Promise.all([
      api(`/api/deals/${id}`),
      api(`/api/deals/${id}/incentives`).catch(() => null),
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
        ${d.source_filing_url ? `<div class="primary-source"><span class="src-label">Primary source:</span> <a href="${esc(d.source_filing_url)}" target="_blank" rel="noopener">${esc(d.primary_source || 'Filing')} ↗</a></div>` : ''}

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
              // Helper: emit a <dt>/<dd> with raw HTML (bypasses esc() in row())
              const rawRow = (label, html) => `<dt>${label}</dt><dd>${html != null ? html : '—'}</dd>`;
              if (isSpin && (d.parent_return_pct != null || d.spinco_return_pct != null)) {
                const pr = d.parent_return_pct, sr = d.spinco_return_pct;
                const pLabel = `Parent return <span class="kv-hint" title="RemainCo performance since ex-date">ⓘ</span>`;
                const sLabel = `SpinCo return <span class="kv-hint" title="New entity performance since first trade">ⓘ</span>`;
                return rawRow(pLabel, pr != null ? `<span class="${returnClass(pr)}">${fmtReturn(pr)}</span>` : null)
                     + rawRow(sLabel, sr != null ? `<span class="${returnClass(sr)}">${fmtReturn(sr)}</span>` : null);
              }
              const ap = d.announce_price, cp = d.current_price;
              if (ap == null || cp == null) return '';
              const r = ((cp-ap)/ap)*100;
              return rawRow('Return since announce', `<span class="${returnClass(r)}">${fmtReturn(r)}</span>`);
            })()}
            ${row('Refreshed', d.market_refreshed_at ? String(d.market_refreshed_at).slice(0,16) : null)}
          </dl>
        </div>

        ${renderIncentiveSection(inc)}

        <div class="section">
          <h3>Deal terms</h3>
          <dl class="kv">
            ${row('Consideration', d.consideration)}
            ${row('Offer price', d.offer_price)}
            ${row('Deal value (USD)', d.deal_value_usd ? fmtM(d.deal_value_usd) + 'M' : null)}
            ${row('Current spread', d.spread_pct != null ? d.spread_pct + '%' : null)}
          </dl>
        </div>

        <div class="section">
          <h3>Parties</h3>
          <dl class="kv">
            ${row('Acquirer', nameTicker(d.acquirer_name, d.acquirer_ticker))}
            ${row('Target', nameTicker(d.target_name, d.target_ticker))}
            ${row('Parent', nameTicker(d.parent_name, d.parent_ticker))}
            ${row('SpinCo', nameTicker(d.spinco_name, d.spinco_ticker))}
          </dl>
        </div>

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

        ${renderNewsTimeline(d.news_items)}

        <div class="section">
          <h3>Other source documents (${d.sources?.length || 0})</h3>
          ${d.sources?.length ? `<ul class="sources-list">${d.sources.map(s => `
            <li>
              <div class="src-source">${esc(s.source)} · ${s.published_at ? esc(String(s.published_at).slice(0,16)) : ''}</div>
              <div>${esc(s.headline || '')}</div>
              ${s.url ? `<div style="margin-top:4px"><a href="${esc(s.url)}" target="_blank" rel="noopener">Open source ↗</a></div>` : ''}
            </li>`).join('')}</ul>` : '<p style="color:var(--muted)">No source documents.</p>'}
        </div>
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

// News timeline — matched news_items attached to this deal by the orchestrator
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
  const pr = d.parent_return_pct, sr = d.spinco_return_pct;
  if (isSpin && (pr != null || sr != null)) {
    const p = pr != null ? `<span class="${returnClass(pr)}" title="Parent (RemainCo) since ex-date">P ${fmtReturn(pr)}</span>` : '<span class="mute">P —</span>';
    const s = sr != null ? `<span class="${returnClass(sr)}" title="SpinCo since first trade">S ${fmtReturn(sr)}</span>` : '<span class="mute">S —</span>';
    return `${p} <span class="mute">/</span> ${s}`;
  }
  return `<span class="${returnClass(d.return_pct)}">${fmtReturn(d.return_pct)}</span>`;
}
function debounce(fn, ms) { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; }

// ---- boot -----------------------------------------------------------------
route();
