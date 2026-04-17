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
const REGIONS = ['US', 'UK', 'EU', 'Nordic', 'Switzerland', 'Global'];

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
    <div class="filters">
      <input id="f-q" type="search" placeholder="Search tickers, companies, headlines…" />
      <select id="f-type"><option value="">All types</option>${DEAL_TYPES.map(([v,l]) => `<option value="${v}">${l}</option>`).join('')}</select>
      <select id="f-status"><option value="">All statuses</option>${STATUSES.map(s => `<option value="${s}">${cap(s)}</option>`).join('')}</select>
      <select id="f-region"><option value="">All regions</option>${REGIONS.map(r => `<option value="${r}">${r}</option>`).join('')}</select>
      <select id="f-country"><option value="">All countries</option>${COUNTRIES.map(([v,l]) => `<option value="${v}">${l}</option>`).join('')}</select>
      <select id="f-mcap"><option value="">Any market cap</option>${MCAP_BUCKETS.map(([v,l]) => `<option value="${v}">${l}</option>`).join('')}</select>
      <select id="f-dsize"><option value="">Any deal size</option>${DEAL_SIZE_BUCKETS.map(([v,l]) => `<option value="${v}">${l}</option>`).join('')}</select>
      <span class="spacer"></span>
      <button class="btn-ghost btn" id="f-reset">Reset</button>
    </div>
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th>Type</th>
          <th>Status</th>
          <th class="hide-sm">Country</th>
          <th>Headline / parties</th>
          <th>Ticker</th>
          <th class="td-right hide-sm">Mkt cap</th>
          <th class="td-right hide-sm">Deal ($M)</th>
          <th class="td-right">Return</th>
          <th class="hide-sm">Announced</th>
        </tr></thead>
        <tbody id="deals-body"><tr><td colspan="9" class="loading">Loading deals…</td></tr></tbody>
      </table>
    </div>
    <div class="drawer-backdrop" id="drawer-backdrop"></div>
    <div class="drawer" id="drawer"></div>
  `;

  document.getElementById('f-q').addEventListener('input', debounce(applyFilters, 250));
  ['f-type','f-status','f-region','f-country','f-mcap','f-dsize'].forEach(id =>
    document.getElementById(id).addEventListener('change', applyFilters));
  document.getElementById('f-reset').addEventListener('click', () => {
    ['f-q','f-type','f-status','f-region','f-country','f-mcap','f-dsize']
      .forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
    applyFilters();
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
      ${kpi('Active', fmt(s.active))}
      ${kpi('Merger arb', fmt(s.merger_arb))}
      ${kpi('Spin-offs', fmt(s.spin_off))}
      ${kpi('IPOs', fmt(s.ipo))}
      ${kpi('SPACs', fmt(s.spac))}
      ${kpi('Pending items', fmt(s.pending_items))}
    `;
  } catch (e) {
    document.getElementById('kpis').innerHTML = `<div class="kpi">Could not load stats</div>`;
  }
}
function kpi(label, value) {
  return `<div class="kpi"><div class="kpi-label">${label}</div><div class="kpi-value">${value}</div></div>`;
}
function skeletonKpis() {
  return Array(7).fill(0).map(() =>
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
      <h3>No deals yet</h3>
      <div>Run the ingestion cycle or forward an alert to your inbox to populate deals.</div>
    </td></tr>`;
    return;
  }
  tbody.innerHTML = state.deals.map(d => `
    <tr data-id="${d.id}">
      <td><span class="badge badge-type-${d.deal_type}">${labelType(d.deal_type)}</span></td>
      <td><span class="badge badge-status-${d.status || 'announced'}">${cap(d.status || 'announced')}</span></td>
      <td class="hide-sm">${countryBadge(d)}</td>
      <td class="td-headline">
        <div>${esc(d.headline || '')}</div>
        <div class="sub">${esc(dealParties(d))}</div>
      </td>
      <td class="td-tickers">${primaryTickerCell(d)}</td>
      <td class="td-right hide-sm mono">${fmtMcap(d.market_cap_usd)}</td>
      <td class="td-right hide-sm mono">${d.deal_value_usd ? fmtM(d.deal_value_usd) : '—'}</td>
      <td class="td-right mono ${returnClass(d.return_pct)}">${fmtReturn(d.return_pct)}</td>
      <td class="hide-sm mono">${d.announce_date || '—'}</td>
    </tr>
  `).join('');
  tbody.querySelectorAll('tr').forEach(tr => {
    tr.addEventListener('click', () => openDrawer(tr.dataset.id));
  });
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
    const d = await api(`/api/deals/${id}`);
    drawer.innerHTML = `
      <div class="drawer-head">
        <h2>${esc(d.headline || '')}</h2>
        <button class="drawer-close" aria-label="Close">&times;</button>
      </div>
      <div class="drawer-body">
        <div class="drawer-meta">
          <span class="badge badge-type-${d.deal_type}">${labelType(d.deal_type)}</span>
          <span class="badge badge-status-${d.status || 'announced'}">${cap(d.status || 'announced')}</span>
          ${d.region ? `<span class="badge badge-region">${esc(d.region)}</span>` : ''}
        </div>

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
            ${row('Return since announce', (() => { const ap = d.announce_price, cp = d.current_price; if (ap == null || cp == null) return null; const r = ((cp-ap)/ap)*100; return fmtReturn(r); })())}
            ${row('Refreshed', d.market_refreshed_at ? String(d.market_refreshed_at).slice(0,16) : null)}
          </dl>
        </div>

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
            ${row('Announced', d.announce_date)}
            ${row('Expected close', d.expected_close_date)}
            ${row('Record date', d.record_date)}
            ${row('Ex-date', d.ex_date)}
            ${row('First seen', d.first_seen_at)}
            ${row('Last updated', d.updated_at)}
          </dl>
        </div>

        <div class="section">
          <h3>Sources (${d.sources?.length || 0})</h3>
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
      <h2>Ingestion</h2>
      <div class="filters">
        <button class="btn" id="run-ingest">Run ingestion now</button>
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
      // NOTE: ingestion uses the INGEST_TOKEN, not admin password. Admin UI asks for it.
      const token = prompt('Ingest token (from env)');
      if (!token) { btn.disabled = false; status.textContent = ''; return; }
      const res = await fetch('/api/ingest/run', { method: 'POST', headers: { 'x-ingest-token': token } });
      const j = await res.json();
      status.textContent = res.ok
        ? `Done — fetched ${j.fetched}, inserted ${j.inserted}, classified ${j.classified}, promoted ${j.promoted}`
        : `Error: ${j.error || res.statusText}`;
      loadAdminTables();
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
  if (r == null || !isFinite(r)) return '—';
  const sign = r >= 0 ? '+' : '';
  return `${sign}${r.toFixed(1)}%`;
}
function returnClass(r) {
  if (r == null || !isFinite(r)) return '';
  return r >= 0 ? 'ret-pos' : 'ret-neg';
}
function debounce(fn, ms) { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; }

// ---- boot -----------------------------------------------------------------
route();
