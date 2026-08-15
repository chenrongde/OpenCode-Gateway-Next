const APP_VERSION = '1.0.5';
const RELEASE_REPO = 'choateyang/OpenCode-Gateway-Next';
const token = sessionStorage.getItem('control-token') || prompt('输入 ADMIN_TOKEN');
if (token) sessionStorage.setItem('control-token', token);
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
const api = async (path, options = {}) => {
  const response = await fetch('/api' + path, { ...options, headers: { Authorization: `Bearer ${sessionStorage.getItem('control-token') || ''}`, ...(options.headers || {}) } });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.detail ? `${data.error || `HTTP ${response.status}`}: ${data.detail}` : data.error || `HTTP ${response.status}`);
  return data;
};
const displayTime = value => value ? new Date(value).toLocaleTimeString('zh-CN') : '-';
let state = {};
let mihomoPage = 0;
let selectedLogSource = 'control';
const mihomoPageSize = 10;
const PAGE_CONFIG = {
  instances: { title: '实例与出口', eyebrow: 'GATEWAY FLEET / OPERATIONS', subtitle: '集中查看网关实例、当前出口和流量池状态。' },
  mihomo: { title: 'Mihomo 协议转换', eyebrow: 'PROXY CONVERSION / MIHOMO', subtitle: '管理订阅节点、健康状态和可分配的 SOCKS5 出口。' },
  keys: { title: '访问密钥', eyebrow: 'ACCESS CONTROL / API KEYS', subtitle: '统一维护客户端访问网关所需的 API 密钥。' },
  logs: { title: '审计与日志', eyebrow: 'OBSERVABILITY / ACTIVITY', subtitle: '按控制面、Mihomo 和各网关实例查看运行事件。' },
  tokens: { title: 'API Token 统计', eyebrow: 'USAGE ANALYTICS / TOKENS', subtitle: '按脱敏调用密钥、模型与实例统计 API 响应中的 Token 用量。' },
};

function currentPage() {
  const name = window.location.pathname.split('/').filter(Boolean).pop() || 'instances';
  return PAGE_CONFIG[name] ? name : 'instances';
}

function initPage() {
  const page = currentPage();
  const config = PAGE_CONFIG[page];
  document.querySelectorAll('[data-page]').forEach(panel => { panel.hidden = panel.dataset.page !== page; });
  document.querySelector('.metrics').hidden = page === 'tokens';
  document.querySelectorAll('[data-page-link]').forEach(link => { link.classList.toggle('active', link.dataset.pageLink === page); });
  $('pageTitle').textContent = config.title;
  $('pageEyebrow').textContent = config.eyebrow;
  $('pageSubtitle').textContent = config.subtitle;
}

function normalizeVersion(value) {
  return String(value || '').replace(/^v/i, '').split('.').map(part => Number.parseInt(part, 10) || 0);
}

function isNewerVersion(remote, local) {
  const a = normalizeVersion(remote);
  const b = normalizeVersion(local);
  for (let index = 0; index < Math.max(a.length, b.length); index += 1) {
    if ((a[index] || 0) !== (b[index] || 0)) return (a[index] || 0) > (b[index] || 0);
  }
  return false;
}

function setVersionStatus(text, state = '') {
  const element = $('versionStatus');
  if (!element) return;
  element.textContent = text;
  element.className = `version-status${state ? ` ${state}` : ''}`;
}

async function checkForUpdate() {
  setVersionStatus(`v${APP_VERSION} · 检查更新中`);
  try {
    let release = null;
    const releaseResponse = await fetch(`https://api.github.com/repos/${RELEASE_REPO}/releases/latest`, { headers: { Accept: 'application/vnd.github+json' } });
    if (releaseResponse.ok) release = await releaseResponse.json();
    let remoteVersion = release?.tag_name || '';
    let releaseURL = release?.html_url || `https://github.com/${RELEASE_REPO}/releases`;
    if (!remoteVersion) {
      const tagsResponse = await fetch(`https://api.github.com/repos/${RELEASE_REPO}/tags`, { headers: { Accept: 'application/vnd.github+json' } });
      if (tagsResponse.ok) {
        const tags = await tagsResponse.json();
        remoteVersion = tags?.[0]?.name || '';
        releaseURL = `https://github.com/${RELEASE_REPO}/tags`;
      }
    }
    if (!remoteVersion) throw new Error('no version');
    if (isNewerVersion(remoteVersion, APP_VERSION)) {
      setVersionStatus(`发现新版本 ${remoteVersion}`, 'update');
      const element = $('versionStatus');
      element.title = '打开 GitHub 查看更新';
      element.style.cursor = 'pointer';
      element.onclick = () => window.open(releaseURL, '_blank', 'noopener');
    } else {
      setVersionStatus(`v${APP_VERSION} · 已是最新`);
    }
  } catch (_) {
    setVersionStatus(`v${APP_VERSION} · 更新检查不可用`, 'error');
  }
}

function initTheme() {
  const saved = localStorage.getItem('gateway-theme');
  const dark = saved ? saved === 'dark' : window.matchMedia?.('(prefers-color-scheme: dark)').matches;
  document.documentElement.dataset.theme = dark ? 'dark' : 'light';
  const button = $('themeToggle');
  if (!button) return;
  const update = () => {
    const isDark = document.documentElement.dataset.theme === 'dark';
    button.querySelector('.theme-icon').textContent = isDark ? '☀' : '☾';
    button.querySelector('.theme-label').textContent = isDark ? '浅色模式' : '深色模式';
    button.title = isDark ? '切换到浅色模式' : '切换到深色模式';
  };
  update();
  button.onclick = () => {
    const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
    document.documentElement.dataset.theme = next;
    localStorage.setItem('gateway-theme', next);
    update();
  };
}

function endpointHealthy(endpoint) {
  return endpoint.healthy === true || endpoint.alive === true;
}

function formProxyValues(form) {
  return form.elements.proxy_urls.value.split(/\r?\n|,/).map(value => value.trim()).filter(Boolean);
}

function renderMihomoChoices() {
  const endpoints = state.mihomoEndpoints || [];
  ['createMihomoChoices', 'settingsMihomoChoices'].forEach(id => {
    const container = $(id);
    if (!container) return;
    const form = id === 'createMihomoChoices' ? $('createForm') : $('settingsForm');
    const selected = new Set(formProxyValues(form));
    container.innerHTML = endpoints.length ? endpoints.map((endpoint, index) => {
      const healthy = endpointHealthy(endpoint);
      const port = 10801 + index;
      return `<label class="mihomo-choice ${healthy ? 'healthy' : 'unhealthy'}"><input type="checkbox" value="${esc(endpoint.url)}" ${healthy ? '' : 'disabled'} ${selected.has(endpoint.url) ? 'checked' : ''}><span class="health-dot" aria-hidden="true"></span><span class="choice-port">${port}</span><code>${esc(endpoint.url)}</code><span class="choice-state">${healthy ? '可用' : '不可用'}</span></label>`;
    }).join('') : '<span class="muted">尚未生成 Mihomo 出口</span>';
  });
}

function syncMihomoChoiceChecks(form) {
  const selected = new Set(formProxyValues(form));
  form.querySelectorAll('.mihomo-choice input').forEach(input => { input.checked = selected.has(input.value); });
}

function selectedProxyValues(form) {
  const values = formProxyValues(form);
  form.querySelectorAll('.mihomo-choice input:checked').forEach(input => values.push(input.value));
  return [...new Set(values)];
}

function syncAuthMode(form) {
  const custom = form.elements.auth_mode.value === 'custom';
  form.querySelectorAll('[data-custom-key]').forEach(label => { label.hidden = !custom; });
  form.elements.zen_api_key.required = custom;
}

function actions(instance) {
  const name = esc(instance.instance);
  const running = instance.status === 'running';
  const rotatable = running && (instance.proxy_urls || []).some(value => /^socks5h?:\/\/(?:mihomo|opencode-gateway-mihomo):108\d+$/.test(value));
  return `<div class="row-actions"><button class="icon-button instance-settings" data-name="${name}" title="设置">⚙</button>${rotatable ? `<button class="icon-button instance-action" data-name="${name}" data-action="rotate" title="更换出口 IP">⇄</button>` : ''}${running ? `<button class="icon-button instance-action" data-name="${name}" data-action="stop" title="停止">Ⅱ</button>` : `<button class="icon-button instance-action" data-name="${name}" data-action="start" title="启动">▶</button>`}<button class="icon-button instance-action" data-name="${name}" data-action="restart" title="重启">↻</button><button class="icon-button danger instance-delete" data-name="${name}" title="删除">×</button></div>`;
}

function activeSlots(instance) {
  const slots = instance.slots || [];
  const active = slots.filter(slot => slot.active && slot.enabled !== false && slot.healthy !== false);
  if (active.length) return active;
  const healthy = slots.filter(slot => slot.enabled !== false && slot.healthy !== false);
  return healthy.length ? healthy.slice(0, 1) : slots.slice(0, 1);
}

function renderMihomoEndpoints() {
  const endpoints = state.mihomoEndpoints || [];
  const pages = Math.max(1, Math.ceil(endpoints.length / mihomoPageSize));
  mihomoPage = Math.min(Math.max(0, mihomoPage), pages - 1);
  const visible = endpoints.slice(mihomoPage * mihomoPageSize, (mihomoPage + 1) * mihomoPageSize);
  $('mihomoProxy').innerHTML = visible.length ? visible.map(endpoint => { const healthy = endpointHealthy(endpoint); return `<div class="proxy-endpoint"><div class="proxy-line"><code>${esc(endpoint.url)}</code><span class="badge ${healthy ? 'ok' : 'bad'}">${healthy ? '健康' : '不可用'}</span></div><span title="${esc(endpoint.active_node || endpoint.node || '')}">${esc(endpoint.active_node || endpoint.node || '节点状态未知')}${endpoint.type ? ` · ${esc(endpoint.type)}` : ''}</span></div>`; }).join('') : '<span class="muted">尚未生成代理入口</span>';
  $('mihomoPage').textContent = endpoints.length ? `${mihomoPage + 1} / ${pages} · 共 ${endpoints.length} 个` : '0 / 0';
  $('mihomoPrev').disabled = mihomoPage === 0;
  $('mihomoNext').disabled = mihomoPage >= pages - 1;
  renderMihomoChoices();
}

function renderLogs() {
  const sources = state.log_sources || [];
  if (!sources.some(source => source.id === selectedLogSource)) selectedLogSource = sources[0]?.id || 'control';
  $('logTabs').innerHTML = sources.map(source => `<button class="log-source ${source.id === selectedLogSource ? 'active' : ''}" data-source="${esc(source.id)}">${esc(source.label)}<span>${(source.entries || []).length}</span></button>`).join('');
  const selected = sources.find(source => source.id === selectedLogSource);
  const entries = (selected?.entries || []).slice(-120).reverse();
  $('logs').innerHTML = entries.map(entry => {
    if (entry.kind === 'audit') {
      return `<div class="log audit"><b class="status-${Number(entry.status) >= 400 ? 'bad' : 'ok'}">${Number(entry.status) || 0}</b><span class="mono">${esc(entry.method)} ${esc(entry.path)}${entry.model ? ` · ${esc(entry.model)}` : ''}</span><span>${esc(entry.egress || '出口未知')} · ${esc(entry.source || 'upstream')} · 第${Number(entry.attempts) || 1}次</span><time>${Number(entry.latency_ms) || 0}ms · ${esc(displayTime(entry.at))}</time></div>`;
    }
    return `<div class="log"><b class="level-${esc(entry.level || 'info')}">${esc(entry.level || entry.kind || 'info')}</b><span class="mono log-message" title="${esc(entry.message || '')}">${esc(entry.message || '-')}</span><time>${esc(displayTime(entry.at))}</time></div>`;
  }).join('') || '<p class="empty-state">该分类暂无日志</p>';
}

const formatNumber = value => new Intl.NumberFormat('zh-CN').format(Number(value) || 0);
const formatUSD = value => `$${(Number(value) || 0).toFixed(6)}`;

function outputSpeed(record) {
  if (!record.stream || !Number(record.completion_tokens) || !Number(record.first_token_ms)) return '';
  const generationMS = Number(record.latency_ms) - Number(record.first_token_ms);
  if (generationMS <= 0) return '';
  return `${(Number(record.completion_tokens) / (generationMS / 1000)).toFixed(1)} t/s`;
}

function costDetails(record) {
  const total = formatUSD(record.total_cost_usd);
  return `<details class="cost-details"><summary>${total}</summary><div class="cost-popover"><strong>费用明细</strong><div><span>输入费用</span><b>${formatUSD(record.input_cost_usd)}</b></div><div><span>输出费用</span><b>${formatUSD(record.output_cost_usd)}</b></div><div><span>缓存读取费用</span><b>${formatUSD(record.cache_cost_usd)}</b></div><hr><div><span>总费用</span><b>${total}</b></div><small>USD · DeepSeek V4 Flash 按每 1M Tokens 计</small></div></details>`;
}

function syncTokenFilters(records) {
  const selections = [
    ['tokenInstance', records.map(record => record.instance)],
    ['tokenModel', records.map(record => record.model)],
    ['tokenKey', records.map(record => record.client_key)],
  ];
  selections.forEach(([id, values]) => {
    const element = $(id);
    const selected = element.value;
    const label = id === 'tokenInstance' ? '全部实例' : id === 'tokenModel' ? '全部模型' : '全部密钥';
    const options = [...new Set(values.filter(Boolean))].sort();
    element.innerHTML = `<option value="">${label}</option>${options.map(value => `<option value="${esc(value)}">${esc(value)}</option>`).join('')}`;
    element.value = options.includes(selected) ? selected : '';
  });
}

function tokenQuery() {
  const params = new URLSearchParams();
  [['instance', 'tokenInstance'], ['model', 'tokenModel'], ['key', 'tokenKey'], ['status', 'tokenStatus']].forEach(([name, id]) => {
    const value = $(id).value;
    if (value) params.set(name, value);
  });
  const value = params.toString();
  return value ? `/tokens?${value}` : '/tokens';
}

function renderTokens(data) {
  const summary = data.summary || {};
  const records = data.records || [];
  syncTokenFilters(records);
  $('tokenRequests').textContent = formatNumber(summary.requests);
  $('tokenSuccess').textContent = `成功 ${formatNumber(summary.success)} · 异常 ${formatNumber(summary.errors)}`;
  $('tokenTotal').textContent = formatNumber(summary.total_tokens);
  $('tokenPrompt').textContent = formatNumber(summary.prompt_tokens);
  $('tokenCompletion').textContent = formatNumber(summary.completion_tokens);
  $('tokenCached').textContent = formatNumber(summary.cached_tokens);
  $('tokenCost').textContent = formatUSD(summary.total_cost_usd);
  $('tokenRows').innerHTML = records.map(record => {
    const success = Number(record.status) >= 200 && Number(record.status) < 400;
    const hasUsage = Number(record.total_tokens) || Number(record.prompt_tokens) || Number(record.completion_tokens);
    const speed = outputSpeed(record);
    const firstToken = Number(record.first_token_ms) ? `${(Number(record.first_token_ms) / 1000).toFixed(1)}s` : '-';
    return `<tr><td><time>${esc(displayTime(record.at))}</time><small>${esc(new Date(record.at).toLocaleDateString('zh-CN'))}</small></td><td><strong>${esc(record.instance || '-')}</strong><small class="mono">${esc(record.egress || '出口未知')}</small></td><td><code class="key-chip">${esc(record.client_key || '旧记录')}</code></td><td><span class="model-chip" title="${esc(record.model || '')}">${esc(record.model || '-')}</span></td><td><span class="stream-state ${record.stream ? 'streaming' : ''}">${record.stream ? '流' : '非流'}</span><small>${speed || '-'}</small></td><td class="usage-cell">${hasUsage ? `<strong>${formatNumber(record.prompt_tokens)} / ${formatNumber(record.completion_tokens)}</strong><small>总计 ${formatNumber(record.total_tokens)}${Number(record.cached_tokens) ? ` · 缓存 ${formatNumber(record.cached_tokens)}` : ''}</small>` : '<span class="muted">未返回用量</span>'}</td><td>${costDetails(record)}</td><td><span class="status-chip ${success ? 'ok' : 'bad'}">${Number(record.status) || '-'}</span><small>${esc(record.source || 'upstream')}</small></td><td class="latency-cell"><i class="${success ? 'ok' : 'bad'}"></i><strong>首字 ${firstToken}</strong><small>耗时 ${(Number(record.latency_ms) / 1000).toFixed(1)}s · 第${Number(record.attempts) || 1}次</small></td></tr>`;
  }).join('') || '<tr><td colspan="9"><p class="empty-state">当前筛选条件下暂无 Token 用量记录</p></td></tr>';
}

function render() {
  const rows = [];
	const egressOwners = new Map();
	(state.instances || []).forEach(instance => activeSlots(instance).forEach(slot => {
		if (!slot.egress) return;
		const owners = egressOwners.get(slot.egress) || [];
		owners.push(instance.instance);
		egressOwners.set(slot.egress, owners);
  }));
  (state.instances || []).forEach(instance => {
	const slots = instance.slots || [];
	const exits = activeSlots(instance).map(slot => {
		const value = esc(slot.egress || (slot.direct ? '直连（未探测）' : '代理未探测'));
		const duplicate = slot.egress && (egressOwners.get(slot.egress) || []).length > 1;
		const cooldowns = (slot.model_cooldowns || []).map(item => `<span class="badge wait" title="恢复时间 ${esc(displayTime(item.ready_at))}">${esc(item.model)} 冷却</span>`).join(' ');
		const node = slot.mihomo_node ? `<small class="muted" style="display:block;margin-top:4px">${esc(slot.mihomo_node)} → ${value}</small>` : value;
		return `${node}${duplicate ? ' <span class="badge wait" title="控制面将自动切换后出现的重复出口">重复出口</span>' : ''}${cooldowns ? ` ${cooldowns}` : ''}${slots.length > 1 ? ` <span class="badge pool" title="仅在 429 或故障时切换">${slots.length} 个候选</span>` : ''}`;
	}).join(', ') || '-';
    const containerID = instance.container_id ? String(instance.container_id).slice(0, 12) : '-';
    const authLabel = instance.zen_auth_mode === 'public' ? '公共 Key' : (instance.zen_api_key_masked || '自定义 Key 未设置');
    rows.push(`<tr><td><strong>${esc(instance.instance)}</strong><small class="mono muted" style="display:block;margin-top:4px" title="Docker 容器 ID">${esc(containerID)}</small></td><td><span class="badge ${instance.online ? 'ok' : instance.status === 'running' ? 'wait' : 'bad'}">${esc(instance.online ? '在线' : instance.status || '停止')}</span> <span class="badge ${instance.in_traffic_pool ? 'ok' : 'wait'}">${instance.in_traffic_pool ? '流量池' : '未接入'}</span></td><td class="mono">${esc(authLabel)}</td><td>${Number(instance.max_concurrency) || 0}</td><td class="mono">${exits}</td><td>${actions(instance)}</td></tr>`);
  });
  $('fleet').innerHTML = rows.join('') || '<tr><td colspan="6">暂无实例，请新建实例并设置 Zen API Key 与代理地址</td></tr>';
  $('keys').innerHTML = (state.keys || []).map(key => `<div class="key"><code>${esc(key)}</code><button class="icon-button danger" data-key="${esc(key)}" title="删除密钥">×</button></div>`).join('') || '<p class="muted">暂无密钥</p>';
  renderLogs();
}

async function refresh() {
  try {
    state = await api('/overview');
    const keys = await api('/keys');
    const mihomo = await api('/mihomo');
    $('mihomoStatus').textContent = mihomo.status === 'running' ? `运行中 · ${mihomo.node_count || 0} 出口` : mihomo.status;
    $('mihomoStatus').className = `badge ${mihomo.status === 'running' ? 'ok' : mihomo.status === 'degraded' ? 'bad' : 'wait'}`;
    $('mihomoForm').elements.subscription_url.value = '';
    state.mihomoProxyURLs = mihomo.proxy_urls || [];
	state.mihomoEndpoints = mihomo.endpoints || [];
	renderMihomoEndpoints();
    state.keys = keys.keys;
    $('instances').textContent = state.instances.length;
    $('online').textContent = state.instances.filter(instance => instance.online).length;
    $('concurrency').textContent = state.max_concurrency;
    $('rate').textContent = state.stats.upstream429 || 0;
    $('updated').textContent = `更新于 ${new Date().toLocaleTimeString('zh-CN')}`;
    render();
    if (currentPage() === 'tokens') renderTokens(await api(tokenQuery()));
  } catch (error) { alert(`刷新失败：${error.message}`); }
}

$('mihomoForm').onsubmit = async event => {
  event.preventDefault();
  const subscription_url = new FormData(event.currentTarget).get('subscription_url').trim();
  try { await api('/mihomo', { method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ subscription_url }) }); alert('Mihomo 配置已保存并重启'); refresh(); } catch (error) { alert(`Mihomo 配置失败：${error.message}`); }
};
$('mihomoProbe').onclick = async () => { try { await api('/mihomo/probe', { method: 'POST' }); alert('健康检测已触发'); refresh(); } catch (error) { alert(`健康检测失败：${error.message}`); } };
$('mihomoCopy').onclick = async () => { const values = state.mihomoProxyURLs || []; if (!values.length) { alert('请先保存有效订阅并生成出口'); return; } await navigator.clipboard?.writeText(values.join('\n')); alert(`已复制 ${values.length} 个 SOCKS5 地址，请在实例设置中逐行填写`); };
$('mihomoClear').onclick = async () => { if (!confirm('清除订阅并停止使用全部 Mihomo 节点？')) return; try { await api('/mihomo', { method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ clear: true }) }); refresh(); } catch (error) { alert(`清除失败：${error.message}`); } };
$('mihomoPrev').onclick = () => { mihomoPage--; renderMihomoEndpoints(); };
$('mihomoNext').onclick = () => { mihomoPage++; renderMihomoEndpoints(); };

$('refresh').onclick = refresh;
$('probe').onclick = refresh;
$('openCreate').onclick = () => $('createDialog').showModal();
$('tokenFilterApply').onclick = refresh;

$('createForm').onsubmit = async event => {
  if (event.submitter?.value === 'cancel') return;
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  const payload = { name: form.get('name').trim(), auth_mode: form.get('auth_mode'), zen_api_key: form.get('zen_api_key').trim(), max_concurrency: Number(form.get('max_concurrency')), queue_size: Number(form.get('queue_size')), proxy_urls: selectedProxyValues(event.currentTarget) };
  try { await api('/instances', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload) }); $('createDialog').close(); refresh(); } catch (error) { alert(`创建失败：${error.message}`); }
};

$('settingsForm').onsubmit = async event => {
  if (event.submitter?.value === 'cancel') return;
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  const name = form.get('name');
  const payload = { auth_mode: form.get('auth_mode'), zen_api_key: form.get('zen_api_key').trim(), max_concurrency: Number(form.get('max_concurrency')), queue_size: Number(form.get('queue_size')), proxy_urls: selectedProxyValues(event.currentTarget) };
  try { await api(`/instances/${encodeURIComponent(name)}`, { method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload) }); $('settingsDialog').close(); refresh(); } catch (error) { alert(`保存失败：${error.message}`); }
};

$('newKey').onclick = async () => { try { const result = await api('/keys', { method: 'POST' }); await navigator.clipboard?.writeText(result.key); alert(`已生成并同步：\n${result.key}`); refresh(); } catch (error) { alert(`生成失败：${error.message}`); } };

['createForm', 'settingsForm'].forEach(id => { const form = $(id); form.elements.auth_mode.onchange = () => syncAuthMode(form); syncAuthMode(form); });
['createForm', 'settingsForm'].forEach(id => { const form = $(id); form.elements.proxy_urls.addEventListener('input', () => syncMihomoChoiceChecks(form)); });

document.addEventListener('click', async event => {
  const source = event.target.closest('.log-source');
  if (source) { selectedLogSource = source.dataset.source; renderLogs(); return; }
  const useMihomo = event.target.closest('.use-mihomo');
  if (useMihomo) {
    const form = $(useMihomo.dataset.form);
    const values = (state.mihomoEndpoints || []).filter(endpointHealthy).map(endpoint => endpoint.url);
    if (!values.length) { alert('请先保存有效订阅并生成出口'); return; }
    form.elements.proxy_urls.value = values.join('\n');
    syncMihomoChoiceChecks(form);
    return;
  }
  const key = event.target.dataset.key;
  if (key && confirm('从全部实例删除该密钥？')) { await api(`/keys/${encodeURIComponent(key)}`, { method: 'DELETE' }); refresh(); return; }
  const settings = event.target.closest('.instance-settings');
  if (settings) {
    const instance = (state.instances || []).find(item => item.instance === settings.dataset.name);
    if (!instance) return;
    $('settingsForm').elements.name.value = instance.instance;
    $('settingsForm').elements.zen_api_key.value = '';
    $('settingsForm').elements.auth_mode.value = instance.zen_auth_mode || (instance.zen_api_key_configured ? 'custom' : 'public');
    $('settingsForm').elements.proxy_urls.value = (instance.proxy_urls || []).join('\n');
    $('settingsForm').elements.max_concurrency.value = instance.max_concurrency || 4;
    $('settingsForm').elements.queue_size.value = instance.queue_size ?? 8;
    $('settingsHint').textContent = `${instance.instance} 当前 Zen Key：${instance.zen_api_key_masked || '未设置'}；留空保持不变。`;
    syncAuthMode($('settingsForm'));
    syncMihomoChoiceChecks($('settingsForm'));
    $('settingsDialog').showModal();
    return;
  }
  const action = event.target.closest('.instance-action');
  if (action) { try { const result = await api(`/instances/${action.dataset.name}/${action.dataset.action}`, { method: 'POST' }); if (action.dataset.action === 'rotate') alert(`出口已切换：${result.previous_egress || '-'} → ${result.egress || '-'}\n节点：${result.node || '-'}`); refresh(); } catch (error) { alert(`操作失败：${error.message}`); } return; }
  const remove = event.target.closest('.instance-delete');
  if (remove && confirm(`永久删除 ${remove.dataset.name}？此操作不可恢复。`)) { try { await api(`/instances/${remove.dataset.name}`, { method: 'DELETE' }); refresh(); } catch (error) { alert(`删除失败：${error.message}`); } }
});

initTheme();
initPage();
checkForUpdate();
refresh();
setInterval(refresh, 10000);
