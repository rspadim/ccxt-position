import I18N from "./i18n.json";
import "@awesome.me/webawesome/dist/components/button/button.js";
import "@awesome.me/webawesome/dist/components/tab-group/tab-group.js";
import "@awesome.me/webawesome/dist/components/tab/tab.js";
import "@awesome.me/webawesome/dist/components/tab-panel/tab-panel.js";
import "@awesome.me/webawesome/dist/components/dialog/dialog.js";

const HISTORY_LIMIT = 12;
const STORAGE = {
  baseUrls: "simple_front.base_urls",
  apiKeys: "simple_front.api_keys",
  accountIds: "simple_front.account_ids",
  strategyIds: "simple_front.strategy_ids",
  language: "simple_front.language",
  themeMode: "simple_front.theme_mode",
  densityMode: "simple_front.density_mode",
  activeMenu: "simple_front.active_menu",
};

const state = {
  wsConnections: new Map(),
  pingTimer: null,
  connected: false,
  eventSeq: 0,
  uiLogSeq: 0,
  tables: {},
  availableAccountIds: [],
  accountLabels: new Map(),
  accountExchanges: new Map(),
  subscribedAccountIds: [],
  locale: "pt-BR",
  userRole: "",
  themeMode: "system",
  densityMode: "normal",
  postTradingLastPreview: null,
  adminOmsView: "open_orders",
  closeBySourceRow: null,
  omsPageSize: 100,
  omsLocalData: {
    openPositions: [],
    openOrders: [],
  },
  omsPager: {
    openPositions: { page: 1, pageSize: 100, total: 0, totalPages: 1 },
    openOrders: { page: 1, pageSize: 100, total: 0, totalPages: 1 },
    deals: { page: 1, pageSize: 100, total: 0, totalPages: 1 },
    historyPositions: { page: 1, pageSize: 100, total: 0, totalPages: 1 },
    historyOrders: { page: 1, pageSize: 100, total: 0, totalPages: 1 },
    ccxtOrders: { page: 1, pageSize: 100, total: 0, totalPages: 1 },
    ccxtTrades: { page: 1, pageSize: 100, total: 0, totalPages: 1 },
    postTrading: { page: 1, pageSize: 100, total: 0, totalPages: 1 },
    adminOms: { page: 1, pageSize: 100, total: 0, totalPages: 1 },
  },
};

const UI_NUMBER_FORMAT = {
  decimalPlaces: 14,
  smartSteps: [14, 5, 2, 0],
};

const UI_LOG_STORAGE = {
  dbName: "ccxt_oms_ui_logs",
  storeName: "ui_logs",
  version: 1,
  maxRows: null,
};

const LANGUAGE_FALLBACK_LABELS = {
  "pt-BR": "Português (Brasil)",
  "en-US": "English (US)",
  es: "Español",
};

function $(id) {
  const el = document.getElementById(id);
  if (!el) throw new Error(`missing element ${id}`);
  return el;
}

function loadHistory(key) {
  try {
    const raw = localStorage.getItem(key);
    const arr = raw ? JSON.parse(raw) : [];
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

function pushHistory(key, value) {
  if (!value) return;
  const current = loadHistory(key).filter((x) => String(x) !== String(value));
  current.unshift(value);
  localStorage.setItem(key, JSON.stringify(current.slice(0, HISTORY_LIMIT)));
}

function renderHistory(datalistId, values) {
  const node = $(datalistId);
  node.innerHTML = "";
  for (const value of values) {
    const option = document.createElement("option");
    option.value = String(value);
    node.appendChild(option);
  }
}

function renderAccountHistory(values) {
  const node = $("accountHistory");
  node.innerHTML = "";
  for (const value of values) {
    const id = Number(value);
    if (!Number.isFinite(id) || id <= 0) continue;
    const label = state.accountLabels.get(id) || "";
    const option = document.createElement("option");
    option.value = String(id);
    if (label) {
      option.label = `${id} - ${label}`;
      option.textContent = `${id} - ${label}`;
    }
    node.appendChild(option);
  }
}

function status(text, isConnected = false) {
  const normalizedText = String(text || "");
  const prettyText = normalizedText
    ? `${normalizedText.charAt(0).toUpperCase()}${normalizedText.slice(1)}`
    : normalizedText;
  const apply = (id) => {
    const node = document.getElementById(id);
    if (!node) return;
    node.textContent = prettyText;
    node.classList.toggle("status-connected", !!isConnected);
    node.classList.toggle("status-idle", !isConnected);
  };
  apply("status");
  apply("loginStatus");
  syncWsActionButtons(!!isConnected);
}

function syncWsActionButtons(isConnected) {
  const connectBtn = document.getElementById("connectBtn");
  const disconnectBtn = document.getElementById("disconnectBtn");
  if (!connectBtn || !disconnectBtn) return;
  connectBtn.classList.toggle("is-hidden", !!isConnected);
  disconnectBtn.classList.toggle("is-hidden", !isConnected);
}

function resolveTheme(mode) {
  if (mode === "dark") return "dark";
  if (mode === "light") return "light";
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function applyThemeMode(mode, persist = true) {
  const normalized = ["system", "light", "dark"].includes(String(mode)) ? String(mode) : "system";
  state.themeMode = normalized;
  const resolved = resolveTheme(normalized);
  document.documentElement.setAttribute("data-theme", resolved);
  const node = document.getElementById("themeModeBtn");
  if (node) {
    const themeTexts = {
      "pt-BR": { system: "Sistema", light: "Claro", dark: "Escuro" },
      "en-US": { system: "System", light: "Light", dark: "Dark" },
      es: { system: "Sistema", light: "Claro", dark: "Oscuro" },
    };
    const localeTexts = themeTexts[state.locale] || themeTexts["en-US"];
    const labels = {
      system: {
        iconHtml: '<span class="theme-icon-dual" aria-hidden="true"><i class="fa-solid fa-sun"></i><span>/</span><i class="fa-solid fa-moon"></i></span>',
        text: localeTexts.system,
      },
      light: { iconHtml: '<i class="fa-solid fa-sun" aria-hidden="true"></i>', text: localeTexts.light },
      dark: { iconHtml: '<i class="fa-solid fa-moon" aria-hidden="true"></i>', text: localeTexts.dark },
    };
    const item = labels[normalized] || labels.system;
    node.innerHTML = `${item.iconHtml}<span>${item.text}</span>`;
    const titlePrefix = state.locale === "pt-BR" ? "Tema" : state.locale === "es" ? "Tema" : "Theme";
    node.setAttribute("title", `${titlePrefix}: ${item.text}`);
  }
  if (persist) localStorage.setItem(STORAGE.themeMode, normalized);
}

function cycleThemeMode() {
  const order = ["system", "light", "dark"];
  const idx = order.indexOf(state.themeMode);
  const next = order[(idx + 1) % order.length];
  applyThemeMode(next, true);
}

function applyDensityMode(mode, persist = true) {
  const normalized = ["compact", "normal", "spacious"].includes(String(mode))
    ? String(mode)
    : "normal";
  state.densityMode = normalized;
  document.documentElement.setAttribute("data-density", normalized);
  const node = document.getElementById("densityModeBtn");
  if (node) {
    const iconByMode = {
      compact: "fa-table-cells",
      normal: "fa-table-cells-large",
      spacious: "fa-grip",
    };
    const labelKey = `density.${normalized}`;
    const label = t(labelKey, normalized);
    const icon = iconByMode[normalized] || iconByMode.normal;
    node.innerHTML = `<i class="fa-solid ${icon}" aria-hidden="true"></i><span>${label}</span>`;
    const titlePrefix = state.locale === "pt-BR" ? "Densidade" : state.locale === "es" ? "Densidad" : "Density";
    node.setAttribute("title", `${titlePrefix}: ${label}`);
  }
  if (persist) localStorage.setItem(STORAGE.densityMode, normalized);
}

function cycleDensityMode() {
  const order = ["compact", "normal", "spacious"];
  const idx = order.indexOf(state.densityMode);
  const next = order[(idx + 1) % order.length];
  applyDensityMode(next, true);
}

function getConfig() {
  return {
    baseUrl: $("baseUrl").value.trim().replace(/\/+$/, ""),
    apiKey: $("apiKey").value.trim(),
    viewAccountIds: getSelectedViewAccountIds(),
  };
}

function requireConfig() {
  const cfg = getConfig();
  if (!cfg.baseUrl) throw new Error("base_url is required");
  if (!cfg.apiKey) throw new Error("api_key is required");
  return cfg;
}

function parseAccountId(value) {
  const text = String(value || "").trim();
  const m = text.match(/^(\d+)\b/);
  if (!m) return null;
  const n = Number(m[1]);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function requireAccountId(inputId, label) {
  const parsed = parseAccountId($(inputId).value);
  if (!parsed) throw new Error(`${label} account_id is required`);
  return parsed;
}

function findAccountIdForOrderId(orderId) {
  const target = String(orderId);
  const sources = [state.tables.openOrders, state.tables.historyOrders].filter(Boolean);
  for (const table of sources) {
    const row = (table.getData() || []).find((item) => String(item.id) === target);
    if (row && Number(row.account_id) > 0) return Number(row.account_id);
  }
  return null;
}

function findAccountIdForPositionId(positionId) {
  const target = String(positionId);
  const sources = [state.tables.openPositions, state.tables.historyPositions].filter(Boolean);
  for (const table of sources) {
    const row = (table.getData() || []).find((item) => String(item.id) === target);
    if (row && Number(row.account_id) > 0) return Number(row.account_id);
  }
  return null;
}

function formStrategy(inputId) {
  const value = Number($(inputId).value || "0");
  return Number.isFinite(value) && value >= 0 ? value : 0;
}

function getSelectedViewAccountIds() {
  const selected = [...$("viewAccountsSelect").selectedOptions]
    .map((opt) => Number(opt.value))
    .filter((n) => Number.isFinite(n) && n > 0);
  return [...new Set(selected)];
}

function renderViewAccountsOptions(ids) {
  const node = $("viewAccountsSelect");
  const previouslySelected = new Set(getSelectedViewAccountIds());
  node.innerHTML = "";
  const grouped = new Map();
  for (const id of ids) {
    const exchangeId = String(state.accountExchanges.get(id) || "unknown");
    if (!grouped.has(exchangeId)) grouped.set(exchangeId, []);
    grouped.get(exchangeId).push(id);
  }
  for (const exchangeId of [...grouped.keys()].sort((a, b) => a.localeCompare(b))) {
    const optgroup = document.createElement("optgroup");
    optgroup.label = exchangeId;
    const groupIds = grouped.get(exchangeId).sort((a, b) => Number(a) - Number(b));
    for (const id of groupIds) {
      const option = document.createElement("option");
      option.value = String(id);
      const label = state.accountLabels.get(id) || "";
      option.textContent = label ? `${id} - ${label}` : String(id);
      option.selected = previouslySelected.has(id);
      optgroup.appendChild(option);
    }
    node.appendChild(optgroup);
  }
}

function renderStrategyAccountsOptions(ids) {
  const node = $("tradeStrategyAccountIds");
  const selected = new Set([...node.selectedOptions].map((opt) => Number(opt.value)));
  node.innerHTML = "";
  for (const id of ids) {
    const option = document.createElement("option");
    option.value = String(id);
    const label = state.accountLabels.get(id) || "";
    option.textContent = label ? `${id} - ${label}` : String(id);
    option.selected = selected.has(id);
    node.appendChild(option);
  }
}

function renderSendStrategyOptions(items) {
  const node = $("sendStrategyId");
  const current = String(node.value || "0");
  node.innerHTML = "";
  const defaultOpt = document.createElement("option");
  defaultOpt.value = "0";
  defaultOpt.textContent = "0 - sem strategy";
  node.appendChild(defaultOpt);
  for (const item of items || []) {
    const sid = Number(item.strategy_id);
    if (!Number.isFinite(sid) || sid <= 0) continue;
    const opt = document.createElement("option");
    opt.value = String(sid);
    opt.textContent = `${sid} - ${String(item.name || "").trim() || "strategy"}`;
    node.appendChild(opt);
  }
  node.value = [...node.options].some((o) => o.value === current) ? current : "0";
}

function selectedTradeStrategyAccountIds() {
  return [...$("tradeStrategyAccountIds").selectedOptions]
    .map((opt) => Number(opt.value))
    .filter((n) => Number.isFinite(n) && n > 0);
}

function parseCsvIntList(value) {
  return String(value || "")
    .split(",")
    .map((x) => Number(String(x).trim()))
    .filter((n) => Number.isFinite(n) && n > 0);
}

function parseNullablePositiveInt(value, label = "value") {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  if (!text) return null;
  const n = Number(text);
  if (!Number.isFinite(n) || n <= 0 || !Number.isInteger(n)) {
    throw new Error(`${label} inválido`);
  }
  return n;
}

function parseAccountIdsValue(value, fallbackAllAccountIds = []) {
  const allIds = [...new Set((fallbackAllAccountIds || []).map((x) => Number(x)).filter((n) => Number.isFinite(n) && n > 0))];
  if (Array.isArray(value)) {
    return [...new Set(value.map((x) => Number(x)).filter((n) => Number.isFinite(n) && n > 0))];
  }
  const text = String(value || "").trim().toLowerCase();
  if (!text || text === "all" || text === "*") {
    if (allIds.length > 0) return allIds;
    throw new Error("nenhuma conta disponível para aplicar 'all'");
  }
  return [...new Set(parseCsvIntList(value))];
}

function defaultPresetAccountId() {
  if (state.availableAccountIds.length > 0) return Number(state.availableAccountIds[0]);
  const known = collectFormAccountIds();
  if (known.length > 0) return Number(known[0]);
  return 1;
}

function presetPermissionsForRole(role) {
  const accountId = defaultPresetAccountId();
  const normalized = String(role || "").trim().toLowerCase();
  if (normalized === "admin") {
    return [];
  }
  if (normalized === "risk") {
    return [{
      account_id: accountId,
      can_read: true,
      can_trade: false,
      can_close_position: true,
      can_risk_manage: true,
      can_block_new_positions: true,
      can_block_account: true,
      restrict_to_strategies: false,
      strategy_ids: [],
    }];
  }
  if (normalized === "readonly") {
    return [{
      account_id: accountId,
      can_read: true,
      can_trade: false,
      can_close_position: false,
      can_risk_manage: false,
      can_block_new_positions: false,
      can_block_account: false,
      restrict_to_strategies: false,
      strategy_ids: [],
    }];
  }
  if (normalized === "robot") {
    return [{
      account_id: accountId,
      can_read: true,
      can_trade: true,
      can_close_position: true,
      can_risk_manage: false,
      can_block_new_positions: false,
      can_block_account: false,
      restrict_to_strategies: true,
      strategy_ids: [],
    }];
  }
  return [{
    account_id: accountId,
    can_read: true,
    can_trade: true,
    can_close_position: true,
    can_risk_manage: false,
    can_block_new_positions: false,
    can_block_account: false,
    restrict_to_strategies: false,
    strategy_ids: [],
  }];
}

async function apiRequest(path, options = {}, cfgOverride = null) {
  const cfg = cfgOverride || requireConfig();
  const headers = new Headers(options.headers || {});
  headers.set("x-api-key", cfg.apiKey);
  if (options.body !== undefined) headers.set("Content-Type", "application/json");
  const res = await fetch(`${cfg.baseUrl}${path}`, {
    method: options.method || "GET",
    headers,
    body: options.body !== undefined ? JSON.stringify(options.body) : undefined,
  });
  const text = await res.text();
  const json = text ? JSON.parse(text) : {};
  if (!res.ok) throw new Error(`${res.status} ${JSON.stringify(json)}`);
  return json;
}

function ensureOmsCommandSuccess(out, actionLabel = "oms_command") {
  if (!out || !Array.isArray(out.results)) return out;
  const failed = out.results.find((item) => item && item.ok === false);
  if (!failed) return out;
  const err = failed.error || {};
  const code = String(err.code || "dispatcher_error");
  const message = String(err.message || "").trim();
  const suffix = message ? `: ${message}` : "";
  throw new Error(`${actionLabel} -> ${code}${suffix}`);
}

async function resolveViewAccountIds(cfg) {
  if (cfg.viewAccountIds.length > 0) return cfg.viewAccountIds;
  if (state.availableAccountIds.length === 0) await loadAccountsByApiKey(cfg);
  const selectedAfterLoad = getSelectedViewAccountIds();
  if (selectedAfterLoad.length > 0) return selectedAfterLoad;
  const formIds = collectFormAccountIds();
  if (formIds.length > 0) return formIds;
  throw new Error("view accounts is empty; selecione uma ou mais contas");
}

function wsUrlFromBase(baseUrl) {
  const url = new URL(baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = "/ws";
  url.search = "";
  return url.toString();
}

function parseJsonInput(value, fallback) {
  const trimmed = value.trim();
  if (!trimmed) return fallback;
  return JSON.parse(trimmed);
}

function setCcxtResultBox(value) {
  const text = typeof value === "string" ? value : JSON.stringify(value, null, 2);
  $("ccxtResultBox").value = text;
}

function redrawTableByPaneId(paneId) {
  const map = {
    paneOpenPositions: "openPositions",
    paneOpenOrders: "openOrders",
    paneDeals: "deals",
    paneHistoryPositions: "historyPositions",
    paneHistoryOrders: "historyOrders",
    paneCcxtOrders: "ccxtOrders",
    paneCcxtTrades: "ccxtTrades",
    paneWsEvents: "events",
    paneUiLogs: "uiLogs",
  };
  const key = map[paneId];
  if (!key) return;
  const table = state.tables[key];
  if (!table || typeof table.redraw !== "function") return;
  table.redraw(true);
}

function tableHasRows(key) {
  if (OMS_LOCAL_KEYS.has(key)) {
    return (state.omsLocalData[key] || []).length > 0;
  }
  const table = state.tables[key];
  if (!table || typeof table.getData !== "function") return false;
  return (table.getData() || []).length > 0;
}

async function maybeAutoReloadOmsTab(panelName) {
  const kind = String(panelName || "").trim();
  const loaders = {
    openPositions: () => refreshOpenPositionsTable(),
    openOrders: () => refreshOpenOrdersTable(),
    deals: () => refreshHistoryTable("deals", false),
    historyPositions: () => refreshHistoryTable("historyPositions", false),
    historyOrders: () => refreshHistoryTable("historyOrders", false),
  };
  const load = loaders[kind];
  if (!load) return;
  if (tableHasRows(kind)) return;
  const cfg = getConfig();
  if (!cfg.baseUrl || !cfg.apiKey) return;
  try {
    await load();
    eventLog("oms_tab_autoload", { tab: kind });
  } catch (err) {
    eventLog("oms_tab_autoload_error", { tab: kind, error: String(err) });
    uiLog("error", "OMS tab auto reload failed", { tab: kind, error: String(err) });
  }
}

function bindWaTabs() {
  const positionTabs = $("positionTabs");
  const systemTabs = $("systemTabs");
  const onShow = (ev) => {
    const panelName = String(ev?.detail?.name || "");
    if (!panelName) return;
    const paneIdMap = {
      openPositions: "paneOpenPositions",
      openOrders: "paneOpenOrders",
      deals: "paneDeals",
      historyPositions: "paneHistoryPositions",
      historyOrders: "paneHistoryOrders",
      ccxtOrders: "paneCcxtOrders",
      ccxtTrades: "paneCcxtTrades",
      wsEvents: "paneWsEvents",
      uiLogs: "paneUiLogs",
    };
    redrawTableByPaneId(String(paneIdMap[panelName] || ""));
    if (ev?.currentTarget === positionTabs) {
      Promise.resolve(maybeAutoReloadOmsTab(panelName)).catch((err) => {
        eventLog("oms_tab_autoload_error", { tab: panelName, error: String(err) });
      });
    }
  };
  positionTabs.addEventListener("wa-tab-show", onShow);
  systemTabs.addEventListener("wa-tab-show", onShow);
  redrawTableByPaneId("paneOpenPositions");
  redrawTableByPaneId("paneCcxtOrders");
}

function languageLabelFor(code) {
  const dict = I18N?.[code] || {};
  return String(dict["meta.language_name"] || LANGUAGE_FALLBACK_LABELS[code] || code);
}

function t(key, fallback = "") {
  const dict = I18N?.[state.locale] || I18N?.["pt-BR"] || {};
  return String(dict[key] || fallback || key);
}

function setLoginMessage(kind, text) {
  const node = document.getElementById("loginMessage");
  if (!node) return;
  if (!text) {
    node.textContent = "";
    node.classList.add("is-hidden");
    node.classList.remove("notice-success", "notice-error", "notice-info");
    return;
  }
  node.textContent = String(text);
  node.classList.remove("is-hidden", "notice-success", "notice-error", "notice-info");
  if (kind === "success") node.classList.add("notice-success");
  else if (kind === "error") node.classList.add("notice-error");
  else node.classList.add("notice-info");
  uiLog(kind, String(text), { source: "login" });
}

function setAdminApiKeyMessage(kind, text) {
  const node = document.getElementById("adminCreatedApiKeyBox");
  if (!node) return;
  if (!text) {
    node.textContent = "";
    node.classList.add("is-hidden");
    node.classList.remove("notice-success", "notice-error", "notice-info");
    return;
  }
  node.textContent = String(text);
  node.classList.remove("is-hidden", "notice-success", "notice-error", "notice-info");
  if (kind === "success") node.classList.add("notice-success");
  else if (kind === "error") node.classList.add("notice-error");
  else node.classList.add("notice-info");
  uiLog(kind, String(text), { source: "admin_api_keys" });
}

function setAdminOmsMessage(kind, text) {
  const node = document.getElementById("adminOmsMessage");
  if (!node) return;
  if (!text) {
    node.textContent = "";
    node.classList.add("is-hidden");
    node.classList.remove("notice-success", "notice-error", "notice-info");
    return;
  }
  node.textContent = String(text);
  node.classList.remove("is-hidden", "notice-success", "notice-error", "notice-info");
  if (kind === "success") node.classList.add("notice-success");
  else if (kind === "error") node.classList.add("notice-error");
  else node.classList.add("notice-info");
  uiLog(kind, String(text), { source: "admin_oms" });
}

function setUserMessage(kind, text) {
  const node = document.getElementById("userMessage");
  if (!node) return;
  if (!text) {
    node.textContent = "";
    node.classList.add("is-hidden");
    node.classList.remove("notice-success", "notice-error", "notice-info");
    return;
  }
  node.textContent = String(text);
  node.classList.remove("is-hidden", "notice-success", "notice-error", "notice-info");
  if (kind === "success") node.classList.add("notice-success");
  else if (kind === "error") node.classList.add("notice-error");
  else node.classList.add("notice-info");
  uiLog(kind, String(text), { source: "user" });
}

function renderLanguageOptions(selectedCode = "pt-BR") {
  const node = $("loginLanguage");
  const languages = Object.keys(I18N || {});
  node.innerHTML = "";
  for (const code of languages) {
    const option = document.createElement("option");
    option.value = code;
    option.textContent = languageLabelFor(code);
    option.selected = code === selectedCode;
    node.appendChild(option);
  }
  if (![...node.options].some((opt) => opt.selected) && node.options.length > 0) {
    node.options[0].selected = true;
  }
}

function applyLanguageTexts() {
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const key = String(node.getAttribute("data-i18n") || "").trim();
    if (!key) return;
    node.textContent = t(key, node.textContent || "");
  });
  document.querySelectorAll("[data-i18n-placeholder]").forEach((node) => {
    const key = String(node.getAttribute("data-i18n-placeholder") || "").trim();
    if (!key) return;
    const fallback = node.getAttribute("placeholder") || "";
    node.setAttribute("placeholder", t(key, fallback));
  });
  const apply = (id, key, fallback) => {
    const node = document.getElementById(id);
    if (!node) return;
    node.textContent = t(key, fallback);
  };
  const applyMenu = (id, key, fallback) => {
    const node = document.getElementById(id);
    if (!node) return;
    const label = node.querySelector("span:last-child");
    if (!label) return;
    label.textContent = t(key, fallback);
  };
  applyMenu("tabLoginBtn", "menu.login_config", "Login / Config");
  applyMenu("tabUserBtn", "menu.user", "User");
  applyMenu("tabStrategiesBtn", "menu.strategies", "Strategies");
  applyMenu("tabPositionsBtn", "menu.oms", "OMS");
  applyMenu("tabPostTradingGroupBtn", "menu.post_trading", "Post Trading");
  applyMenu("tabPostTradingOrdersBtn", "menu.post_trading_external_orders", "External Orders");
  applyMenu("tabCommandsBtn", "menu.ccxt_commands", "CCXT Commands");
  applyMenu("tabSystemBtn", "menu.system_monitor", "System Monitor");
  applyMenu("tabRiskBtn", "menu.risk", "Risk");
  applyMenu("tabAdminGroupBtn", "menu.admin", "Administration");
  applyMenu("tabAdminBtn", "admin.accounts", "Accounts");
  applyMenu("tabAdminUsersBtn", "admin.users", "Users");
  applyMenu("tabAdminApiKeysBtn", "menu.api_keys", "API Keys");
  applyMenu("tabAdminStatusBtn", "admin.system_status", "System Status");
  applyMenu("tabAdminOmsBtn", "menu.admin_oms_crud", "OMS CRUD");
  apply("loginModeLabel", "login.mode", "Mode");
  apply("loginModeApiKeyOption", "login.mode_api_key", "API Key");
  apply("loginModeUserPassOption", "login.mode_user_password", "User + Password");
  apply("selectAllAccountsBtn", "common.select_all", "Select All");
  apply("clearAccountsBtn", "common.clear", "Clear");
  apply("viewAccountsLabel", "trade.filter_accounts", "Filter Accounts");
  apply("loginAuthBtn", "login.authenticate", "Authenticate");
  apply("connectBtn", "trade.connect_ws", "Connect WS");
  apply("disconnectBtn", "trade.disconnect_ws", "Disconnect");
  apply("refreshOmsBtn", "trade.refresh_tables", "Refresh Tables");
  apply("accountsBtn", "trade.load_accounts", "Reload Accounts");
  apply("tradeCreateStrategyTitle", "trade.create_strategy", "Create Strategy");
  apply("tradeCreateStrategyBtn", "trade.create_strategy", "Create Strategy");
  apply("tradeMyStrategiesTitle", "trade.my_strategies", "My Strategies");
  apply("loadTradeStrategiesBtn", "trade.load_strategies", "Load Strategies");
  apply("tradeStrategyNameLabel", "trade.name", "Name");
  apply("tradeStrategyAccountsLabel", "trade.accounts", "Accounts");
  apply("loadUserApiKeysBtn", "user.load_api_keys", "Reload API Keys");
  apply("userCreateApiKeyBtn", "user.create_api_key", "Create API Key");
  apply("userSaveProfileBtn", "user.save_profile", "Save Profile");
  apply("userSavePasswordBtn", "user.save_password", "Save Password");
  apply("riskSavePermissionBtn", "risk.save_permission", "Save Permission");
  const closeByModal = document.getElementById("closeByModal");
  if (closeByModal) {
    closeByModal.setAttribute("label", t("closeby.title", "Close By"));
  }
  updatePagerButtonI18n();
  applyDensityMode(state.densityMode, false);
  updateApiKeyToggleLabel();
  updateLoginPasswordToggleLabel();
}

function updateApiKeyToggleLabel() {
  const input = document.getElementById("apiKey");
  const btn = document.getElementById("toggleApiKeyBtn");
  if (!input || !btn) return;
  const isHidden = input.type === "password";
  const text = isHidden ? t("login.show", "show") : t("login.hide", "hide");
  btn.setAttribute("aria-label", text);
  btn.setAttribute("title", text);
  btn.innerHTML = isHidden
    ? '<i class="fa-solid fa-eye" aria-hidden="true"></i>'
    : '<i class="fa-solid fa-eye-slash" aria-hidden="true"></i>';
}

function updateLoginPasswordToggleLabel() {
  const input = document.getElementById("loginPassword");
  const btn = document.getElementById("toggleLoginPasswordBtn");
  if (!input || !btn) return;
  const isHidden = input.type === "password";
  const text = isHidden ? t("login.show", "show") : t("login.hide", "hide");
  btn.setAttribute("aria-label", text);
  btn.setAttribute("title", text);
  btn.innerHTML = isHidden
    ? '<i class="fa-solid fa-eye" aria-hidden="true"></i>'
    : '<i class="fa-solid fa-eye-slash" aria-hidden="true"></i>';
}

function bindSidebarMenu() {
  const adminGroup = $("tabAdminGroupBtn");
  const adminSubmenu = $("adminSubmenu");
  const postTradingGroup = $("tabPostTradingGroupBtn");
  const postTradingSubmenu = $("postTradingSubmenu");
  const setAdminExpanded = (expanded) => {
    adminSubmenu.classList.toggle("is-hidden", !expanded);
    adminGroup.classList.toggle("active", expanded);
  };
  const setPostTradingExpanded = (expanded) => {
    postTradingSubmenu.classList.toggle("is-hidden", !expanded);
    postTradingGroup.classList.toggle("active", expanded);
  };
  $("tabLoginBtn").addEventListener("click", () => switchTab("login"));
  $("tabUserBtn").addEventListener("click", () => switchTab("user"));
  $("tabCommandsBtn").addEventListener("click", () => switchTab("commands"));
  $("tabPositionsBtn").addEventListener("click", () => switchTab("positions"));
  $("tabSystemBtn").addEventListener("click", () => switchTab("system"));
  $("tabStrategiesBtn").addEventListener("click", () => switchTab("strategies"));
  $("tabRiskBtn").addEventListener("click", () => switchTab("risk"));
  postTradingGroup.addEventListener("click", () => {
    const collapsed = postTradingSubmenu.classList.contains("is-hidden");
    setPostTradingExpanded(collapsed);
    if (collapsed) switchTab("postTrading");
  });
  $("tabPostTradingOrdersBtn").addEventListener("click", () => switchTab("postTrading"));
  adminGroup.addEventListener("click", () => {
    const collapsed = adminSubmenu.classList.contains("is-hidden");
    setAdminExpanded(collapsed);
    if (collapsed) switchTab("admin");
  });
  $("tabAdminBtn").addEventListener("click", () => switchTab("admin"));
  $("tabAdminUsersBtn").addEventListener("click", () => switchTab("adminUsers"));
  $("tabAdminApiKeysBtn").addEventListener("click", () => switchTab("adminApiKeys"));
  $("tabAdminStatusBtn").addEventListener("click", () => switchTab("adminStatus"));
  $("tabAdminOmsBtn").addEventListener("click", () => switchTab("adminOms"));
  setAdminExpanded(false);
  setPostTradingExpanded(false);
}

function bindShellControls() {
  const app = $("app");
  const toggleBtn = $("menuToggleBtn");
  const collapseBtn = $("sidebarCollapseBtn");
  const sync = () => {
    const collapsed = app.classList.contains("sidebar-collapsed");
    toggleBtn.setAttribute("aria-expanded", String(!collapsed));
    toggleBtn.setAttribute("title", collapsed ? "Abrir menu" : "Fechar menu");
    collapseBtn.setAttribute("title", collapsed ? "Menu recolhido" : "Recolher menu");
  };
  toggleBtn.addEventListener("click", () => {
    app.classList.toggle("sidebar-collapsed");
    window.scrollTo({ top: 0, left: 0, behavior: "auto" });
    const mainContent = document.querySelector(".main-content");
    if (mainContent && typeof mainContent.scrollTo === "function") {
      mainContent.scrollTo({ top: 0, left: 0, behavior: "auto" });
    }
    sync();
  });
  collapseBtn.addEventListener("click", () => {
    app.classList.add("sidebar-collapsed");
    sync();
  });
  sync();
}

function collectFormAccountIds() {
  const ids = [
    parseAccountId($("sendAccountId").value),
    parseAccountId($("cancelOrderAccountId").value),
    parseAccountId($("ccxtAccountId").value),
    parseAccountId($("changeOrderAccountId").value),
    parseAccountId($("positionChangeAccountId").value),
    parseAccountId($("closePositionAccountId").value),
    parseAccountId($("closeByAccountId").value),
    parseAccountId($("riskAccountId").value),
    parseAccountId($("riskStrategyAccountId").value),
    parseAccountId($("riskPermAccountId").value),
  ].filter((n) => Number.isFinite(n) && n > 0);
  return [...new Set(ids)];
}

function normalizeTabulatorColumns(input) {
  const isIntegerLikeField = (field) => {
    const key = String(field || "").toLowerCase();
    return (
      key === "id"
      || key.endsWith("_id")
      || key.includes("seq")
      || key.includes("page")
    );
  };
  const isDecimalLikeField = (field) => {
    const key = String(field || "").toLowerCase();
    return (
      key.includes("qty")
      || key.includes("price")
      || key.includes("fee")
      || key.includes("pnl")
    );
  };
  const shouldCenter = (field) => {
    const key = String(field || "").toLowerCase();
    return (
      key.startsWith("_")
      || key.includes("status")
      || key === "side"
      || key.includes("state")
      || key.includes("mode")
      || key.includes("role")
      || key.includes("level")
      || key.includes("kind")
      || key.includes("reconciled")
    );
  };
  const formatDecimalForUi = (value) => {
    if (value === null || value === undefined) return "";
    const raw = String(value).trim();
    if (!raw) return "";
    const n = Number(raw);
    if (!Number.isFinite(n)) return raw;
    const maxPlaces = Math.max(0, Number(UI_NUMBER_FORMAT.decimalPlaces) || 14);
    const zeroThreshold = 10 ** (-maxPlaces);
    if (Math.abs(n) < zeroThreshold) return "0";
    const normalize = (num, places) => Number(num.toFixed(places));
    const render = (num, places) => {
      const fixed = num.toFixed(places);
      const trimmed = fixed.replace(/\.?0+$/, "");
      return trimmed === "-0" ? "0" : trimmed;
    };
    const base = normalize(n, maxPlaces);
    const seen = new Set();
    const steps = [];
    for (const step of UI_NUMBER_FORMAT.smartSteps || []) {
      const p = Math.max(0, Number(step) || 0);
      if (!seen.has(p)) {
        seen.add(p);
        steps.push(p);
      }
    }
    if (!seen.has(maxPlaces)) steps.unshift(maxPlaces);
    let chosen = maxPlaces;
    for (const p of steps) {
      if (normalize(n, p) === base) chosen = p;
    }
    return render(n, chosen);
  };
  if (!Array.isArray(input)) return [];
  return input.map((col) => {
    if (!col || typeof col !== "object") return col;
    const next = { ...col };
    if (Array.isArray(next.columns)) {
      next.columns = normalizeTabulatorColumns(next.columns);
      return next;
    }
    if (!next.hozAlign) {
      if (shouldCenter(next.field)) next.hozAlign = "center";
      else if (isIntegerLikeField(next.field) || isDecimalLikeField(next.field)) next.hozAlign = "right";
      else next.hozAlign = "left";
    }
    if (!next.headerHozAlign) {
      if (shouldCenter(next.field)) next.headerHozAlign = "center";
      else if (isIntegerLikeField(next.field) || isDecimalLikeField(next.field)) next.headerHozAlign = "right";
      else next.headerHozAlign = "left";
    }
    if (!next.formatter && isDecimalLikeField(next.field)) {
      next.formatter = (cell) => formatDecimalForUi(cell.getValue());
    }
    return next;
  });
}

function makeTable(id, columns, options = {}) {
  const Tabulator = window.Tabulator;
  if (!Tabulator) throw new Error("Tabulator not loaded");
  const tableHost = document.getElementById(id);
  const mergedOptions = { ...options };
  const initialHeight = String(mergedOptions.height || mergedOptions.maxHeight || "300px");
  delete mergedOptions.maxHeight;
  if (tableHost) {
    tableHost.classList.add("table-resizable");
    if (!tableHost.style.height) tableHost.style.height = initialHeight;
  }
  const table = new Tabulator(`#${id}`, {
    data: [],
    layout: "fitDataStretch",
    height: initialHeight,
    placeholder: "sem dados",
    columns: normalizeTabulatorColumns(columns),
    ...mergedOptions,
  });
  if (tableHost && typeof ResizeObserver !== "undefined") {
    const observer = new ResizeObserver(() => {
      if (table && typeof table.redraw === "function") table.redraw(true);
    });
    observer.observe(tableHost);
  }
  return table;
}

function upsert(table, row, key = "id") {
  const rows = table.getData();
  const idx = rows.findIndex((item) => String(item[key]) === String(row[key]));
  if (idx >= 0) {
    rows[idx] = { ...rows[idx], ...row };
    table.setData(rows);
    return;
  }
  table.addData([row], true);
}

function removeByKey(table, keyValue, key = "id") {
  if (keyValue === undefined || keyValue === null) return;
  const rows = table.getData().filter((item) => String(item[key]) !== String(keyValue));
  table.setData(rows);
}

function replaceRowsForAccount(table, accountId, items) {
  const normalizedItems = Array.isArray(items) ? items.filter((x) => x && typeof x === "object") : [];
  if (!Number.isFinite(Number(accountId)) || Number(accountId) <= 0) {
    table.setData(mergeById(normalizedItems));
    return;
  }
  const aid = Number(accountId);
  const keep = (table.getData() || []).filter((row) => Number(row.account_id) !== aid);
  table.setData(mergeById([...keep, ...normalizedItems]));
}

function append(table, row, max = 400) {
  table.addData([row], true);
  const rows = table.getData();
  if (rows.length > max) {
    table.setData(rows.slice(0, max));
  }
}

function buildTradeStrategiesColumns() {
  return [
    {
      title: "",
      field: "_save",
      hozAlign: "center",
      headerHozAlign: "center",
      width: 44,
      minWidth: 44,
      maxWidth: 44,
      widthGrow: 0,
      widthShrink: 0,
      resizable: false,
      cssClass: "col-action",
      headerSort: false,
      formatter: () => '<i class="fa-solid fa-floppy-disk icon-save" title="Save" aria-hidden="true"></i>',
      cellClick: async (_ev, cell) => {
        const row = cell.getRow().getData();
        try {
          const cfg = requireConfig();
          const out = await apiRequest(`/admin/strategies/${row.strategy_id}`, {
            method: "PATCH",
            body: {
              name: String(row.name || "").trim() || null,
              status: String(row.status || "").trim() || null,
              client_strategy_id: parseNullablePositiveInt(row.client_strategy_id, "client_strategy_id"),
              account_ids: parseAccountIdsValue(row.account_ids, state.availableAccountIds),
            },
          }, cfg);
          eventLog("trade_update_strategy", out);
          await loadTradeStrategies(cfg);
        } catch (err) {
          eventLog("trade_update_strategy_error", { error: String(err), strategy_id: row.strategy_id });
        }
      },
    },
    {
      title: "",
      field: "_toggle",
      hozAlign: "center",
      headerHozAlign: "center",
      width: 44,
      minWidth: 44,
      maxWidth: 44,
      widthGrow: 0,
      widthShrink: 0,
      resizable: false,
      cssClass: "col-action",
      headerSort: false,
      formatter: (cell) => {
        const row = cell.getRow().getData();
        if (row.status === "active") {
          return '<i class="fa-solid fa-xmark icon-danger" title="Disable" aria-hidden="true"></i>';
        }
        return '<i class="fa-solid fa-check icon-ok" title="Enable" aria-hidden="true"></i>';
      },
      cellClick: async (_ev, cell) => {
        const row = cell.getRow().getData();
        const nextStatus = row.status === "active" ? "disabled" : "active";
        try {
          const cfg = requireConfig();
          const out = await apiRequest(`/admin/strategies/${row.strategy_id}`, {
            method: "PATCH",
            body: { status: nextStatus },
          }, cfg);
          eventLog("trade_toggle_strategy_status", out);
          await loadTradeStrategies(cfg);
        } catch (err) {
          eventLog("trade_toggle_strategy_status_error", { error: String(err), strategy_id: row.strategy_id });
        }
      },
    },
    {
      title: t("trade.col_client_strategy_id", "Client Strategy ID"),
      field: "client_strategy_id",
      editor: "input",
      width: 145,
      formatter: (cell) => {
        const v = cell.getValue();
        return v === null || v === undefined || String(v).trim() === "" ? "" : String(v);
      },
    },
    { title: t("trade.col_strategy_id", "Strategy ID"), field: "strategy_id", width: 100 },
    { title: t("trade.col_name", "Name"), field: "name", editor: "input", width: 240 },
    {
      title: t("trade.col_status", "Status"),
      field: "status",
      editor: "list",
      editorParams: { values: ["active", "disabled"] },
      width: 110,
    },
    {
      title: t("trade.col_account_ids", "Accounts"),
      field: "account_ids",
      widthGrow: 1,
      editor: "input",
      formatter: (cell) => {
        const arr = cell.getValue();
        if (Array.isArray(arr)) return arr.join(", ");
        return String(arr || "");
      },
    },
  ];
}

function nowIso() {
  return new Date().toISOString();
}

function eventLog(kind, payload) {
  state.eventSeq += 1;
  append(state.tables.events, {
    seq: state.eventSeq,
    at: nowIso(),
    kind,
    payload: JSON.stringify(payload).slice(0, 2400),
  });
}

function uiLogStorageAvailable() {
  return typeof indexedDB !== "undefined";
}

function hashScope(text) {
  let h = 2166136261;
  const src = String(text || "");
  for (let i = 0; i < src.length; i += 1) {
    h ^= src.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return `h${(h >>> 0).toString(16).padStart(8, "0")}`;
}

function currentUiLogScopeHash() {
  const base = String(document.getElementById("baseUrl")?.value || "").trim().toLowerCase();
  const key = String(document.getElementById("apiKey")?.value || "").trim();
  if (!base && !key) return "h_anonymous";
  return hashScope(`${base}|${key}`);
}

function openUiLogDb() {
  return new Promise((resolve, reject) => {
    if (!uiLogStorageAvailable()) {
      reject(new Error("indexeddb_unavailable"));
      return;
    }
    const req = indexedDB.open(UI_LOG_STORAGE.dbName, UI_LOG_STORAGE.version);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(UI_LOG_STORAGE.storeName)) {
        db.createObjectStore(UI_LOG_STORAGE.storeName, { keyPath: "id", autoIncrement: true });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error || new Error("indexeddb_open_error"));
  });
}

function idbRequest(req) {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error || new Error("indexeddb_request_error"));
  });
}

async function persistUiLogRow(row) {
  try {
    const db = await openUiLogDb();
    try {
      const tx = db.transaction(UI_LOG_STORAGE.storeName, "readwrite");
      const store = tx.objectStore(UI_LOG_STORAGE.storeName);
      const scopeHash = currentUiLogScopeHash();
      await idbRequest(store.add({ ...row, scope_hash: scopeHash, created_ts: Date.now() }));
      const maxRows = Number(UI_LOG_STORAGE.maxRows);
      if (Number.isFinite(maxRows) && maxRows > 0) {
        const keys = await idbRequest(store.getAllKeys());
        const overflow = Math.max(0, (keys?.length || 0) - maxRows);
        if (overflow > 0) {
          for (let i = 0; i < overflow; i += 1) {
            await idbRequest(store.delete(keys[i]));
          }
        }
      }
      await new Promise((resolve, reject) => {
        tx.oncomplete = () => resolve(true);
        tx.onerror = () => reject(tx.error || new Error("indexeddb_tx_error"));
        tx.onabort = () => reject(tx.error || new Error("indexeddb_tx_abort"));
      });
    } finally {
      db.close();
    }
  } catch {
    // Persistência de log não pode quebrar a UI.
  }
}

async function loadUiLogsFromStorage() {
  if (!state.tables.uiLogs) return;
  try {
    const db = await openUiLogDb();
    try {
      const tx = db.transaction(UI_LOG_STORAGE.storeName, "readonly");
      const store = tx.objectStore(UI_LOG_STORAGE.storeName);
      const rows = await idbRequest(store.getAll());
      const scopeHash = currentUiLogScopeHash();
      const normalized = (rows || [])
        .filter((row) => String(row?.scope_hash || "h_anonymous") === scopeHash)
        .map((row) => ({
        seq: Number(row?.seq || 0),
        at: String(row?.at || ""),
        level: String(row?.level || "info"),
        message: String(row?.message || ""),
        payload: String(row?.payload || ""),
      }));
      state.tables.uiLogs.setData(normalized);
      if (typeof state.tables.uiLogs.setPage === "function") {
        await state.tables.uiLogs.setPage(1);
      }
      const maxSeq = normalized.reduce((acc, row) => Math.max(acc, Number(row.seq || 0)), 0);
      state.uiLogSeq = Math.max(state.uiLogSeq, maxSeq);
      updateUiLogsPagerInfo();
    } finally {
      db.close();
    }
  } catch {
    // Sem IndexedDB, segue normal só em memória.
  }
}

async function clearUiLogsStorage() {
  try {
    const db = await openUiLogDb();
    try {
      const tx = db.transaction(UI_LOG_STORAGE.storeName, "readwrite");
      const store = tx.objectStore(UI_LOG_STORAGE.storeName);
      const scopeHash = currentUiLogScopeHash();
      const rows = await idbRequest(store.getAll());
      for (const row of rows || []) {
        if (String(row?.scope_hash || "h_anonymous") !== scopeHash) continue;
        if (row?.id !== undefined) {
          await idbRequest(store.delete(row.id));
        }
      }
      await new Promise((resolve, reject) => {
        tx.oncomplete = () => resolve(true);
        tx.onerror = () => reject(tx.error || new Error("indexeddb_tx_error"));
        tx.onabort = () => reject(tx.error || new Error("indexeddb_tx_abort"));
      });
    } finally {
      db.close();
    }
  } catch {
    // no-op
  }
}

function updateUiLogsPagerInfo() {
  const table = state.tables.uiLogs;
  const info = document.getElementById("uiLogsPageInfo");
  const input = document.getElementById("uiLogsPageInput");
  if (!table || !info) return;
  const page = Number(table.getPage?.() || 1);
  const maxPage = Math.max(1, Number(table.getPageMax?.() || 1));
  info.textContent = `${page} / ${maxPage}`;
  if (input) input.value = String(page);
}

async function goToUiLogsPage(nextPage) {
  const table = state.tables.uiLogs;
  if (!table || typeof table.setPage !== "function") return;
  const maxPage = Math.max(1, Number(table.getPageMax?.() || 1));
  const page = Math.min(maxPage, Math.max(1, Number(nextPage) || 1));
  await table.setPage(page);
  updateUiLogsPagerInfo();
}

async function setUiLogsPageSize(nextSize) {
  const table = state.tables.uiLogs;
  if (!table || typeof table.setPageSize !== "function") return;
  const size = Math.max(1, Math.min(5000, Number(nextSize) || 100));
  await table.setPageSize(size);
  await table.setPage(1);
  updateUiLogsPagerInfo();
}

async function reloadUiLogsForCurrentScope() {
  if (state.tables.uiLogs) state.tables.uiLogs.clearData();
  state.uiLogSeq = 0;
  await loadUiLogsFromStorage();
}

function uiLog(level, message, payload = {}) {
  if (!state.tables.uiLogs) return;
  state.uiLogSeq += 1;
  const row = {
    seq: state.uiLogSeq,
    at: nowIso(),
    level: String(level || "info"),
    message: String(message || ""),
    payload: JSON.stringify(payload).slice(0, 2400),
  };
  const maxRows = Number(UI_LOG_STORAGE.maxRows);
  if (Number.isFinite(maxRows) && maxRows > 0) append(state.tables.uiLogs, row, maxRows);
  else append(state.tables.uiLogs, row, Number.POSITIVE_INFINITY);
  updateUiLogsPagerInfo();
  Promise.resolve(persistUiLogRow(row)).catch(() => {});
}

function accountIdsCsv(accountIds) {
  return accountIds.map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0).join(",");
}

async function fetchCombinedSnapshot(accountIds, cfg) {
  const csv = accountIdsCsv(accountIds);
  const query = new URLSearchParams({ account_ids: csv, limit: "5000" });
  const [openOrders, openPositions] = await Promise.all([
    apiRequest(`/oms/orders/open?${query.toString()}`, {}, cfg),
    apiRequest(`/oms/positions/open?${query.toString()}`, {}, cfg),
  ]);
  return {
    openOrders: openOrders.items || [],
    openPositions: openPositions.items || [],
  };
}

function mergeById(items) {
  const map = new Map();
  for (const item of items) {
    const id = item.id !== undefined ? String(item.id) : JSON.stringify(item);
    map.set(id, item);
  }
  return [...map.values()];
}

function sortByIdAsc(items) {
  return [...(items || [])].sort((a, b) => Number(a?.id || 0) - Number(b?.id || 0));
}

function clampPage(page, totalPages) {
  const p = Number(page);
  if (!Number.isFinite(p) || p < 1) return 1;
  const max = Math.max(1, Number(totalPages) || 1);
  return Math.min(Math.trunc(p), max);
}

function updatePagerInfo(key) {
  const pager = state.omsPager[key];
  if (!pager) return;
  const infoId = `${key}PageInfo`;
  const inputId = `${key}PageInput`;
  const info = document.getElementById(infoId);
  const input = document.getElementById(inputId);
  if (info) info.textContent = `${pager.page} / ${pager.totalPages}`;
  if (input) input.value = String(pager.page);
}

function setPagerFromTotal(key, total, page = 1, pageSize = state.omsPageSize) {
  const normalizedPageSize = Math.max(1, Number(pageSize) || state.omsPageSize);
  const normalizedTotal = Math.max(0, Number(total) || 0);
  const totalPages = Math.max(1, Math.ceil(normalizedTotal / normalizedPageSize));
  const normalizedPage = clampPage(page, totalPages);
  state.omsPager[key] = {
    page: normalizedPage,
    pageSize: normalizedPageSize,
    total: normalizedTotal,
    totalPages,
  };
  updatePagerInfo(key);
}

function renderLocalOmsPage(key) {
  const tableKey = key === "openPositions" ? "openPositions" : "openOrders";
  const rows = sortByIdAsc(mergeById(state.omsLocalData[key] || []));
  const pager = state.omsPager[key];
  const pageSize = Math.max(1, Number(pager?.pageSize || state.omsPageSize));
  const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
  const page = clampPage(pager?.page || 1, totalPages);
  const offset = (page - 1) * pageSize;
  const pageRows = rows.slice(offset, offset + pageSize);
  state.omsLocalData[key] = rows;
  setPagerFromTotal(key, rows.length, page, pageSize);
  state.tables[tableKey].setData(pageRows);
}

function setLocalOmsRows(key, rows, page = null) {
  state.omsLocalData[key] = sortByIdAsc(mergeById(rows || []));
  if (page !== null) {
    state.omsPager[key].page = Math.max(1, Number(page) || 1);
  }
  renderLocalOmsPage(key);
}

function upsertLocalOmsRow(key, row) {
  const rows = [...(state.omsLocalData[key] || [])];
  const idx = rows.findIndex((item) => String(item.id) === String(row.id));
  if (idx >= 0) rows[idx] = { ...rows[idx], ...row };
  else rows.push(row);
  state.omsLocalData[key] = sortByIdAsc(rows);
  renderLocalOmsPage(key);
}

function removeLocalOmsRow(key, idValue) {
  const rows = (state.omsLocalData[key] || []).filter((item) => String(item.id) !== String(idValue));
  state.omsLocalData[key] = rows;
  renderLocalOmsPage(key);
}

function replaceLocalOmsRowsForAccount(key, accountId, items) {
  const normalizedItems = Array.isArray(items) ? items.filter((x) => x && typeof x === "object") : [];
  if (!Number.isFinite(Number(accountId)) || Number(accountId) <= 0) {
    setLocalOmsRows(key, normalizedItems);
    return;
  }
  const aid = Number(accountId);
  const keep = (state.omsLocalData[key] || []).filter((row) => Number(row.account_id) !== aid);
  setLocalOmsRows(key, [...keep, ...normalizedItems]);
}

function todayIsoDate() {
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function endOfMonthIsoDate(base = new Date()) {
  const d = new Date(base.getFullYear(), base.getMonth() + 1, 0);
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function minusDaysIsoDate(days) {
  const d = new Date();
  d.setDate(d.getDate() - Math.max(0, Number(days) || 0));
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function startOfMonthIsoDate(base = new Date()) {
  const d = new Date(base.getFullYear(), base.getMonth(), 1);
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function defaultHistoryDateRange() {
  const startOfMonth = startOfMonthIsoDate();
  const sevenDaysAgo = minusDaysIsoDate(7);
  const startDate = sevenDaysAgo < startOfMonth ? sevenDaysAgo : startOfMonth;
  return {
    startDate,
    endDate: endOfMonthIsoDate(),
  };
}

function applyDefaultHistoryDates() {
  const range = defaultHistoryDateRange();
  const pairs = [
    ["dealsStartDate", "dealsEndDate"],
    ["historyPositionsStartDate", "historyPositionsEndDate"],
    ["historyOrdersStartDate", "historyOrdersEndDate"],
    ["ccxtOrdersStartDate", "ccxtOrdersEndDate"],
    ["ccxtTradesStartDate", "ccxtTradesEndDate"],
    ["postTradingStartDate", "postTradingEndDate"],
    ["adminOmsStartDate", "adminOmsEndDate"],
  ];
  for (const [startId, endId] of pairs) {
    const startNode = document.getElementById(startId);
    const endNode = document.getElementById(endId);
    if (startNode && !startNode.value) startNode.value = range.startDate;
    if (endNode && !endNode.value) endNode.value = range.endDate;
  }
}

function readHistoryFilter(kind) {
  const map = {
    deals: ["dealsStartDate", "dealsEndDate"],
    historyPositions: ["historyPositionsStartDate", "historyPositionsEndDate"],
    historyOrders: ["historyOrdersStartDate", "historyOrdersEndDate"],
    ccxtOrders: ["ccxtOrdersStartDate", "ccxtOrdersEndDate"],
    ccxtTrades: ["ccxtTradesStartDate", "ccxtTradesEndDate"],
  };
  const pair = map[kind];
  if (!pair) throw new Error("invalid history kind");
  const startDate = String($(pair[0]).value || "").trim();
  const endDate = String($(pair[1]).value || "").trim();
  if (!startDate || !endDate) {
    throw new Error("start_date e end_date são obrigatórios");
  }
  return { startDate, endDate };
}

async function refreshHistoryTable(kind, resetPage = false) {
  const cfg = requireConfig();
  const accountIds = await resolveViewAccountIds(cfg);
  const csv = accountIdsCsv(accountIds);
  const pager = state.omsPager[kind];
  const page = resetPage ? 1 : Math.max(1, Number(pager?.page || 1));
  const pageSize = Math.max(1, Number(pager?.pageSize || state.omsPageSize));
  const { startDate, endDate } = readHistoryFilter(kind);
  const pathByKind = {
    deals: "/oms/deals",
    historyPositions: "/oms/positions/history",
    historyOrders: "/oms/orders/history",
    ccxtOrders: "/ccxt/orders/raw",
    ccxtTrades: "/ccxt/trades/raw",
  };
  const path = pathByKind[kind];
  const query = new URLSearchParams({
    account_ids: csv,
    start_date: startDate,
    end_date: endDate,
    page: String(page),
    page_size: String(pageSize),
  });
  const out = await apiRequest(`${path}?${query.toString()}`, {}, cfg);
  const items = mergeById(out.items || []);
  state.tables[kind].setData(items);
  setPagerFromTotal(kind, Number(out.total || 0), Number(out.page || page), Number(out.page_size || pageSize));
}

const OMS_PAGER_KEYS = ["openPositions", "openOrders", "deals", "historyPositions", "historyOrders", "ccxtOrders", "ccxtTrades", "postTrading", "adminOms"];
const OMS_LOCAL_KEYS = new Set(["openPositions", "openOrders"]);

function updatePagerButtonI18n() {
  const prevTitle = t("common.previous", "Previous");
  const nextTitle = t("common.next", "Next");
  const copyTitle = t("common.copy", "Copy");
  const exportCsvTitle = t("common.export_csv", "Export CSV");
  const exportExcelTitle = t("common.export_excel", "Export Excel");
  const exportJsonTitle = t("common.export_json", "Export JSON");
  const exportHtmlTitle = t("common.export_html", "Export HTML");
  for (const key of OMS_PAGER_KEYS) {
    const prevBtn = document.getElementById(`${key}PrevPageBtn`);
    const nextBtn = document.getElementById(`${key}NextPageBtn`);
    const copyBtn = document.getElementById(`${key}CopyBtn`);
    const exportCsvBtn = document.getElementById(`${key}ExportCsvBtn`);
    const exportExcelBtn = document.getElementById(`${key}ExportExcelBtn`);
    const exportJsonBtn = document.getElementById(`${key}ExportJsonBtn`);
    const exportHtmlBtn = document.getElementById(`${key}ExportHtmlBtn`);
    if (prevBtn) {
      prevBtn.setAttribute("title", prevTitle);
      prevBtn.setAttribute("aria-label", prevTitle);
    }
    if (nextBtn) {
      nextBtn.setAttribute("title", nextTitle);
      nextBtn.setAttribute("aria-label", nextTitle);
    }
    if (copyBtn) {
      copyBtn.setAttribute("title", copyTitle);
      copyBtn.setAttribute("aria-label", copyTitle);
    }
    if (exportCsvBtn) {
      exportCsvBtn.setAttribute("title", exportCsvTitle);
      exportCsvBtn.setAttribute("aria-label", exportCsvTitle);
    }
    if (exportExcelBtn) {
      exportExcelBtn.setAttribute("title", exportExcelTitle);
      exportExcelBtn.setAttribute("aria-label", exportExcelTitle);
    }
    if (exportJsonBtn) {
      exportJsonBtn.setAttribute("title", exportJsonTitle);
      exportJsonBtn.setAttribute("aria-label", exportJsonTitle);
    }
    if (exportHtmlBtn) {
      exportHtmlBtn.setAttribute("title", exportHtmlTitle);
      exportHtmlBtn.setAttribute("aria-label", exportHtmlTitle);
    }
  }
}

function omsRowsForExport(key) {
  if (OMS_LOCAL_KEYS.has(key)) return sortByIdAsc(mergeById(state.omsLocalData[key] || []));
  if (key === "postTrading") {
    const rows = state.tables[key]?.getData() || [];
    return [...rows];
  }
  return sortByIdAsc(mergeById(state.tables[key]?.getData() || []));
}

function omsExportColumns(key) {
  const table = state.tables[key];
  if (!table) return [];
  return table.getColumns()
    .map((col) => col.getDefinition())
    .filter((def) => def && def.field && !String(def.field).startsWith("_"))
    .map((def) => ({ field: String(def.field), title: String(def.title || def.field) }));
}

function escapeCsvCell(value) {
  const text = value === null || value === undefined ? "" : String(value);
  if (!/[",\n\r]/.test(text)) return text;
  return `"${text.replace(/"/g, "\"\"")}"`;
}

function rowsToCsv(rows, columns) {
  const header = columns.map((col) => escapeCsvCell(col.title)).join(",");
  const body = rows.map((row) => columns.map((col) => escapeCsvCell(row[col.field])).join(",")).join("\n");
  return `${header}\n${body}`;
}

function rowsToHtml(rows, columns) {
  const esc = (value) => String(value === null || value === undefined ? "" : value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
  const head = columns.map((col) => `<th>${esc(col.title)}</th>`).join("");
  const body = rows.map((row) => `<tr>${columns.map((col) => `<td>${esc(row[col.field])}</td>`).join("")}</tr>`).join("\n");
  return `<!doctype html><html><head><meta charset="utf-8"><title>OMS Export</title></head><body><table border="1"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></body></html>`;
}

function downloadText(filename, text, mimeType = "text/plain;charset=utf-8") {
  const blob = new Blob([text], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function copyOmsTable(key) {
  const rows = omsRowsForExport(key);
  const columns = omsExportColumns(key);
  const csv = rowsToCsv(rows, columns);
  await navigator.clipboard.writeText(csv);
  uiLog("info", `OMS copy ok (${key})`, { rows: rows.length });
}

function exportOmsTable(key, format) {
  const rows = omsRowsForExport(key);
  const columns = omsExportColumns(key);
  const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
  if (format === "csv") {
    downloadText(`oms-${key}-${stamp}.csv`, rowsToCsv(rows, columns), "text/csv;charset=utf-8");
    return;
  }
  if (format === "excel") {
    downloadText(`oms-${key}-${stamp}.xls`, rowsToHtml(rows, columns), "application/vnd.ms-excel;charset=utf-8");
    return;
  }
  if (format === "json") {
    downloadText(`oms-${key}-${stamp}.json`, JSON.stringify(rows, null, 2), "application/json;charset=utf-8");
    return;
  }
  downloadText(`oms-${key}-${stamp}.html`, rowsToHtml(rows, columns), "text/html;charset=utf-8");
}

async function goToOmsPage(key, nextPage) {
  const pager = state.omsPager[key];
  if (!pager) return;
  const page = clampPage(nextPage, pager.totalPages);
  state.omsPager[key].page = page;
  if (key === "adminOms") {
    await loadAdminOms(false);
    return;
  }
  if (key === "postTrading") {
    await loadPostTradingPreview(false);
    return;
  }
  if (OMS_LOCAL_KEYS.has(key)) {
    renderLocalOmsPage(key);
    return;
  }
  await refreshHistoryTable(key, false);
}

async function setOmsPageSize(key, nextSize) {
  const size = Math.max(1, Math.min(500, Number(nextSize) || state.omsPageSize));
  state.omsPager[key].pageSize = size;
  state.omsPager[key].page = 1;
  if (key === "adminOms") {
    await loadAdminOms(true);
    return;
  }
  if (key === "postTrading") {
    await loadPostTradingPreview(true);
    return;
  }
  if (OMS_LOCAL_KEYS.has(key)) {
    renderLocalOmsPage(key);
    return;
  }
  await refreshHistoryTable(key, true);
}

async function refreshTables() {
  const cfg = requireConfig();
  const accountIds = await resolveViewAccountIds(cfg);
  const snapshot = await fetchCombinedSnapshot(accountIds, cfg);

  setLocalOmsRows("openOrders", snapshot.openOrders, 1);
  setLocalOmsRows("openPositions", snapshot.openPositions, 1);
  await Promise.all([
    refreshHistoryTable("deals", true),
    refreshHistoryTable("historyPositions", true),
    refreshHistoryTable("historyOrders", true),
    refreshHistoryTable("ccxtOrders", true),
    refreshHistoryTable("ccxtTrades", true),
  ]);

  status(`connected ${state.wsConnections.size} ws | viewing ${accountIds.length} account(s)`, state.connected);
}

async function refreshOpenPositionsTable() {
  const cfg = requireConfig();
  const accountIds = await resolveViewAccountIds(cfg);
  const csv = accountIdsCsv(accountIds);
  const out = await apiRequest(`/oms/positions/open?account_ids=${encodeURIComponent(csv)}&limit=5000`, {}, cfg);
  setLocalOmsRows("openPositions", out.items || []);
}

async function refreshOpenOrdersTable() {
  const cfg = requireConfig();
  const accountIds = await resolveViewAccountIds(cfg);
  const csv = accountIdsCsv(accountIds);
  const out = await apiRequest(`/oms/orders/open?account_ids=${encodeURIComponent(csv)}&limit=5000`, {}, cfg);
  setLocalOmsRows("openOrders", out.items || []);
}

async function cancelOpenOrderInline(row) {
  const cfg = requireConfig();
  const accountId = Number(row.account_id || 0);
  const orderId = Number(row.id || 0);
  if (!Number.isFinite(accountId) || accountId <= 0) throw new Error("order account_id inválido");
  if (!Number.isFinite(orderId) || orderId <= 0) throw new Error("order id inválido");
  const out = await apiRequest("/oms/commands", {
    method: "POST",
    body: { account_id: accountId, command: "cancel_order", payload: { order_id: orderId } },
  }, cfg);
  ensureOmsCommandSuccess(out, "cancel_order");
  eventLog("open_order_cancel_inline", { account_id: accountId, order_id: orderId, out });
  await refreshOpenOrdersTable();
}

async function changeOpenOrderInline(row) {
  const cfg = requireConfig();
  const accountId = Number(row.account_id || 0);
  const orderId = Number(row.id || 0);
  if (!Number.isFinite(accountId) || accountId <= 0) throw new Error("order account_id inválido");
  if (!Number.isFinite(orderId) || orderId <= 0) throw new Error("order id inválido");
  const payload = { order_id: orderId };
  const qty = String(row.qty ?? "").trim();
  const price = String(row.price ?? "").trim();
  if (qty) payload.new_qty = qty;
  if (price) payload.new_price = price;
  if (!payload.new_qty && !payload.new_price) throw new Error("edite qty e/ou price antes de aplicar");
  const out = await apiRequest("/oms/commands", {
    method: "POST",
    body: { account_id: accountId, command: "change_order", payload },
  }, cfg);
  ensureOmsCommandSuccess(out, "change_order");
  eventLog("open_order_change_inline", { account_id: accountId, order_id: orderId, out });
  await refreshOpenOrdersTable();
}

async function closeOpenPositionInline(row) {
  const cfg = requireConfig();
  const accountId = Number(row.account_id || 0);
  const positionId = Number(row.id || 0);
  if (!Number.isFinite(accountId) || accountId <= 0) throw new Error("position account_id inválido");
  if (!Number.isFinite(positionId) || positionId <= 0) throw new Error("position id inválido");
  const out = await apiRequest("/oms/commands", {
    method: "POST",
    body: {
      account_id: accountId,
      command: "close_position",
      payload: { position_id: positionId, order_type: "market" },
    },
  }, cfg);
  ensureOmsCommandSuccess(out, "close_position");
  eventLog("open_position_close_inline", { account_id: accountId, position_id: positionId, out });
  await refreshOpenPositionsTable();
}

async function changeOpenPositionInline(row) {
  const cfg = requireConfig();
  const accountId = Number(row.account_id || 0);
  const positionId = Number(row.id || 0);
  if (!Number.isFinite(accountId) || accountId <= 0) throw new Error("position account_id inválido");
  if (!Number.isFinite(positionId) || positionId <= 0) throw new Error("position id inválido");
  const payload = { position_id: positionId };
  const stopLossRaw = row.stop_loss;
  const stopGainRaw = row.stop_gain;
  const commentRaw = row.comment;
  if (stopLossRaw !== undefined) payload.oms_stop_loss = stopLossRaw === "" || stopLossRaw === null ? null : String(stopLossRaw).trim();
  if (stopGainRaw !== undefined) payload.oms_stop_gain = stopGainRaw === "" || stopGainRaw === null ? null : String(stopGainRaw).trim();
  if (commentRaw !== undefined) payload.comment = commentRaw === "" || commentRaw === null ? null : String(commentRaw);
  const out = await apiRequest("/oms/commands", {
    method: "POST",
    body: { account_id: accountId, command: "position_change", payload },
  }, cfg);
  ensureOmsCommandSuccess(out, "position_change");
  eventLog("open_position_change_inline", { account_id: accountId, position_id: positionId, out });
  await refreshOpenPositionsTable();
}

function closeCloseByModal() {
  state.closeBySourceRow = null;
  $("closeBySourceId").value = "";
  $("closeByTargetSelect").innerHTML = "";
  $("closeBySourceInfo").textContent = "";
  $("closeByModal").open = false;
}

function openCloseByModal(sourceRow) {
  const accountId = Number(sourceRow?.account_id || 0);
  const sourceId = Number(sourceRow?.id || 0);
  const symbol = String(sourceRow?.symbol || "");
  const side = String(sourceRow?.side || "").toLowerCase();
  if (!Number.isFinite(accountId) || accountId <= 0 || !Number.isFinite(sourceId) || sourceId <= 0) {
    throw new Error("source position inválida");
  }
  const opposite = side === "buy" ? "sell" : "buy";
  const openRows = state.tables.openPositions.getData() || [];
  const candidates = openRows.filter((row) => (
    Number(row.account_id) === accountId
    && Number(row.id) !== sourceId
    && String(row.symbol || "") === symbol
    && String(row.side || "").toLowerCase() === opposite
  ));
  if (candidates.length === 0) {
    throw new Error("nenhuma posição oposta encontrada para close by");
  }
  state.closeBySourceRow = { ...sourceRow };
  $("closeBySourceId").value = String(sourceId);
  $("closeBySourceInfo").textContent = `${t("closeby.source_prefix", "Source")} #${sourceId} | ${t("closeby.account", "account")} ${accountId} | ${symbol} | ${side}`;
  const targetSelect = $("closeByTargetSelect");
  targetSelect.innerHTML = "";
  for (const row of candidates) {
    const opt = document.createElement("option");
    opt.value = String(row.id);
    opt.textContent = `#${row.id} | ${row.side} | qty=${row.qty}`;
    targetSelect.appendChild(opt);
  }
  $("closeByModal").open = true;
}

async function confirmCloseByModal() {
  const source = state.closeBySourceRow;
  if (!source) throw new Error("source position ausente");
  const accountId = Number(source.account_id || 0);
  const positionIdA = Number(source.id || 0);
  const positionIdB = Number($("closeByTargetSelect").value || 0);
  if (!Number.isFinite(accountId) || accountId <= 0) throw new Error("account_id inválido");
  if (!Number.isFinite(positionIdA) || positionIdA <= 0) throw new Error("position_id_a inválido");
  if (!Number.isFinite(positionIdB) || positionIdB <= 0) throw new Error("position_id_b inválido");
  const cfg = requireConfig();
  const out = await apiRequest("/oms/commands", {
    method: "POST",
    body: {
      account_id: accountId,
      command: "close_by",
      payload: { position_id_a: positionIdA, position_id_b: positionIdB },
    },
  }, cfg);
  ensureOmsCommandSuccess(out, "close_by");
  eventLog("open_position_close_by_inline", {
    account_id: accountId,
    position_id_a: positionIdA,
    position_id_b: positionIdB,
    out,
  });
  closeCloseByModal();
  await refreshOpenPositionsTable();
}

function routeWs(msg, accountId = null) {
  const namespace = msg.namespace || "system";
  const eventName = msg.event || "event";
  const payload = { ...(msg.payload || {}) };
  if (payload.oms_stop_loss !== undefined && payload.stop_loss === undefined) payload.stop_loss = payload.oms_stop_loss;
  if (payload.oms_stop_gain !== undefined && payload.stop_gain === undefined) payload.stop_gain = payload.oms_stop_gain;
  const resolvedAccountId = payload.account_id || accountId;
  eventLog(`${namespace}:${eventName}`, { account_id: resolvedAccountId, payload: msg.payload || {} });

  if (!payload.account_id && resolvedAccountId) payload.account_id = resolvedAccountId;

  if (namespace === "position") {
    if (eventName === "snapshot_open_orders" && Array.isArray(payload.items)) {
      replaceLocalOmsRowsForAccount("openOrders", payload.account_id || accountId, payload.items);
      return;
    }
    if (eventName === "snapshot_open_positions" && Array.isArray(payload.items)) {
      replaceLocalOmsRowsForAccount("openPositions", payload.account_id || accountId, payload.items);
      return;
    }
    if (payload.order_id || payload.order_type || payload.exchange_order_id) {
      const orderRow = { ...payload };
      if (!orderRow.id && payload.order_id) orderRow.id = payload.order_id;
      if (orderRow.__deleted) {
        removeLocalOmsRow("openOrders", orderRow.id);
        return;
      }
      const closedStatuses = new Set(["FILLED", "CANCELED", "REJECTED", "CLOSED"]);
      if (closedStatuses.has(String(orderRow.status || "").toUpperCase())) {
        removeLocalOmsRow("openOrders", orderRow.id);
      } else {
        upsertLocalOmsRow("openOrders", orderRow);
      }
    }
    if (payload.exchange_trade_id || payload.position_id) {
      if (payload.__deleted && payload.id) {
        removeByKey(state.tables.deals, payload.id);
        return;
      }
      append(state.tables.deals, payload);
    }
    if (payload.state || payload.avg_price || payload.side) {
      const positionRow = { ...payload };
      if (!positionRow.id && payload.position_id) positionRow.id = payload.position_id;
      if (positionRow.__deleted) {
        removeLocalOmsRow("openPositions", positionRow.id);
        return;
      }
      const isClosed =
        String(positionRow.state || "").toLowerCase() === "closed" ||
        String(positionRow.qty || "") === "0";
      if (isClosed) {
        removeLocalOmsRow("openPositions", positionRow.id);
      } else {
        upsertLocalOmsRow("openPositions", positionRow);
      }
    }
  }

  if (namespace === "ccxt") {
    return;
  }
}

function persistCurrentValues() {
  const cfg = getConfig();
  pushHistory(STORAGE.baseUrls, cfg.baseUrl);
  pushHistory(STORAGE.apiKeys, cfg.apiKey);
  for (const id of cfg.viewAccountIds) pushHistory(STORAGE.accountIds, id);
  for (const id of collectFormAccountIds()) pushHistory(STORAGE.accountIds, id);
  [
    formStrategy("sendStrategyId"),
    formStrategy("ccxtStrategyId"),
  ].forEach((strategy) => pushHistory(STORAGE.strategyIds, strategy));
  renderAllHistories();
}

function renderAllHistories() {
  renderHistory("baseUrlHistory", loadHistory(STORAGE.baseUrls));
  renderHistory("apiKeyHistory", loadHistory(STORAGE.apiKeys));
  renderAccountHistory(loadHistory(STORAGE.accountIds));
  renderHistory("strategyHistory", loadHistory(STORAGE.strategyIds));
}

function disconnectWs() {
  if (state.pingTimer) {
    clearInterval(state.pingTimer);
    state.pingTimer = null;
  }
  for (const ws of state.wsConnections.values()) {
    ws.close();
  }
  state.wsConnections.clear();
  state.subscribedAccountIds = [];
  state.connected = false;
  status("disconnected", false);
}

async function connectWs() {
  const cfg = requireConfig();
  persistCurrentValues();
  disconnectWs();
  const accountIds = await resolveViewAccountIds(cfg);
  const url = wsUrlFromBase(cfg.baseUrl);
  const ws = new WebSocket(url);
  state.wsConnections.set("main", ws);
  state.subscribedAccountIds = [...accountIds];

  ws.onopen = async () => {
    ws.send(JSON.stringify({
      id: `auth-${Date.now()}`,
      action: "auth",
      payload: { api_key: cfg.apiKey },
    }));
    ws.send(JSON.stringify({
      id: `sub-${Date.now()}`,
      action: "subscribe",
      payload: {
        account_ids: accountIds,
        namespaces: ["position", "ccxt"],
        with_snapshot: true,
      },
    }));
    state.connected = true;
    status(`connected 1 ws | viewing ${accountIds.length} account(s)`, true);
  };
  ws.onmessage = (ev) => {
    try {
      const msg = JSON.parse(ev.data);
      routeWs(msg, null);
    } catch (err) {
      eventLog("ws_parse_error", { raw: ev.data, error: String(err) });
    }
  };
  ws.onerror = () => status("ws error", false);
  ws.onclose = () => {
    state.wsConnections.delete("main");
    status("disconnected", false);
    state.connected = false;
  };

  state.pingTimer = setInterval(() => {
    for (const [accountId, ws] of state.wsConnections.entries()) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ id: `ping-${accountId}-${Date.now()}`, action: "ping" }));
      }
    }
  }, 20000);
}

function omsRowNumberColumn(kind) {
  return {
    title: "#",
    field: "_rownum",
    width: 52,
    minWidth: 52,
    maxWidth: 52,
    hozAlign: "right",
    headerHozAlign: "right",
    headerSort: false,
    cssClass: "col-rownum",
    formatter: (cell) => {
      const pager = state.omsPager[kind] || { page: 1, pageSize: 100 };
      const page = Math.max(1, Number(pager.page) || 1);
      const pageSize = Math.max(1, Number(pager.pageSize) || 100);
      const pos = Number(cell.getRow().getPosition(true) || 1);
      const n = ((page - 1) * pageSize) + pos;
      return `<span class="rownum-badge">${n}</span>`;
    },
  };
}

function riskValueBadge(value, kind = "loss") {
  if (value === null || value === undefined || String(value).trim() === "") return "";
  const text = String(value).trim();
  const css = kind === "gain" ? "risk-pill-gain" : "risk-pill-loss";
  return `<span class="risk-pill ${css}">${text}</span>`;
}

function sideBadge(value) {
  const text = String(value || "").trim();
  const normalized = text.toLowerCase();
  if (normalized === "buy") return `<span class="side-pill side-buy">${text}</span>`;
  if (normalized === "sell") return `<span class="side-pill side-sell">${text}</span>`;
  return text;
}

function jsonCellFormatter(cell) {
  const v = cell.getValue();
  if (v === null || v === undefined) return "";
  if (typeof v === "string") return v;
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}

function adminOmsColumnsForView(view) {
  const shared = [
    { title: t("oms.col_id", "ID"), field: "id", width: 84, editor: "input" },
    { title: t("oms.col_account_id", "Account ID"), field: "account_id", width: 100, editor: "input" },
  ];
  if (view === "open_orders" || view === "history_orders") {
    return [
      ...shared,
      { title: t("oms.col_command_id", "Command ID"), field: "command_id", width: 100, editor: "input" },
      { title: t("oms.col_symbol", "Symbol"), field: "symbol", width: 120, editor: "input" },
      { title: t("oms.col_side", "Side"), field: "side", width: 90, editor: "input", formatter: (cell) => sideBadge(cell.getValue()) },
      { title: t("oms.col_order_type", "Order Type"), field: "order_type", width: 100, editor: "input" },
      { title: t("oms.col_status", "Status"), field: "status", width: 120, editor: "input" },
      { title: t("oms.col_strategy_id", "Strategy ID"), field: "strategy_id", width: 100, editor: "input" },
      { title: t("oms.col_position_id", "Position ID"), field: "position_id", width: 100, editor: "input" },
      { title: t("oms.col_qty", "Qty"), field: "qty", width: 100, editor: "input" },
      { title: t("oms.col_price", "Price"), field: "price", width: 110, editor: "input" },
      { title: t("oms.col_stop_loss", "Stop Loss"), field: "stop_loss", width: 120, editor: "input" },
      { title: t("oms.col_stop_gain", "Stop Gain"), field: "stop_gain", width: 120, editor: "input" },
      { title: t("oms.col_filled_qty", "Filled Qty"), field: "filled_qty", width: 100, editor: "input" },
      { title: t("oms.col_avg_fill_price", "Avg Fill Price"), field: "avg_fill_price", width: 120, editor: "input" },
      { title: t("oms.col_reason", "Reason"), field: "reason", width: 120, editor: "input" },
      { title: t("oms.col_comment", "Comment"), field: "comment", width: 180, editor: "input" },
      { title: t("oms.col_client_order_id", "Client Order ID"), field: "client_order_id", width: 140, editor: "input" },
      { title: t("oms.col_exchange_order_id", "Exchange Order ID"), field: "exchange_order_id", width: 150, editor: "input" },
      { title: t("oms.col_created_at", "Created At"), field: "created_at", width: 170, editor: "input" },
      { title: t("oms.col_updated_at", "Updated At"), field: "updated_at", width: 170, editor: "input" },
      { title: t("oms.col_closed_at", "Closed At"), field: "closed_at", width: 170, editor: "input" },
    ];
  }
  if (view === "open_positions" || view === "history_positions") {
    return [
      ...shared,
      { title: t("oms.col_symbol", "Symbol"), field: "symbol", width: 120, editor: "input" },
      { title: t("oms.col_strategy_id", "Strategy ID"), field: "strategy_id", width: 100, editor: "input" },
      { title: t("oms.col_side", "Side"), field: "side", width: 90, editor: "input", formatter: (cell) => sideBadge(cell.getValue()) },
      { title: t("oms.col_qty", "Qty"), field: "qty", width: 100, editor: "input" },
      { title: t("oms.col_avg_price", "Avg Price"), field: "avg_price", width: 120, editor: "input" },
      { title: t("oms.col_stop_loss", "Stop Loss"), field: "stop_loss", width: 120, editor: "input" },
      { title: t("oms.col_stop_gain", "Stop Gain"), field: "stop_gain", width: 120, editor: "input" },
      { title: t("oms.col_state", "State"), field: "state", width: 100, editor: "input" },
      { title: t("oms.col_reason", "Reason"), field: "reason", width: 120, editor: "input" },
      { title: t("oms.col_comment", "Comment"), field: "comment", width: 180, editor: "input" },
      { title: t("oms.col_opened_at", "Opened At"), field: "opened_at", width: 170, editor: "input" },
      { title: t("oms.col_updated_at", "Updated At"), field: "updated_at", width: 170, editor: "input" },
      { title: t("oms.col_closed_at", "Closed At"), field: "closed_at", width: 170, editor: "input" },
    ];
  }
  return [
    ...shared,
    { title: t("oms.col_order_id", "Order ID"), field: "order_id", width: 90, editor: "input" },
    { title: t("oms.col_position_id", "Position ID"), field: "position_id", width: 100, editor: "input" },
    { title: t("oms.col_symbol", "Symbol"), field: "symbol", width: 120, editor: "input" },
    { title: t("oms.col_side", "Side"), field: "side", width: 90, editor: "input", formatter: (cell) => sideBadge(cell.getValue()) },
    { title: t("oms.col_qty", "Qty"), field: "qty", width: 100, editor: "input" },
    { title: t("oms.col_price", "Price"), field: "price", width: 110, editor: "input" },
    { title: t("oms.col_fee", "Fee"), field: "fee", width: 100, editor: "input" },
    { title: t("oms.col_fee_currency", "Fee Ccy"), field: "fee_currency", width: 100, editor: "input" },
    { title: t("oms.col_pnl", "PnL"), field: "pnl", width: 100, editor: "input" },
    { title: t("oms.col_strategy_id", "Strategy ID"), field: "strategy_id", width: 100, editor: "input" },
    { title: t("oms.col_reason", "Reason"), field: "reason", width: 120, editor: "input" },
    { title: t("oms.col_comment", "Comment"), field: "comment", width: 180, editor: "input" },
    { title: t("oms.col_reconciled", "Reconciled"), field: "reconciled", width: 110, editor: true },
    { title: t("oms.col_exchange_trade_id", "Exchange Trade ID"), field: "exchange_trade_id", width: 150, editor: "input" },
    { title: t("oms.col_created_at", "Created At"), field: "created_at", width: 170, editor: "input" },
    { title: t("oms.col_executed_at", "Executed At"), field: "executed_at", width: 170, editor: "input" },
  ];
}

function setupTables() {
  state.tables.openPositions = makeTable("openPositionsTable", [
    omsRowNumberColumn("openPositions"),
    {
      title: "",
      field: "_close",
      width: 86,
      hozAlign: "left",
      headerHozAlign: "left",
      headerSort: false,
      formatter: () => `<button data-act='close' class='row-icon-btn' title='${t("common.close", "Close")}' aria-label='${t("common.close", "Close")}'><i class='fa-solid fa-xmark icon-danger' aria-hidden='true'></i></button> <button data-act='close_by' class='row-icon-btn' title='${t("cmd.position_close_by", "Position Close By")}' aria-label='${t("cmd.position_close_by", "Position Close By")}'><i class='fa-solid fa-right-left icon-ok' aria-hidden='true'></i></button>`,
      cellClick: async (ev, cell) => {
        const btn = ev?.target?.closest ? ev.target.closest("[data-act]") : null;
        const action = btn?.dataset?.act;
        if (!action) return;
        const row = cell.getRow().getData();
        try {
          if (action === "close") {
            await closeOpenPositionInline(row);
            return;
          }
          if (action === "close_by") {
            openCloseByModal(row);
            return;
          }
        } catch (err) {
          eventLog("open_position_inline_error", { action, error: String(err), row });
          uiLog("error", `Open position action failed (${action})`, { error: String(err), row });
        }
      },
    },
    { title: t("oms.col_id", "ID"), field: "id", width: 84 },
    { title: t("oms.col_account_id", "Account ID"), field: "account_id", width: 96 },
    { title: t("oms.col_strategy_id", "Strategy ID"), field: "strategy_id", width: 100 },
    { title: t("oms.col_symbol", "Symbol"), field: "symbol", width: 120 },
    { title: t("oms.col_side", "Side"), field: "side", width: 90, formatter: (cell) => sideBadge(cell.getValue()) },
    { title: t("oms.col_qty", "Qty"), field: "qty", width: 110 },
    { title: t("oms.col_avg_price", "Avg Price"), field: "avg_price", width: 130 },
    {
      title: t("oms.col_stop_loss", "Stop Loss"),
      field: "stop_loss",
      width: 120,
      editor: "input",
      formatter: (cell) => riskValueBadge(cell.getValue(), "loss"),
    },
    {
      title: t("oms.col_stop_gain", "Stop Gain"),
      field: "stop_gain",
      width: 120,
      editor: "input",
      formatter: (cell) => riskValueBadge(cell.getValue(), "gain"),
    },
    {
      title: "",
      field: "_save_targets",
      width: 46,
      hozAlign: "left",
      headerHozAlign: "left",
      headerSort: false,
      formatter: () => `<button data-act='save_targets' class='row-icon-btn' title='${t("common.apply", "Apply")}' aria-label='${t("common.apply", "Apply")}'><i class='fa-solid fa-floppy-disk icon-save' aria-hidden='true'></i></button>`,
      cellClick: async (ev, cell) => {
        const btn = ev?.target?.closest ? ev.target.closest("[data-act]") : null;
        const action = btn?.dataset?.act;
        if (action !== "save_targets") return;
        const row = cell.getRow().getData();
        try {
          await changeOpenPositionInline(row);
        } catch (err) {
          eventLog("open_position_inline_error", { action, error: String(err), row });
          uiLog("error", `Open position action failed (${action})`, { error: String(err), row });
        }
      },
    },
    { title: t("oms.col_state", "State"), field: "state", width: 90 },
    { title: t("oms.col_reason", "Reason"), field: "reason", width: 120 },
    { title: t("oms.col_comment", "Comment"), field: "comment", width: 180, editor: "input" },
    {
      title: "",
      field: "_save_comment",
      width: 46,
      hozAlign: "left",
      headerHozAlign: "left",
      headerSort: false,
      formatter: () => `<button data-act='save_comment' class='row-icon-btn' title='${t("common.apply", "Apply")}' aria-label='${t("common.apply", "Apply")}'><i class='fa-solid fa-floppy-disk icon-save' aria-hidden='true'></i></button>`,
      cellClick: async (ev, cell) => {
        const btn = ev?.target?.closest ? ev.target.closest("[data-act]") : null;
        const action = btn?.dataset?.act;
        if (action !== "save_comment") return;
        const row = cell.getRow().getData();
        try {
          await changeOpenPositionInline(row);
        } catch (err) {
          eventLog("open_position_inline_error", { action, error: String(err), row });
          uiLog("error", `Open position action failed (${action})`, { error: String(err), row });
        }
      },
    },
    { title: t("oms.col_opened_at", "Opened At"), field: "opened_at", width: 170 },
    { title: t("oms.col_updated_at", "Updated At"), field: "updated_at", width: 170 },
    { title: t("oms.col_closed_at", "Closed At"), field: "closed_at", width: 170 },
  ]);
  state.tables.openOrders = makeTable("openOrdersTable", [
    omsRowNumberColumn("openOrders"),
    {
      title: "",
      field: "_close",
      width: 46,
      hozAlign: "left",
      headerHozAlign: "left",
      headerSort: false,
      formatter: () => `<button data-act='cancel' class='row-icon-btn' title='${t("common.cancel", "Cancel")}' aria-label='${t("common.cancel", "Cancel")}'><i class='fa-solid fa-xmark icon-danger' aria-hidden='true'></i></button>`,
      cellClick: async (ev, cell) => {
        const btn = ev?.target?.closest ? ev.target.closest("[data-act]") : null;
        const action = btn?.dataset?.act;
        if (!action) return;
        const row = cell.getRow().getData();
        try {
          if (action === "cancel") {
            await cancelOpenOrderInline(row);
          }
        } catch (err) {
          eventLog("open_order_inline_error", { action, error: String(err), row });
          uiLog("error", `Open order action failed (${action})`, { error: String(err), row });
        }
      },
    },
    { title: t("oms.col_command_id", "Command ID"), field: "command_id", width: 100 },
    { title: t("oms.col_id", "ID"), field: "id", width: 84 },
    { title: t("oms.col_account_id", "Account ID"), field: "account_id", width: 110 },
    { title: t("oms.col_strategy_id", "Strategy ID"), field: "strategy_id", width: 100 },
    { title: t("oms.col_position_id", "Position ID"), field: "position_id", width: 100 },
    { title: t("oms.col_symbol", "Symbol"), field: "symbol", width: 120 },
    { title: t("oms.col_side", "Side"), field: "side", width: 80, formatter: (cell) => sideBadge(cell.getValue()) },
    { title: t("oms.col_order_type", "Order Type"), field: "order_type", width: 100 },
    { title: t("oms.col_status", "Status"), field: "status", width: 110 },
    { title: t("oms.col_qty", "Qty"), field: "qty", width: 100, editor: "input" },
    { title: t("oms.col_price", "Price"), field: "price", width: 110, editor: "input" },
    { title: t("oms.col_stop_loss", "Stop Loss"), field: "stop_loss", width: 120 },
    { title: t("oms.col_stop_gain", "Stop Gain"), field: "stop_gain", width: 120 },
    { title: t("oms.col_filled_qty", "Filled Qty"), field: "filled_qty", width: 100 },
    { title: t("oms.col_avg_fill_price", "Avg Fill Price"), field: "avg_fill_price", width: 120 },
    { title: t("oms.col_reason", "Reason"), field: "reason", width: 120 },
    { title: t("oms.col_client_order_id", "Client Order ID"), field: "client_order_id", width: 140 },
    { title: t("oms.col_exchange_order_id", "Exchange Order ID"), field: "exchange_order_id", width: 140 },
    { title: t("oms.col_comment", "Comment"), field: "comment", width: 180 },
    { title: t("oms.col_created_at", "Created At"), field: "created_at", width: 170 },
    { title: t("oms.col_updated_at", "Updated At"), field: "updated_at", width: 170 },
    { title: t("oms.col_closed_at", "Closed At"), field: "closed_at", width: 170 },
    {
      title: "",
      field: "_save",
      width: 46,
      hozAlign: "right",
      headerHozAlign: "right",
      headerSort: false,
      formatter: () => `<button data-act='apply' class='row-icon-btn' title='${t("common.apply", "Apply")}' aria-label='${t("common.apply", "Apply")}'><i class='fa-solid fa-floppy-disk icon-save' aria-hidden='true'></i></button>`,
      cellClick: async (ev, cell) => {
        const btn = ev?.target?.closest ? ev.target.closest("[data-act]") : null;
        const action = btn?.dataset?.act;
        if (action !== "apply") return;
        const row = cell.getRow().getData();
        try {
          await changeOpenOrderInline(row);
        } catch (err) {
          eventLog("open_order_inline_error", { action, error: String(err), row });
          uiLog("error", `Open order action failed (${action})`, { error: String(err), row });
        }
      },
    },
  ]);
  state.tables.historyPositions = makeTable("historyPositionsTable", [
    omsRowNumberColumn("historyPositions"),
    { title: t("oms.col_id", "ID"), field: "id", width: 84 },
    { title: t("oms.col_account_id", "Account ID"), field: "account_id", width: 96 },
    { title: t("oms.col_strategy_id", "Strategy ID"), field: "strategy_id", width: 100 },
    { title: t("oms.col_symbol", "Symbol"), field: "symbol", width: 120 },
    { title: t("oms.col_side", "Side"), field: "side", width: 90, formatter: (cell) => sideBadge(cell.getValue()) },
    { title: t("oms.col_qty", "Qty"), field: "qty", width: 110 },
    { title: t("oms.col_avg_price", "Avg Price"), field: "avg_price", width: 130 },
    { title: t("oms.col_stop_loss", "Stop Loss"), field: "stop_loss", width: 120 },
    { title: t("oms.col_stop_gain", "Stop Gain"), field: "stop_gain", width: 120 },
    { title: t("oms.col_state", "State"), field: "state", width: 90 },
    { title: t("oms.col_reason", "Reason"), field: "reason", width: 120 },
    { title: t("oms.col_comment", "Comment"), field: "comment", width: 180 },
    { title: t("oms.col_opened_at", "Opened At"), field: "opened_at", width: 170 },
    { title: t("oms.col_updated_at", "Updated At"), field: "updated_at", width: 170 },
    { title: t("oms.col_closed_at", "Closed At"), field: "closed_at", width: 170 },
  ]);
  state.tables.historyOrders = makeTable("historyOrdersTable", [
    omsRowNumberColumn("historyOrders"),
    { title: t("oms.col_command_id", "Command ID"), field: "command_id", width: 100 },
    { title: t("oms.col_id", "ID"), field: "id", width: 84 },
    { title: t("oms.col_account_id", "Account ID"), field: "account_id", width: 96 },
    { title: t("oms.col_strategy_id", "Strategy ID"), field: "strategy_id", width: 100 },
    { title: t("oms.col_position_id", "Position ID"), field: "position_id", width: 100 },
    { title: t("oms.col_symbol", "Symbol"), field: "symbol", width: 120 },
    { title: t("oms.col_side", "Side"), field: "side", width: 80, formatter: (cell) => sideBadge(cell.getValue()) },
    { title: t("oms.col_order_type", "Order Type"), field: "order_type", width: 100 },
    { title: t("oms.col_status", "Status"), field: "status", width: 110 },
    { title: t("oms.col_qty", "Qty"), field: "qty", width: 100 },
    { title: t("oms.col_price", "Price"), field: "price", width: 110 },
    { title: t("oms.col_stop_loss", "Stop Loss"), field: "stop_loss", width: 120 },
    { title: t("oms.col_stop_gain", "Stop Gain"), field: "stop_gain", width: 120 },
    { title: t("oms.col_filled_qty", "Filled Qty"), field: "filled_qty", width: 100 },
    { title: t("oms.col_avg_fill_price", "Avg Fill Price"), field: "avg_fill_price", width: 120 },
    { title: t("oms.col_reason", "Reason"), field: "reason", width: 120 },
    { title: t("oms.col_comment", "Comment"), field: "comment", width: 180 },
    { title: t("oms.col_client_order_id", "Client Order ID"), field: "client_order_id", width: 140 },
    { title: t("oms.col_exchange_order_id", "Exchange Order ID"), field: "exchange_order_id", width: 140 },
    { title: t("oms.col_created_at", "Created At"), field: "created_at", width: 170 },
    { title: t("oms.col_updated_at", "Updated At"), field: "updated_at", width: 170 },
    { title: t("oms.col_closed_at", "Closed At"), field: "closed_at", width: 170 },
  ]);
  state.tables.deals = makeTable("dealsTable", [
    omsRowNumberColumn("deals"),
    { title: t("oms.col_id", "ID"), field: "id", width: 84 },
    { title: t("oms.col_account_id", "Account ID"), field: "account_id", width: 96 },
    { title: t("oms.col_order_id", "Order ID"), field: "order_id", width: 84 },
    { title: t("oms.col_position_id", "Position ID"), field: "position_id", width: 90 },
    { title: t("oms.col_strategy_id", "Strategy ID"), field: "strategy_id", width: 100 },
    { title: t("oms.col_symbol", "Symbol"), field: "symbol", width: 120 },
    { title: t("oms.col_side", "Side"), field: "side", width: 80, formatter: (cell) => sideBadge(cell.getValue()) },
    { title: t("oms.col_qty", "Qty"), field: "qty", width: 100 },
    { title: t("oms.col_price", "Price"), field: "price", width: 120 },
    { title: t("oms.col_fee", "Fee"), field: "fee", width: 100 },
    { title: t("oms.col_fee_currency", "Fee Ccy"), field: "fee_currency", width: 100 },
    { title: t("oms.col_pnl", "PnL"), field: "pnl", width: 100 },
    { title: t("oms.col_reason", "Reason"), field: "reason", width: 120 },
    { title: t("oms.col_reconciled", "Reconciled"), field: "reconciled", width: 110 },
    { title: t("oms.col_comment", "Comment"), field: "comment", width: 180 },
    { title: t("oms.col_exchange_trade_id", "Exchange Trade ID"), field: "exchange_trade_id", width: 130 },
    { title: t("oms.col_created_at", "Created At"), field: "created_at", width: 170 },
    { title: t("oms.col_executed_at", "Executed At"), field: "executed_at", width: 170 },
  ]);
  state.tables.ccxtTrades = makeTable("ccxtTradesTable", [
    omsRowNumberColumn("ccxtTrades"),
    { title: t("oms.col_id", "ID"), field: "id", width: 84 },
    { title: t("oms.col_account_id", "Account ID"), field: "account_id", width: 96 },
    { title: t("system.col_exchange_id", "Exchange ID"), field: "exchange_id", width: 140 },
    { title: t("oms.col_exchange_trade_id", "Exchange Trade ID"), field: "exchange_trade_id", width: 150 },
    { title: t("oms.col_exchange_order_id", "Exchange Order ID"), field: "exchange_order_id", width: 150 },
    { title: t("oms.col_symbol", "Symbol"), field: "symbol", width: 120 },
    { title: t("system.col_raw", "Raw"), field: "raw_json", widthGrow: 1, formatter: jsonCellFormatter },
    { title: t("system.col_observed_at", "Observed At"), field: "observed_at", width: 170 },
  ]);
  state.tables.ccxtOrders = makeTable("ccxtOrdersTable", [
    omsRowNumberColumn("ccxtOrders"),
    { title: t("oms.col_id", "ID"), field: "id", width: 84 },
    { title: t("oms.col_account_id", "Account ID"), field: "account_id", width: 96 },
    { title: t("system.col_exchange_id", "Exchange ID"), field: "exchange_id", width: 140 },
    { title: t("oms.col_exchange_order_id", "Exchange Order ID"), field: "exchange_order_id", width: 150 },
    { title: t("oms.col_client_order_id", "Client Order ID"), field: "client_order_id", width: 150 },
    { title: t("oms.col_symbol", "Symbol"), field: "symbol", width: 120 },
    { title: t("system.col_raw", "Raw"), field: "raw_json", widthGrow: 1, formatter: jsonCellFormatter },
    { title: t("system.col_observed_at", "Observed At"), field: "observed_at", width: 170 },
  ]);
  state.tables.events = makeTable("eventsTable", [
    { title: t("system.col_seq", "Seq"), field: "seq", width: 70 },
    { title: t("system.col_at", "At"), field: "at", width: 185 },
    { title: t("system.col_kind", "Kind"), field: "kind", width: 170 },
    { title: t("system.col_payload", "Payload"), field: "payload", widthGrow: 1 },
  ]);
  state.tables.uiLogs = makeTable("uiLogsTable", [
    { title: t("system.col_seq", "Seq"), field: "seq", width: 70 },
    { title: t("system.col_at", "At"), field: "at", width: 185 },
    { title: t("system.col_level", "Level"), field: "level", width: 110 },
    { title: t("system.col_message", "Message"), field: "message", width: 300 },
    { title: t("system.col_payload", "Payload"), field: "payload", widthGrow: 1 },
  ], {
    pagination: true,
    paginationMode: "local",
    paginationSize: 100,
    paginationSizeSelector: [50, 100, 200, 500, 1000],
  });
  state.tables.postTrading = makeTable("postTradingTable", [
    { title: t("oms.col_id", "ID"), field: "id", width: 84 },
    { title: t("oms.col_account_id", "Account ID"), field: "account_id", width: 100 },
    { title: t("system.col_exchange_id", "Exchange"), field: "exchange_id", width: 140 },
    { title: t("oms.col_exchange_order_id", "Exchange Order ID"), field: "exchange_order_id", width: 150 },
    { title: t("oms.col_symbol", "Symbol"), field: "symbol", width: 130 },
    { title: t("oms.col_side", "Side"), field: "side", width: 80, formatter: (cell) => sideBadge(cell.getValue()) },
    { title: t("oms.col_qty", "Qty"), field: "qty", width: 120, hozAlign: "right", headerHozAlign: "right" },
    { title: t("oms.col_price", "Price"), field: "price", width: 120, hozAlign: "right", headerHozAlign: "right" },
    { title: t("oms.col_filled_qty", "Filled Qty"), field: "filled_qty", width: 120, hozAlign: "right", headerHozAlign: "right" },
    { title: t("oms.col_avg_fill_price", "Avg Fill Price"), field: "avg_fill_price", width: 130, hozAlign: "right", headerHozAlign: "right" },
    { title: t("oms.col_status", "Status"), field: "status", width: 120 },
    { title: t("oms.col_reconciled", "Reconciled"), field: "reconciled", width: 110 },
    { title: t("post_trading.col_target_strategy_id", "Target Strategy ID"), field: "target_strategy_id", width: 140, editor: "input", hozAlign: "right", headerHozAlign: "right" },
    { title: t("oms.col_strategy_id", "Current Strategy ID"), field: "strategy_id", width: 140, hozAlign: "right", headerHozAlign: "right" },
    { title: t("oms.col_created_at", "Created At"), field: "created_at", width: 170 },
    { title: t("oms.col_executed_at", "Executed At"), field: "executed_at", width: 170 },
  ], {
    selectableRows: true,
    rowHeader: {
      formatter: "rowSelection",
      titleFormatter: "rowSelection",
      hozAlign: "center",
      headerSort: false,
      resizable: false,
      frozen: true,
      width: 40,
      minWidth: 40,
    },
  });
  state.tables.tradeStrategies = makeTable("tradeStrategiesTable", buildTradeStrategiesColumns());
  state.tables.adminOms = makeTable(
    "adminOmsTable",
    adminOmsColumnsForView("open_orders"),
    {
      selectableRows: true,
      movableColumns: true,
      maxHeight: "420px",
      rowHeader: {
        formatter: "rowSelection",
        titleFormatter: "rowSelection",
        hozAlign: "center",
        headerSort: false,
        resizable: false,
        frozen: true,
        width: 40,
        minWidth: 40,
      },
    },
  );
  state.tables.adminAccounts = makeTable("adminAccountsTable", [
    { title: "account_id", field: "account_id", width: 100 },
    { title: "label", field: "label", editor: "input", width: 180 },
    { title: "exchange_id", field: "exchange_id", editor: "input", width: 140 },
    {
      title: "position_mode",
      field: "position_mode",
      editor: "list",
      editorParams: { values: ["hedge", "netting", "strategy_netting"] },
      width: 150,
    },
    { title: "extra_config_json", field: "extra_config_json", editor: "textarea", width: 260 },
    { title: "testnet", field: "is_testnet", formatter: "tickCross", editor: true, width: 90 },
    {
      title: "status",
      field: "status",
      editor: "list",
      editorParams: { values: ["active", "blocked"] },
      width: 100,
    },
    { title: "reconcile_enabled", field: "reconcile_enabled", width: 120 },
    { title: "dispatcher_worker_hint", field: "dispatcher_worker_hint", width: 150 },
    { title: "raw_storage_mode", field: "raw_storage_mode", width: 130 },
    { title: "created_at", field: "created_at", width: 180 },
    { title: "api_key_enc", field: "api_key_enc", width: 240 },
    { title: "secret_enc", field: "secret_enc", width: 240 },
    { title: "passphrase_enc", field: "passphrase_enc", width: 220 },
    { title: "credentials_updated_at", field: "credentials_updated_at", width: 180 },
    { title: "set_api_key", field: "set_api_key", editor: "input", width: 180 },
    { title: "set_secret", field: "set_secret", editor: "input", width: 180 },
    { title: "set_passphrase", field: "set_passphrase", editor: "input", width: 180 },
    {
      title: "save",
      field: "_save",
      hozAlign: "center",
      formatter: () => t("common.save", "Save"),
      cellClick: async (_ev, cell) => {
        const row = cell.getRow().getData();
        try {
          const cfg = requireConfig();
          let extra = row.extra_config_json;
          if (typeof extra === "string") {
            extra = parseJsonInput(extra, {});
          }
          const body = {
            exchange_id: String(row.exchange_id || "").trim() || null,
            label: String(row.label || "").trim() || null,
            position_mode: String(row.position_mode || "").trim() || null,
            is_testnet: Boolean(row.is_testnet),
            status: String(row.status || "").trim() || null,
            extra_config_json: extra && typeof extra === "object" ? extra : {},
          };
          const credentials = {};
          if (String(row.set_api_key || "").trim()) credentials.api_key = String(row.set_api_key || "").trim();
          if (String(row.set_secret || "").trim()) credentials.secret = String(row.set_secret || "").trim();
          if (String(row.set_passphrase || "").trim()) credentials.passphrase = String(row.set_passphrase || "").trim();
          if (Object.keys(credentials).length > 0) body.credentials = credentials;
          const out = await apiRequest(`/admin/accounts/${row.account_id}`, {
            method: "PATCH",
            body,
          }, cfg);
          eventLog("admin_update_account", out);
          await loadAdminAccounts(cfg);
        } catch (err) {
          eventLog("admin_update_account_error", { error: String(err), account_id: row.account_id });
        }
      },
    },
  ]);
  state.tables.adminUsers = makeTable("adminUsersTable", [
    { title: "user_id", field: "user_id", width: 90 },
    { title: "user_name", field: "user_name", width: 190 },
    { title: "role", field: "role", width: 90 },
    { title: "status", field: "status", width: 100 },
    { title: "created_at", field: "created_at", width: 180 },
  ]);
  state.tables.adminUsersKeys = makeTable("adminUsersKeysTable", [
    { title: "user_id", field: "user_id", width: 90 },
    { title: "user_name", field: "user_name", width: 180 },
    { title: "role", field: "role", width: 90 },
    { title: "user_status", field: "user_status", width: 100 },
    { title: "api_key_id", field: "api_key_id", width: 100 },
    {
      title: "api_key_status",
      field: "api_key_status",
      editor: "list",
      editorParams: { values: ["active", "disabled"] },
      width: 130,
    },
    { title: "created_at", field: "created_at", width: 180 },
    {
      title: "",
      field: "_save",
      hozAlign: "center",
      headerSort: false,
      width: 56,
      minWidth: 56,
      formatter: () => '<i class="fa-solid fa-floppy-disk icon-save" title="Save" aria-hidden="true"></i>',
      cellClick: async (_ev, cell) => {
        const row = cell.getRow().getData();
        try {
          const cfg = requireConfig();
          const apiKeyId = Number(row.api_key_id || 0);
          if (!Number.isFinite(apiKeyId) || apiKeyId <= 0) throw new Error("api_key_id inválido");
          const status = String(row.api_key_status || "").trim().toLowerCase();
          if (!["active", "disabled"].includes(status)) throw new Error("api_key_status inválido");
          const out = await apiRequest(`/admin/api-keys/${apiKeyId}`, {
            method: "PATCH",
            body: { status },
          }, cfg);
          eventLog("admin_update_api_key_status", out);
          await loadAdminUsersKeys(cfg);
        } catch (err) {
          eventLog("admin_update_api_key_status_error", { error: String(err) });
          uiLog("error", "Admin update API key status failed", { error: String(err) });
        }
      },
    },
  ]);
  state.tables.userApiKeys = makeTable("userApiKeysTable", [
    { title: "api_key_id", field: "api_key_id", width: 110, hozAlign: "right", headerHozAlign: "right" },
    { title: "user_id", field: "user_id", width: 100, hozAlign: "right", headerHozAlign: "right" },
    { title: "user_name", field: "user_name", width: 200 },
    { title: "role", field: "role", width: 120 },
    {
      title: "status",
      field: "status",
      editor: "list",
      editorParams: { values: ["active", "disabled"] },
      width: 130,
    },
    { title: "created_at", field: "created_at", width: 180 },
    {
      title: "",
      field: "_save",
      hozAlign: "center",
      headerSort: false,
      width: 56,
      minWidth: 56,
      formatter: () => '<i class="fa-solid fa-floppy-disk icon-save" title="Save" aria-hidden="true"></i>',
      cellClick: async (_ev, cell) => {
        const row = cell.getRow().getData();
        try {
          const cfg = requireConfig();
          const apiKeyId = Number(row.api_key_id || 0);
          if (!Number.isFinite(apiKeyId) || apiKeyId <= 0) throw new Error("api_key_id invalido");
          const status = String(row.status || "").trim().toLowerCase();
          if (!["active", "disabled"].includes(status)) throw new Error("status invalido");
          const out = await apiRequest(`/user/api-keys/${apiKeyId}`, {
            method: "PATCH",
            body: { status },
          }, cfg);
          eventLog("user_update_api_key_status", out);
          await loadUserApiKeys(cfg);
          setUserMessage("success", t("user.api_key_status_updated", "Status da API Key atualizado."));
        } catch (err) {
          eventLog("user_update_api_key_status_error", { error: String(err) });
          setUserMessage("error", `${t("user.api_key_status_update_error", "Erro ao atualizar status da API Key")}: ${String(err)}`);
        }
      },
    },
  ]);
  state.tables.riskPermissions = makeTable("riskPermissionsTable", [
    { title: "api_key_id", field: "api_key_id", width: 90 },
    { title: "account_id", field: "account_id", width: 90 },
    { title: "can_read", field: "can_read", width: 90 },
    { title: "can_trade", field: "can_trade", width: 90 },
    { title: "can_close_position", field: "can_close_position", width: 130 },
    { title: "can_risk_manage", field: "can_risk_manage", width: 130 },
    { title: "can_block_new_positions", field: "can_block_new_positions", width: 160 },
    { title: "can_block_account", field: "can_block_account", width: 140 },
    { title: "restrict_to_strategies", field: "restrict_to_strategies", width: 150 },
    {
      title: "strategy_ids",
      field: "strategy_ids",
      width: 160,
      formatter: (cell) => {
        const arr = cell.getValue();
        return Array.isArray(arr) ? arr.join(",") : "";
      },
    },
    { title: "status", field: "status", width: 90 },
  ]);
}

async function loadAdminAccounts(cfgOverride = null) {
  const cfg = cfgOverride || requireConfig();
  const res = await apiRequest("/admin/accounts", {}, cfg);
  const rows = (res.items || []).map((item) => ({
    ...item,
    extra_config_json: JSON.stringify(item.extra_config_json || {}, null, 2),
    set_api_key: "",
    set_secret: "",
    set_passphrase: "",
  }));
  state.tables.adminAccounts.setData(rows);
}

async function loadAdminUsersKeys(cfgOverride = null) {
  const cfg = cfgOverride || requireConfig();
  const res = await apiRequest("/admin/users-api-keys", {}, cfg);
  state.tables.adminUsersKeys.setData(res.items || []);
}

async function loadAdminUsers(cfgOverride = null) {
  const cfg = cfgOverride || requireConfig();
  const res = await apiRequest("/admin/users", {}, cfg);
  state.tables.adminUsers.setData(res.items || []);
}

function setAdminSystemStatus(text, kind = "info") {
  const box = document.getElementById("adminSystemStatusBox");
  if (!box) return;
  box.textContent = String(text || "-");
  box.classList.remove("notice-info", "notice-success", "notice-error");
  if (kind === "success") box.classList.add("notice-success");
  else if (kind === "error") box.classList.add("notice-error");
  else box.classList.add("notice-info");
}

async function loadAdminSystemStatus(cfgOverride = null) {
  const cfg = cfgOverride || requireConfig();
  let health = null;
  let dispatcher = null;
  let healthErr = "";
  let dispatcherErr = "";
  try {
    const res = await fetch(`${cfg.baseUrl}/healthz`, { method: "GET" });
    if (!res.ok) throw new Error(`health ${res.status}`);
    health = await res.json();
  } catch (err) {
    healthErr = String(err);
  }
  try {
    const out = await apiRequest("/dispatcher/status", {}, cfg);
    dispatcher = out?.result || {};
  } catch (err) {
    dispatcherErr = String(err);
  }
  if (health && dispatcher) {
    const txt = `${t("admin.api_status", "API")}: ${String(health.status || "ok")} | ${t("admin.dispatcher_status", "Dispatcher")}: ok`;
    setAdminSystemStatus(txt, "success");
    return;
  }
  const txt = `${t("admin.api_status", "API")}: ${health ? "ok" : "error"}${healthErr ? ` (${healthErr})` : ""} | ${t("admin.dispatcher_status", "Dispatcher")}: ${dispatcher ? "ok" : "error"}${dispatcherErr ? ` (${dispatcherErr})` : ""}`;
  setAdminSystemStatus(txt, "error");
}

async function loadUserProfile(cfgOverride = null) {
  const cfg = cfgOverride || requireConfig();
  const res = await apiRequest("/user/profile", {}, cfg);
  $("userProfileUserId").value = String(res.user_id || "");
  $("userProfileName").value = String(res.user_name || "");
  $("userProfileRole").value = String(res.role || "");
  $("userProfileStatus").value = String(res.status || "");
  $("userProfileApiKeyId").value = String(res.api_key_id || "");
  return res;
}

async function loadUserApiKeys(cfgOverride = null) {
  const cfg = cfgOverride || requireConfig();
  const res = await apiRequest("/user/api-keys", {}, cfg);
  state.tables.userApiKeys.setData(res.items || []);
  return res;
}

async function loadTradeStrategies(cfgOverride = null) {
  const cfg = cfgOverride || requireConfig();
  const res = await apiRequest("/strategies", {}, cfg);
  const items = (res.items || []).slice().sort((a, b) => {
    const rank = (status) => {
      const s = String(status || "").toLowerCase();
      if (s === "active") return 0;
      if (s === "disabled") return 1;
      return 2;
    };
    const byStatus = rank(a?.status) - rank(b?.status);
    if (byStatus !== 0) return byStatus;
    const aid = Number(a?.strategy_id || 0);
    const bid = Number(b?.strategy_id || 0);
    return aid - bid;
  });
  state.tables.tradeStrategies.setData(items);
  renderSendStrategyOptions(items);
  for (const item of items) {
    if (Number(item.strategy_id) > 0) pushHistory(STORAGE.strategyIds, Number(item.strategy_id));
  }
  renderAllHistories();
}

async function loadRiskPermissions(cfgOverride = null) {
  const cfg = cfgOverride || requireConfig();
  const apiKeyId = Number($("riskApiKeyId").value || 0);
  if (!Number.isFinite(apiKeyId) || apiKeyId <= 0) {
    throw new Error("risk api_key_id is required");
  }
  const res = await apiRequest(`/admin/api-keys/${apiKeyId}/permissions`, {}, cfg);
  state.tables.riskPermissions.setData(res.items || []);
  eventLog("risk_load_permissions", { api_key_id: apiKeyId, rows: (res.items || []).length });
}

async function loadCcxtExchanges(cfgOverride = null) {
  const cfg = cfgOverride || requireConfig();
  const res = await apiRequest("/meta/exchanges", {}, cfg);
  const node = $("adminExchangeIdSelect");
  const current = String(node.value || "").trim();
  node.innerHTML = "";
  const toEngineId = (value) => {
    const raw = String(value || "").trim();
    if (!raw) return "";
    const lower = raw.toLowerCase();
    if (lower.startsWith("ccxt.") || lower.startsWith("ccxtpro.")) return raw;
    return `ccxt.${raw}`;
  };
  const items = (Array.isArray(res.items) ? res.items : []).map((x) => toEngineId(x)).filter((x) => !!x);
  for (const ex of items) {
    const opt = document.createElement("option");
    opt.value = String(ex);
    opt.textContent = String(ex);
    node.appendChild(opt);
  }
  if (current && items.includes(current)) node.value = current;
  else if (items.includes("ccxt.binance")) node.value = "ccxt.binance";
  else if (items[0]) node.value = String(items[0]);
}

async function loadAccountsByApiKey(cfgOverride = null) {
  const cfg = cfgOverride || requireConfig();
  const resAccounts = await apiRequest("/oms/accounts", {}, cfg);
  const items = (resAccounts.items || []).map((item) => ({
    account_id: Number(item.account_id),
    label: String(item.label || "").trim(),
    trader_name: String(item.trader_name || "").trim(),
    exchange_id: String(item.exchange_id || "unknown").trim() || "unknown",
  }));
  const ids = items.map((item) => item.account_id).filter((n) => Number.isFinite(n) && n > 0);
  state.accountLabels.clear();
  state.accountExchanges.clear();
  items.forEach((item) => {
    if (Number.isFinite(item.account_id) && item.account_id > 0) {
      const traderLabel = item.trader_name;
      const fallbackLabel = item.label;
      const displayLabel = state.userRole === "trader"
        ? (traderLabel || fallbackLabel)
        : (fallbackLabel || traderLabel);
      if (displayLabel) state.accountLabels.set(item.account_id, displayLabel);
      state.accountExchanges.set(item.account_id, item.exchange_id);
    }
  });
  state.availableAccountIds = ids;
  if (ids.length > 0) {
    renderViewAccountsOptions(ids);
    renderStrategyAccountsOptions(ids);
    ids.forEach((id) => pushHistory(STORAGE.accountIds, id));
    if (getSelectedViewAccountIds().length === 0) {
      for (const opt of $("viewAccountsSelect").options) opt.selected = true;
    }
    renderAllHistories();
    if (!$("sendAccountId").value) $("sendAccountId").value = String(ids[0]);
    if (!$("cancelOrderAccountId").value) $("cancelOrderAccountId").value = String(ids[0]);
    if (!$("ccxtAccountId").value) $("ccxtAccountId").value = String(ids[0]);
    if (!$("changeOrderAccountId").value) $("changeOrderAccountId").value = String(ids[0]);
    if (!$("positionChangeAccountId").value) $("positionChangeAccountId").value = String(ids[0]);
    if (!$("closeByAccountId").value) $("closeByAccountId").value = String(ids[0]);
    if (!$("riskAccountId").value) $("riskAccountId").value = String(ids[0]);
    if (!$("riskStrategyAccountId").value) $("riskStrategyAccountId").value = String(ids[0]);
    if (!$("riskPermAccountId").value) $("riskPermAccountId").value = String(ids[0]);
  }
  eventLog("accounts_loaded", { ids, labels: Object.fromEntries(state.accountLabels.entries()) });
}

function bindForms() {
  bindShellControls();
  bindSidebarMenu();
  $("themeModeBtn").addEventListener("click", () => cycleThemeMode());
  $("densityModeBtn").addEventListener("click", () => cycleDensityMode());
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => {
    if (state.themeMode === "system") applyThemeMode("system", false);
  });
  try {
    bindWaTabs();
  } catch (err) {
    eventLog("bind_wa_tabs_error", { error: String(err) });
  }
  const applyLoginMode = () => {
    const mode = $("loginMode").value;
    $("loginApiKeyFields").classList.toggle("is-hidden", mode !== "api_key");
    $("loginUserPassFields").classList.toggle("is-hidden", mode !== "user_password");
  };
  $("loginLanguage").addEventListener("change", () => {
    const next = String($("loginLanguage").value || "").trim();
    if (!next) return;
    state.locale = next;
    localStorage.setItem(STORAGE.language, next);
    applyLanguageTexts();
    applyThemeMode(state.themeMode, false);
    applyDensityMode(state.densityMode, false);
  });
  $("loginMode").addEventListener("change", applyLoginMode);
  applyLoginMode();
  setLoginMessage("info", "");

  $("loginForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const mode = $("loginMode").value;
      const loginBaseUrl = $("baseUrl").value.trim().replace(/\/+$/, "");
      if (!loginBaseUrl) throw new Error("base_url is required");
      if (mode === "api_key") {
        state.userRole = "";
        const key = $("apiKey").value.trim();
        if (!key) throw new Error("api_key is required");
        $("baseUrl").value = loginBaseUrl;
        $("apiKey").value = key;
        persistCurrentValues();
        await reloadUiLogsForCurrentScope();
        await loadAccountsByApiKey({ baseUrl: loginBaseUrl, apiKey: key, viewAccountIds: [] });
        await loadTradeStrategies({ baseUrl: loginBaseUrl, apiKey: key, viewAccountIds: [] });
        eventLog("login_api_key_ok", { base_url: loginBaseUrl });
        setLoginMessage("success", t("login.success", "Autenticação concluída com sucesso."));
      } else {
        const userName = $("loginUserName").value.trim();
        const password = $("loginPassword").value;
        if (!userName || !password) throw new Error("user_name e password sao obrigatorios");
        const payload = { user_name: userName, password };
        const res = await fetch(`${loginBaseUrl}/auth/login-password`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const text = await res.text();
        const json = text ? JSON.parse(text) : {};
        if (!res.ok) throw new Error(`${res.status} ${JSON.stringify(json)}`);
        $("baseUrl").value = loginBaseUrl;
        const token = String(json.token || "");
        $("apiKey").value = token;
        state.userRole = String(json.role || "").trim().toLowerCase();
        persistCurrentValues();
        await reloadUiLogsForCurrentScope();
        await loadAccountsByApiKey({ baseUrl: loginBaseUrl, apiKey: token, viewAccountIds: [] });
        await loadTradeStrategies({ baseUrl: loginBaseUrl, apiKey: token, viewAccountIds: [] });
        eventLog("login_user_password_ok", {
          user_id: json.user_id,
          role: json.role,
          api_key_id: json.api_key_id,
          expires_at: json.expires_at,
        });
        setLoginMessage("success", t("login.success", "Autenticação concluída com sucesso."));
      }
    } catch (err) {
      eventLog("login_error", { error: String(err) });
      setLoginMessage("error", `${t("login.error", "Erro na autenticação")}: ${String(err)}`);
    }
  });

  $("toggleApiKeyBtn").addEventListener("click", () => {
    const input = $("apiKey");
    const nextType = input.type === "password" ? "text" : "password";
    input.type = nextType;
    updateApiKeyToggleLabel();
  });
  $("toggleLoginPasswordBtn").addEventListener("click", () => {
    const input = $("loginPassword");
    const nextType = input.type === "password" ? "text" : "password";
    input.type = nextType;
    updateLoginPasswordToggleLabel();
  });

  $("connectBtn").addEventListener("click", async () => {
    try {
      await connectWs();
    } catch (err) {
      status(String(err), false);
    }
  });
  $("disconnectBtn").addEventListener("click", () => disconnectWs());
  $("refreshOmsBtn").addEventListener("click", async () => {
    try {
      await refreshTables();
      eventLog("refresh_ok", {});
    } catch (err) {
      eventLog("refresh_error", { error: String(err) });
    }
  });
  $("accountsBtn").addEventListener("click", async () => {
    try {
      await loadAccountsByApiKey();
    } catch (err) {
      eventLog("accounts_error", { error: String(err) });
    }
  });
  $("refreshOpenPositionsBtn").addEventListener("click", async () => {
    try {
      await refreshOpenPositionsTable();
      eventLog("refresh_open_positions_ok", {});
    } catch (err) {
      eventLog("refresh_open_positions_error", { error: String(err) });
    }
  });
  $("refreshOpenOrdersBtn").addEventListener("click", async () => {
    try {
      await refreshOpenOrdersTable();
      eventLog("refresh_open_orders_ok", {});
    } catch (err) {
      eventLog("refresh_open_orders_error", { error: String(err) });
    }
  });
  $("refreshDealsBtn").addEventListener("click", async () => {
    try {
      await refreshHistoryTable("deals", true);
      eventLog("refresh_deals_ok", {});
    } catch (err) {
      eventLog("refresh_deals_error", { error: String(err) });
    }
  });
  $("refreshHistoryPositionsBtn").addEventListener("click", async () => {
    try {
      await refreshHistoryTable("historyPositions", true);
      eventLog("refresh_history_positions_ok", {});
    } catch (err) {
      eventLog("refresh_history_positions_error", { error: String(err) });
    }
  });
  $("refreshHistoryOrdersBtn").addEventListener("click", async () => {
    try {
      await refreshHistoryTable("historyOrders", true);
      eventLog("refresh_history_orders_ok", {});
    } catch (err) {
      eventLog("refresh_history_orders_error", { error: String(err) });
    }
  });

  for (const key of OMS_PAGER_KEYS) {
    const prevBtn = document.getElementById(`${key}PrevPageBtn`);
    const nextBtn = document.getElementById(`${key}NextPageBtn`);
    const pageInput = document.getElementById(`${key}PageInput`);
    const pageSize = document.getElementById(`${key}PageSize`);
    const copyBtn = document.getElementById(`${key}CopyBtn`);
    const exportCsvBtn = document.getElementById(`${key}ExportCsvBtn`);
    const exportExcelBtn = document.getElementById(`${key}ExportExcelBtn`);
    const exportJsonBtn = document.getElementById(`${key}ExportJsonBtn`);
    const exportHtmlBtn = document.getElementById(`${key}ExportHtmlBtn`);
    if (prevBtn) {
      prevBtn.addEventListener("click", async () => {
        try {
          await goToOmsPage(key, (state.omsPager[key]?.page || 1) - 1);
        } catch (err) {
          eventLog("oms_pager_prev_error", { key, error: String(err) });
        }
      });
    }
    if (nextBtn) {
      nextBtn.addEventListener("click", async () => {
        try {
          await goToOmsPage(key, (state.omsPager[key]?.page || 1) + 1);
        } catch (err) {
          eventLog("oms_pager_next_error", { key, error: String(err) });
        }
      });
    }
    if (pageInput) {
      pageInput.addEventListener("keydown", async (ev) => {
        if (ev.key !== "Enter") return;
        ev.preventDefault();
        try {
          await goToOmsPage(key, Number(pageInput.value || 1));
        } catch (err) {
          eventLog("oms_pager_page_input_error", { key, error: String(err) });
        }
      });
      pageInput.addEventListener("blur", async () => {
        try {
          await goToOmsPage(key, Number(pageInput.value || 1));
        } catch (err) {
          eventLog("oms_pager_page_blur_error", { key, error: String(err) });
        }
      });
    }
    if (pageSize) {
      pageSize.addEventListener("change", async () => {
        try {
          await setOmsPageSize(key, Number(pageSize.value || state.omsPageSize));
        } catch (err) {
          eventLog("oms_pager_size_error", { key, error: String(err) });
        }
      });
    }
    if (copyBtn) {
      copyBtn.addEventListener("click", async () => {
        try {
          await copyOmsTable(key);
        } catch (err) {
          eventLog("oms_copy_error", { key, error: String(err) });
          uiLog("error", `OMS copy failed (${key})`, { error: String(err) });
        }
      });
    }
    if (exportCsvBtn) {
      exportCsvBtn.addEventListener("click", () => {
        try {
          exportOmsTable(key, "csv");
        } catch (err) {
          eventLog("oms_export_csv_error", { key, error: String(err) });
        }
      });
    }
    if (exportExcelBtn) {
      exportExcelBtn.addEventListener("click", () => {
        try {
          exportOmsTable(key, "excel");
        } catch (err) {
          eventLog("oms_export_excel_error", { key, error: String(err) });
        }
      });
    }
    if (exportJsonBtn) {
      exportJsonBtn.addEventListener("click", () => {
        try {
          exportOmsTable(key, "json");
        } catch (err) {
          eventLog("oms_export_json_error", { key, error: String(err) });
        }
      });
    }
    if (exportHtmlBtn) {
      exportHtmlBtn.addEventListener("click", () => {
        try {
          exportOmsTable(key, "html");
        } catch (err) {
          eventLog("oms_export_html_error", { key, error: String(err) });
        }
      });
    }
  }
  $("closeByModalCancelBtn").addEventListener("click", () => closeCloseByModal());
  $("closeByModalConfirmBtn").addEventListener("click", async () => {
    try {
      await confirmCloseByModal();
    } catch (err) {
      eventLog("open_position_close_by_inline_error", { error: String(err) });
      uiLog("error", "Open position action failed (close_by)", { error: String(err) });
    }
  });
  $("refreshCcxtOrdersBtn").addEventListener("click", async () => {
    try {
      await refreshHistoryTable("ccxtOrders", true);
      eventLog("refresh_ccxt_orders_ok", {});
    } catch (err) {
      eventLog("refresh_ccxt_orders_error", { error: String(err) });
      uiLog("error", "Refresh CCXT orders failed", { error: String(err) });
    }
  });
  $("refreshCcxtTradesBtn").addEventListener("click", async () => {
    try {
      await refreshHistoryTable("ccxtTrades", true);
      eventLog("refresh_ccxt_trades_ok", {});
    } catch (err) {
      eventLog("refresh_ccxt_trades_error", { error: String(err) });
      uiLog("error", "Refresh CCXT trades failed", { error: String(err) });
    }
  });
  $("postTradingPreviewBtn").addEventListener("click", async () => {
    try {
      await loadPostTradingPreview(true);
      eventLog("post_trading_preview_ok", {});
    } catch (err) {
      eventLog("post_trading_preview_error", { error: String(err) });
      setPostTradingMessage("error", `${t("post_trading.preview_error", "Preview failed")}: ${String(err)}`);
    }
  });
  $("postTradingReconcileBtn").addEventListener("click", async () => {
    try {
      await runPostTradingReconcile();
    } catch (err) {
      const msg = String(err || "");
      if (msg.includes("period_requires_date_range")) {
        setPostTradingMessage("error", t("post_trading.period_requires_date_range", "Para scope Período, preencha Data Início e Data Fim."));
      } else {
        setPostTradingMessage("error", `${t("post_trading.reconcile_error", "Erro ao reconciliar")}: ${msg}`);
      }
      eventLog("post_trading_reconcile_error", { error: String(err) });
    }
  });
  $("postTradingApplyBtn").addEventListener("click", async () => {
    try {
      await applyPostTradingReassign();
    } catch (err) {
      const msg = String(err || "");
      if (msg.includes("post_trading_select_rows")) {
        setPostTradingMessage("error", t("post_trading.select_rows", "Select one or more rows to apply reassign."));
      } else if (msg.includes("post_trading_target_strategy_required")) {
        setPostTradingMessage("error", t("post_trading.target_strategy_required", "Preencha o ID da estratégia alvo (> 0) nas linhas selecionadas."));
      } else {
        setPostTradingMessage("error", `${t("post_trading.apply_error", "Apply reassign failed")}: ${msg}`);
      }
      eventLog("post_trading_apply_error", { error: String(err) });
    }
  });
  $("clearEventsBtn").addEventListener("click", () => state.tables.events.clearData());
  $("clearUiLogsViewBtn").addEventListener("click", () => {
    state.tables.uiLogs.clearData();
    state.uiLogSeq = 0;
    updateUiLogsPagerInfo();
    uiLog("info", "UI logs view cleared", { scope_hash: currentUiLogScopeHash() });
  });
  $("clearUiLogsDbBtn").addEventListener("click", async () => {
    try {
      await clearUiLogsStorage();
      state.tables.uiLogs.clearData();
      state.uiLogSeq = 0;
      updateUiLogsPagerInfo();
      uiLog("warn", "UI logs local db cleared", { scope_hash: currentUiLogScopeHash() });
    } catch (err) {
      uiLog("error", "UI logs local db clear failed", { error: String(err) });
    }
  });
  $("uiLogsPrevPageBtn").addEventListener("click", async () => {
    try {
      await goToUiLogsPage(Number(state.tables.uiLogs.getPage?.() || 1) - 1);
    } catch (err) {
      eventLog("ui_logs_pager_prev_error", { error: String(err) });
    }
  });
  $("uiLogsNextPageBtn").addEventListener("click", async () => {
    try {
      await goToUiLogsPage(Number(state.tables.uiLogs.getPage?.() || 1) + 1);
    } catch (err) {
      eventLog("ui_logs_pager_next_error", { error: String(err) });
    }
  });
  $("uiLogsPageInput").addEventListener("keydown", async (ev) => {
    if (ev.key !== "Enter") return;
    ev.preventDefault();
    try {
      await goToUiLogsPage(Number($("uiLogsPageInput").value || 1));
    } catch (err) {
      eventLog("ui_logs_pager_input_error", { error: String(err) });
    }
  });
  $("uiLogsPageInput").addEventListener("blur", async () => {
    try {
      await goToUiLogsPage(Number($("uiLogsPageInput").value || 1));
    } catch (err) {
      eventLog("ui_logs_pager_blur_error", { error: String(err) });
    }
  });
  $("uiLogsPageSize").addEventListener("change", async () => {
    try {
      await setUiLogsPageSize(Number($("uiLogsPageSize").value || 100));
    } catch (err) {
      eventLog("ui_logs_pager_size_error", { error: String(err) });
    }
  });
  $("selectAllAccountsBtn").addEventListener("click", () => {
    for (const opt of $("viewAccountsSelect").options) opt.selected = true;
  });
  $("clearAccountsBtn").addEventListener("click", () => {
    for (const opt of $("viewAccountsSelect").options) opt.selected = false;
  });
  $("tradeStrategyAccountsAllBtn").addEventListener("click", () => {
    for (const opt of $("tradeStrategyAccountIds").options) opt.selected = true;
  });
  $("tradeStrategyAccountsClearBtn").addEventListener("click", () => {
    for (const opt of $("tradeStrategyAccountIds").options) opt.selected = false;
  });
  $("goToCreateStrategyBtn").addEventListener("click", () => {
    switchTab("strategies");
    const node = document.getElementById("tradeCreateStrategyForm");
    const nameInput = node ? node.querySelector("input[name='name']") : null;
    if (nameInput && typeof nameInput.focus === "function") nameInput.focus();
  });
  const applyCcxtMode = () => {
    const mode = $("ccxtMode").value;
    $("ccxtGenericFields").classList.toggle("is-hidden", mode !== "generic");
    $("ccxtCoreFields").classList.toggle("is-hidden", mode !== "core");
  };

  const defaultCoreBodies = {
    fetch_balance: {},
    fetch_open_orders: { symbol: "BTC/USDT", since: null, limit: 200, params: {} },
    fetch_order: { id: "", symbol: null, params: {} },
    cancel_order: { id: "", symbol: null, params: {} },
    create_order: { symbol: "BTC/USDT", side: "buy", order_type: "market", amount: "0.001", params: {} },
  };

  const applyCoreBodyPreset = () => {
    const func = $("ccxtCoreFunc").value;
    const preset = defaultCoreBodies[func] || {};
    $("ccxtCoreBody").value = JSON.stringify(preset, null, 2);
  };

  $("ccxtMode").addEventListener("change", applyCcxtMode);
  $("ccxtCoreFunc").addEventListener("change", applyCoreBodyPreset);
  applyCcxtMode();
  applyCoreBodyPreset();

  $("loadSymbolsBtn").addEventListener("click", async () => {
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("sendAccountId", "send_order");
      const out = await apiRequest(`/ccxt/${accountId}/load_markets`, {
        method: "POST",
        body: { args: [], kwargs: {} },
      }, cfg);
      const result = out.result;
      let symbols = [];
      if (Array.isArray(result)) {
        symbols = result
          .map((item) => {
            if (!item || typeof item !== "object") return "";
            const symbol = String(item.symbol || "").trim();
            if (symbol) return symbol;
            const id = String(item.id || "").trim();
            if (id) return id;
            const base = String(item.base || "").trim();
            const quote = String(item.quote || "").trim();
            if (base && quote) return `${base}/${quote}`;
            return "";
          })
          .filter((s) => !!s);
      } else if (result && typeof result === "object") {
        symbols = Object.entries(result).map(([key, value]) => {
          const k = String(key || "").trim();
          const v = (value && typeof value === "object") ? value : {};
          const symbol = String(v.symbol || "").trim();
          if (symbol) return symbol;
          if (k) return k;
          const id = String(v.id || "").trim();
          if (id) return id;
          const base = String(v.base || "").trim();
          const quote = String(v.quote || "").trim();
          if (base && quote) return `${base}/${quote}`;
          return "";
        }).filter((s) => !!s);
      }
      symbols = [...new Set(symbols)].sort();
      renderHistory("symbolHistory", symbols.slice(0, 2000));
      if (!$("sendSymbol").value && symbols[0]) $("sendSymbol").value = symbols[0];
      eventLog("load_symbols", { account_id: accountId, count: symbols.length });
      uiLog("info", "Load symbols ok", {
        account_id: accountId,
        count: symbols.length,
        sample: symbols.slice(0, 5),
      });
    } catch (err) {
      eventLog("load_symbols_error", { error: String(err) });
      uiLog("error", "Load symbols failed", { error: String(err) });
    }
  });

  $("sendOrderForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const fd = new FormData(ev.currentTarget);
      const accountId = requireAccountId("sendAccountId", "send_order");
      const strategyId = formStrategy("sendStrategyId");
      const body = {
        account_id: accountId,
        command: "send_order",
        payload: {
          symbol: String(fd.get("symbol") || ""),
          side: String(fd.get("side") || "buy"),
          order_type: String(fd.get("order_type") || "market"),
          qty: String(fd.get("qty") || ""),
          strategy_id: strategyId,
          position_id: Number(fd.get("position_id") || 0) || 0,
          post_only: fd.get("post_only") === "on",
          reduce_only: fd.get("reduce_only") === "on",
        },
      };
      const tif = String(fd.get("time_in_force") || "").trim();
      const price = String(fd.get("price") || "").trim();
      const triggerPrice = String(fd.get("trigger_price") || "").trim();
      const stopPrice = String(fd.get("stop_price") || "").trim();
      const takeProfitPrice = String(fd.get("take_profit_price") || "").trim();
      const trailingAmount = String(fd.get("trailing_amount") || "").trim();
      const trailingPercent = String(fd.get("trailing_percent") || "").trim();
      if (price) body.payload.price = price;
      if (tif) body.payload.time_in_force = tif;
      if (triggerPrice) body.payload.trigger_price = triggerPrice;
      if (stopPrice) body.payload.stop_price = stopPrice;
      if (takeProfitPrice) body.payload.take_profit_price = takeProfitPrice;
      if (trailingAmount) body.payload.trailing_amount = trailingAmount;
      if (trailingPercent) body.payload.trailing_percent = trailingPercent;
      const stopLoss = String(fd.get("oms_stop_loss") || "").trim();
      const stopGain = String(fd.get("oms_stop_gain") || "").trim();
      const comment = String(fd.get("comment") || "").trim();
      const paramsJson = String(fd.get("params_json") || "").trim();
      if (stopLoss) body.payload.oms_stop_loss = stopLoss;
      if (stopGain) body.payload.oms_stop_gain = stopGain;
      if (comment) body.payload.comment = comment;
      if (paramsJson) body.payload.params = parseJsonInput(paramsJson, {});
      const out = await apiRequest("/oms/commands", { method: "POST", body }, cfg);
      eventLog("send_order", out);
    } catch (err) {
      eventLog("send_order_error", { error: String(err) });
    }
  });

  $("cancelOrderForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("cancelOrderAccountId", "cancel_order");
      const fd = new FormData(ev.currentTarget);
      const csv = String(fd.get("order_ids_csv") || "").trim();
      if (!csv) throw new Error("order_ids_csv is required");
      const ids = parseCsvIntList(csv);
      if (ids.length === 0) throw new Error("nenhum order_id válido no CSV");
      const body = {
        account_id: accountId,
        command: "cancel_order",
        payload: {
          order_id: ids.length === 1 ? ids[0] : null,
          order_ids: ids,
          order_ids_csv: csv,
        },
      };
      const out = await apiRequest("/oms/commands", { method: "POST", body }, cfg);
      eventLog("cancel_order", { account_id: accountId, count: ids.length, out });
    } catch (err) {
      eventLog("cancel_order_error", { error: String(err) });
    }
  });
  $("cancelAllBtn").addEventListener("click", async () => {
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("cancelOrderAccountId", "cancel_all_orders");
      const strategiesCsv = String($("cancelAllStrategyIdsCsv").value || "").trim();
      const strategyIds = parseCsvIntList(strategiesCsv);
      const body = {
        account_id: accountId,
        command: "cancel_all_orders",
        payload: {
          strategy_ids: strategyIds,
          strategy_ids_csv: strategiesCsv || null,
        },
      };
      const out = await apiRequest("/oms/commands", { method: "POST", body }, cfg);
      eventLog("cancel_all_orders", { account_id: accountId, strategy_ids: strategyIds, out });
    } catch (err) {
      eventLog("cancel_all_orders_error", { error: String(err) });
    }
  });

  $("changeOrderForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("changeOrderAccountId", "change_order");
      const fd = new FormData(ev.currentTarget);
      const orderId = Number(fd.get("order_id"));
      const payload = { order_id: orderId };
      const newQty = String(fd.get("new_qty") || "").trim();
      const newPrice = String(fd.get("new_price") || "").trim();
      if (newQty) payload.new_qty = newQty;
      if (newPrice) payload.new_price = newPrice;
      const body = { account_id: accountId, command: "change_order", payload };
      const out = await apiRequest("/oms/commands", { method: "POST", body }, cfg);
      eventLog("change_order", { account_id: accountId, out });
    } catch (err) {
      eventLog("change_order_error", { error: String(err) });
    }
  });

  $("positionChangeForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("positionChangeAccountId", "position_change");
      const fd = new FormData(ev.currentTarget);
      const positionId = Number(fd.get("position_id"));
      const payload = { position_id: positionId };
      const stopLoss = String(fd.get("oms_stop_loss") || "").trim();
      const stopGain = String(fd.get("oms_stop_gain") || "").trim();
      const comment = String(fd.get("comment") || "").trim();
      const useStopLoss = $("positionChangeUseStopLoss").checked;
      const useStopGain = $("positionChangeUseStopGain").checked;
      const useComment = $("positionChangeUseComment").checked;
      if (useStopLoss) payload.oms_stop_loss = stopLoss ? stopLoss : null;
      if (useStopGain) payload.oms_stop_gain = stopGain ? stopGain : null;
      if (useComment) payload.comment = comment ? comment : null;
      if (!useStopLoss && !useStopGain && !useComment) {
        throw new Error("marque ao menos um campo para alterar");
      }
      const body = { account_id: accountId, command: "position_change", payload };
      const out = await apiRequest("/oms/commands", { method: "POST", body }, cfg);
      eventLog("position_change", { account_id: accountId, out });
    } catch (err) {
      eventLog("position_change_error", { error: String(err) });
    }
  });

  $("closePositionForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("closePositionAccountId", "close_position");
      const fd = new FormData(ev.currentTarget);
      const positionId = Number(fd.get("position_id"));
      if (!Number.isFinite(positionId) || positionId <= 0) throw new Error("position_id is required");
      const stopLoss = String(fd.get("oms_stop_loss") || "").trim();
      const stopGain = String(fd.get("oms_stop_gain") || "").trim();
      const comment = String(fd.get("comment") || "").trim();
      const paramsJson = String(fd.get("params_json") || "").trim();
      const payload = {
        position_id: positionId,
        order_type: "market",
        post_only: fd.get("post_only") === "on",
        reduce_only: fd.get("reduce_only") === "on",
      };
      if (stopLoss) payload.oms_stop_loss = stopLoss;
      if (stopGain) payload.oms_stop_gain = stopGain;
      if (comment) payload.comment = comment;
      if (paramsJson) payload.params = parseJsonInput(paramsJson, {});
      const body = { account_id: accountId, command: "close_position", payload };
      const out = await apiRequest("/oms/commands", { method: "POST", body }, cfg);
      eventLog("close_position", { account_id: accountId, position_id: positionId, out });
    } catch (err) {
      eventLog("close_position_error", { error: String(err) });
    }
  });

  $("closeByForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("closeByAccountId", "close_by");
      const fd = new FormData(ev.currentTarget);
      const positionIdA = Number(fd.get("position_id_a"));
      const positionIdB = Number(fd.get("position_id_b"));
      if (!Number.isFinite(positionIdA) || positionIdA <= 0) throw new Error("position_id_a is required");
      if (!Number.isFinite(positionIdB) || positionIdB <= 0) throw new Error("position_id_b is required");
      const qtyRaw = String(fd.get("qty") || "").trim();
      const body = {
        account_id: accountId,
        command: "close_by",
        payload: {
          position_id_a: positionIdA,
          position_id_b: positionIdB,
        },
      };
      if (qtyRaw) body.payload.qty = qtyRaw;
      const out = await apiRequest("/oms/commands", { method: "POST", body }, cfg);
      eventLog("close_by", { account_id: accountId, out });
    } catch (err) {
      eventLog("close_by_error", { error: String(err) });
    }
  });

  $("mergePositionsForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("mergePositionsAccountId", "merge_positions");
      const fd = new FormData(ev.currentTarget);
      const sourceId = Number(fd.get("source_position_id"));
      const targetId = Number(fd.get("target_position_id"));
      if (!Number.isFinite(sourceId) || sourceId <= 0) throw new Error("source_position_id is required");
      if (!Number.isFinite(targetId) || targetId <= 0) throw new Error("target_position_id is required");
      if (sourceId === targetId) throw new Error("source_position_id and target_position_id must differ");
      const payload = {
        source_position_id: sourceId,
        target_position_id: targetId,
        stop_mode: String(fd.get("stop_mode") || "keep").trim() || "keep",
      };
      const stopLoss = String(fd.get("oms_stop_loss") || "").trim();
      const stopGain = String(fd.get("oms_stop_gain") || "").trim();
      if (stopLoss) payload.oms_stop_loss = stopLoss;
      if (stopGain) payload.oms_stop_gain = stopGain;
      const body = { account_id: accountId, command: "merge_positions", payload };
      const out = await apiRequest("/oms/commands", { method: "POST", body }, cfg);
      eventLog("merge_positions", { account_id: accountId, out });
    } catch (err) {
      eventLog("merge_positions_error", { error: String(err) });
    }
  });

  $("ccxtCallForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("ccxtAccountId", "ccxt_call");
      const strategyId = formStrategy("ccxtStrategyId");
      const fd = new FormData(ev.currentTarget);
      const mode = $("ccxtMode").value;
      if (mode === "core") {
        const func = String(fd.get("core_func") || "").trim();
        const body = parseJsonInput(String(fd.get("core_body") || "{}"), {});
        const out = await apiRequest(`/ccxt/core/${accountId}/${encodeURIComponent(func)}`, {
          method: "POST",
          body,
        }, cfg);
        setCcxtResultBox(out);
        eventLog("ccxt_core_call", { strategy_id: strategyId, func, out });
        const result = out.result || {};
        result.account_id = result.account_id || accountId;
        if (result.exchange_trade_id || func.includes("trade")) append(state.tables.ccxtTrades, result);
        if (result.exchange_order_id || func.includes("order")) append(state.tables.ccxtOrders, result);
      } else {
        const func = String(fd.get("func") || "").trim();
        const args = parseJsonInput(String(fd.get("args") || "[]"), []);
        const kwargs = parseJsonInput(String(fd.get("kwargs") || "{}"), {});
        const out = await apiRequest(`/ccxt/${accountId}/${encodeURIComponent(func)}`, {
          method: "POST",
          body: { args, kwargs },
        }, cfg);
        setCcxtResultBox(out);
        eventLog("ccxt_call", { strategy_id: strategyId, func, out });
        const result = out.result || {};
        result.account_id = result.account_id || accountId;
        if (result.exchange_trade_id || func.includes("trade")) append(state.tables.ccxtTrades, result);
        if (result.exchange_order_id || func.includes("order")) append(state.tables.ccxtOrders, result);
      }
    } catch (err) {
      setCcxtResultBox({ ok: false, error: String(err) });
      eventLog("ccxt_call_error", { error: String(err) });
    }
  });

  $("adminCreateAccountForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const fd = new FormData(ev.currentTarget);
      const out = await apiRequest("/admin/accounts", {
        method: "POST",
        body: {
          exchange_id: String(fd.get("exchange_id") || "").trim(),
          label: String(fd.get("label") || "").trim(),
          position_mode: String(fd.get("position_mode") || "hedge"),
          is_testnet: fd.get("is_testnet") === "on",
          extra_config_json: parseJsonInput(String(fd.get("extra_config_json") || "{}"), {}),
        },
      }, cfg);
      eventLog("admin_create_account", out);
      await loadAccountsByApiKey(cfg);
      await loadAdminAccounts(cfg);
    } catch (err) {
      eventLog("admin_create_account_error", { error: String(err) });
    }
  });

  $("userProfileForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const userName = String($("userProfileName").value || "").trim();
      if (!userName) throw new Error("user_name is required");
      const out = await apiRequest("/user/profile", {
        method: "PATCH",
        body: { user_name: userName },
      }, cfg);
      eventLog("user_profile_update", out);
      await loadUserProfile(cfg);
      setUserMessage("success", t("user.profile_updated", "Perfil atualizado com sucesso."));
    } catch (err) {
      eventLog("user_profile_update_error", { error: String(err) });
      setUserMessage("error", `${t("user.profile_update_error", "Erro ao atualizar perfil")}: ${String(err)}`);
    }
  });

  $("userPasswordForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const currentPassword = String($("userCurrentPassword").value || "");
      const newPassword = String($("userNewPassword").value || "");
      if (!currentPassword || !newPassword) throw new Error("current_password and new_password are required");
      const out = await apiRequest("/user/password", {
        method: "POST",
        body: { current_password: currentPassword, new_password: newPassword },
      }, cfg);
      eventLog("user_password_update", out);
      $("userCurrentPassword").value = "";
      $("userNewPassword").value = "";
      setUserMessage("success", t("user.password_updated", "Senha atualizada com sucesso."));
    } catch (err) {
      eventLog("user_password_update_error", { error: String(err) });
      setUserMessage("error", `${t("user.password_update_error", "Erro ao atualizar senha")}: ${String(err)}`);
    }
  });

  $("userCreateApiKeyForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const apiKeyRaw = String($("userNewApiKeyValue").value || "").trim();
      const body = {};
      if (apiKeyRaw) body.api_key = apiKeyRaw;
      const out = await apiRequest("/user/api-keys", {
        method: "POST",
        body,
      }, cfg);
      eventLog("user_create_api_key", out);
      $("userNewApiKeyValue").value = "";
      const box = $("userCreatedApiKeyBox");
      box.textContent = t("user.api_key_created", "API Key criada. id={api_key_id} key={key}")
        .replace("{api_key_id}", String(out.api_key_id || ""))
        .replace("{key}", String(out.api_key_plain || ""));
      box.classList.remove("is-hidden", "notice-error");
      box.classList.add("notice-info");
      await loadUserApiKeys(cfg);
      setUserMessage("success", t("user.api_key_created_success", "API Key criada com sucesso."));
    } catch (err) {
      eventLog("user_create_api_key_error", { error: String(err) });
      const box = $("userCreatedApiKeyBox");
      box.textContent = `${t("user.api_key_create_error", "Erro ao criar API Key")}: ${String(err)}`;
      box.classList.remove("is-hidden", "notice-info");
      box.classList.add("notice-error");
      setUserMessage("error", `${t("user.api_key_create_error", "Erro ao criar API Key")}: ${String(err)}`);
    }
  });

  $("loadUserApiKeysBtn").addEventListener("click", async () => {
    try {
      const cfg = requireConfig();
      await Promise.all([loadUserProfile(cfg), loadUserApiKeys(cfg)]);
      setUserMessage("info", t("user.reloaded", "Dados do usuário recarregados."));
    } catch (err) {
      eventLog("user_load_error", { error: String(err) });
      setUserMessage("error", `${t("user.load_error", "Erro ao carregar dados do usuário")}: ${String(err)}`);
    }
  });

  $("adminCreateUserApiKeyForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const fd = new FormData(ev.currentTarget);
      const permissions = parseJsonInput(String(fd.get("permissions") || "[]"), []);
      const out = await apiRequest("/admin/users-with-api-key", {
        method: "POST",
        body: {
          user_name: String(fd.get("user_name") || "").trim(),
          role: String(fd.get("role") || "trader").trim(),
          password: String(fd.get("password") || "").trim() || null,
          permissions,
        },
      }, cfg);
      eventLog("admin_create_user_api_key", out);
      if (out.api_key_plain) {
        $("loginMode").value = "api_key";
        $("apiKey").value = String(out.api_key_plain);
        applyLoginMode();
      }
      await loadAdminUsers(cfg);
      await loadAdminUsersKeys(cfg);
    } catch (err) {
      eventLog("admin_create_user_api_key_error", { error: String(err) });
    }
  });

  const adminOmsViewTabs = document.getElementById("adminOmsViewTabs");
  if (adminOmsViewTabs) {
    const syncFromTabs = () => {
      const active = String(adminOmsViewTabs.active || adminOmsViewTabs.getAttribute("active") || "open_orders");
      state.adminOmsView = active || "open_orders";
    };
    syncFromTabs();
    adminOmsViewTabs.addEventListener("click", async (ev) => {
      const tabEl = ev.target && ev.target.closest ? ev.target.closest("wa-tab") : null;
      const panel = String(tabEl?.getAttribute?.("panel") || "").trim();
      if (!panel) return;
      state.adminOmsView = panel;
      adminOmsViewTabs.active = panel;
      adminOmsViewTabs.setAttribute("active", panel);
      try {
        await loadAdminOms(true);
      } catch (err) {
        eventLog("admin_oms_change_view_error", { error: String(err) });
      }
    });
    adminOmsViewTabs.addEventListener("wa-tab-show", async () => {
      syncFromTabs();
      try {
        await loadAdminOms(true);
      } catch (err) {
        eventLog("admin_oms_change_view_error", { error: String(err) });
      }
    });
  }
  $("adminOmsLoadBtn").addEventListener("click", async () => {
    try {
      await loadAdminOms(true);
      setAdminOmsMessage("info", "");
      eventLog("admin_oms_load", { ok: true });
    } catch (err) {
      eventLog("admin_oms_load_error", { error: String(err) });
      setAdminOmsMessage("error", `${t("admin.oms_load_error", "Erro ao carregar Admin OMS")}: ${String(err)}`);
      uiLog("error", "Admin OMS load failed", { error: String(err) });
    }
  });
  $("adminOmsAddRowBtn").addEventListener("click", () => addAdminOmsRow());
  $("adminOmsSaveSelectedBtn").addEventListener("click", async () => {
    try {
      await saveSelectedAdminOmsRows();
    } catch (err) {
      eventLog("admin_oms_save_error", { error: String(err) });
      const msg = String(err || "");
      if (msg.includes("admin_oms_unlock_required")) {
        setAdminOmsMessage("error", t("admin.oms_unlock_before_save", "Ative 'Unlock dangerous mode' antes de salvar."));
      } else if (msg.includes("admin_oms_select_rows")) {
        setAdminOmsMessage("error", t("admin.oms_select_rows", "Selecione uma ou mais linhas no checkbox da esquerda."));
      } else {
        setAdminOmsMessage("error", `${t("admin.oms_save_error", "Erro ao salvar")}: ${msg}`);
      }
      uiLog("error", "Admin OMS save failed", { error: String(err) });
    }
  });
  $("adminOmsDeleteSelectedBtn").addEventListener("click", async () => {
    try {
      await deleteSelectedAdminOmsRows();
    } catch (err) {
      eventLog("admin_oms_delete_error", { error: String(err) });
      const msg = String(err || "");
      if (msg.includes("admin_oms_unlock_required")) {
        setAdminOmsMessage("error", t("admin.oms_unlock_before_delete", "Ative 'Unlock dangerous mode' antes de deletar."));
      } else if (msg.includes("admin_oms_select_rows")) {
        setAdminOmsMessage("error", t("admin.oms_select_rows", "Selecione uma ou mais linhas no checkbox da esquerda."));
      } else {
        setAdminOmsMessage("error", `${t("admin.oms_delete_error", "Erro ao deletar")}: ${msg}`);
      }
      uiLog("error", "Admin OMS delete failed", { error: String(err) });
    }
  });
  $("loadAdminAccountsBtn").addEventListener("click", async () => {
    try {
      await loadAdminAccounts();
      eventLog("admin_load_accounts", { ok: true });
    } catch (err) {
      eventLog("admin_load_accounts_error", { error: String(err) });
    }
  });
  $("adminRefreshSystemStatusBtn").addEventListener("click", async () => {
    try {
      await loadAdminSystemStatus();
      eventLog("admin_system_status_refresh_ok", { ok: true });
    } catch (err) {
      eventLog("admin_system_status_refresh_error", { error: String(err) });
      setAdminSystemStatus(`${t("admin.status_error", "Erro ao consultar estado")}: ${String(err)}`, "error");
    }
  });
  $("loadAdminUsersKeysBtn").addEventListener("click", async () => {
    try {
      await loadAdminUsersKeys();
      eventLog("admin_load_users_keys", { ok: true });
    } catch (err) {
      eventLog("admin_load_users_keys_error", { error: String(err) });
    }
  });
  $("loadAdminUsersBtn").addEventListener("click", async () => {
    try {
      await loadAdminUsers();
      eventLog("admin_load_users", { ok: true });
    } catch (err) {
      eventLog("admin_load_users_error", { error: String(err) });
    }
  });
  $("adminCreateApiKeyForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const userId = Number($("adminApiKeyUserId").value || 0);
      if (!Number.isFinite(userId) || userId <= 0) throw new Error("user_id inválido");
      const apiKeyRaw = String($("adminApiKeyValue").value || "").trim();
      const body = { user_id: userId };
      if (apiKeyRaw) body.api_key = apiKeyRaw;
      const out = await apiRequest("/admin/api-keys", {
        method: "POST",
        body,
      }, cfg);
      eventLog("admin_create_api_key", out);
      const plain = String(out.api_key_plain || "");
      setAdminApiKeyMessage(
        "success",
        t("admin.api_key_created", "API Key criada. user_id={user_id} api_key_id={api_key_id} key={key}")
          .replace("{user_id}", String(out.user_id))
          .replace("{api_key_id}", String(out.api_key_id))
          .replace("{key}", plain),
      );
      await loadAdminUsersKeys(cfg);
    } catch (err) {
      eventLog("admin_create_api_key_error", { error: String(err) });
      setAdminApiKeyMessage("error", `${t("admin.api_key_create_error", "Erro ao criar API Key")}: ${String(err)}`);
      uiLog("error", "Admin create API key failed", { error: String(err) });
    }
  });
  $("loadCcxtExchangesBtn").addEventListener("click", async () => {
    try {
      await loadCcxtExchanges();
      eventLog("admin_load_ccxt_exchanges", { ok: true });
    } catch (err) {
      eventLog("admin_load_ccxt_exchanges_error", { error: String(err) });
    }
  });

  $("tradeCreateStrategyForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const fd = new FormData(ev.currentTarget);
      const out = await apiRequest("/strategies", {
        method: "POST",
        body: {
          name: String(fd.get("name") || "").trim(),
          account_ids: selectedTradeStrategyAccountIds(),
          client_strategy_id: parseNullablePositiveInt(fd.get("client_strategy_id"), "client_strategy_id"),
        },
      }, cfg);
      eventLog("trade_create_strategy", out);
      await loadTradeStrategies(cfg);
    } catch (err) {
      eventLog("trade_create_strategy_error", { error: String(err) });
    }
  });
  $("loadTradeStrategiesBtn").addEventListener("click", async () => {
    try {
      await loadTradeStrategies();
      eventLog("trade_load_strategies", { ok: true });
    } catch (err) {
      eventLog("trade_load_strategies_error", { error: String(err) });
    }
  });

  $("riskAllowNewPositionsBtn").addEventListener("click", async () => {
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("riskAccountId", "risk");
      const comment = String($("riskAccountComment").value || "").trim();
      if (!comment) throw new Error("risk comment is required");
      const out = await apiRequest(`/oms/risk/${accountId}/allow_new_positions`, {
        method: "POST",
        body: { allow_new_positions: true, comment },
      }, cfg);
      eventLog("risk_allow_new_positions", out);
    } catch (err) {
      eventLog("risk_allow_new_positions_error", { error: String(err) });
    }
  });
  $("riskBlockNewPositionsBtn").addEventListener("click", async () => {
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("riskAccountId", "risk");
      const comment = String($("riskAccountComment").value || "").trim();
      if (!comment) throw new Error("risk comment is required");
      const out = await apiRequest(`/oms/risk/${accountId}/allow_new_positions`, {
        method: "POST",
        body: { allow_new_positions: false, comment },
      }, cfg);
      eventLog("risk_block_new_positions", out);
    } catch (err) {
      eventLog("risk_block_new_positions_error", { error: String(err) });
    }
  });
  $("riskSetAccountActiveBtn").addEventListener("click", async () => {
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("riskAccountId", "risk");
      const comment = String($("riskAccountComment").value || "").trim();
      if (!comment) throw new Error("risk comment is required");
      const out = await apiRequest(`/oms/risk/${accountId}/status`, {
        method: "POST",
        body: { status: "active", comment },
      }, cfg);
      eventLog("risk_set_account_active", out);
    } catch (err) {
      eventLog("risk_set_account_active_error", { error: String(err) });
    }
  });
  $("riskSetAccountBlockedBtn").addEventListener("click", async () => {
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("riskAccountId", "risk");
      const comment = String($("riskAccountComment").value || "").trim();
      if (!comment) throw new Error("risk comment is required");
      const out = await apiRequest(`/oms/risk/${accountId}/status`, {
        method: "POST",
        body: { status: "blocked", comment },
      }, cfg);
      eventLog("risk_set_account_blocked", out);
    } catch (err) {
      eventLog("risk_set_account_blocked_error", { error: String(err) });
    }
  });
  $("riskAllowStrategyBtn").addEventListener("click", async () => {
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("riskStrategyAccountId", "risk_strategy");
      const strategyId = Number($("riskStrategyId").value || 0);
      const comment = String($("riskStrategyComment").value || "").trim();
      if (!Number.isFinite(strategyId) || strategyId < 0) throw new Error("strategy_id inválido");
      if (!comment) throw new Error("risk comment is required");
      const out = await apiRequest(`/oms/risk/${accountId}/strategies/allow_new_positions`, {
        method: "POST",
        body: { strategy_id: strategyId, allow_new_positions: true, comment },
      }, cfg);
      eventLog("risk_allow_strategy", out);
    } catch (err) {
      eventLog("risk_allow_strategy_error", { error: String(err) });
    }
  });
  $("riskBlockStrategyBtn").addEventListener("click", async () => {
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("riskStrategyAccountId", "risk_strategy");
      const strategyId = Number($("riskStrategyId").value || 0);
      const comment = String($("riskStrategyComment").value || "").trim();
      if (!Number.isFinite(strategyId) || strategyId < 0) throw new Error("strategy_id inválido");
      if (!comment) throw new Error("risk comment is required");
      const out = await apiRequest(`/oms/risk/${accountId}/strategies/allow_new_positions`, {
        method: "POST",
        body: { strategy_id: strategyId, allow_new_positions: false, comment },
      }, cfg);
      eventLog("risk_block_strategy", out);
    } catch (err) {
      eventLog("risk_block_strategy_error", { error: String(err) });
    }
  });
  $("riskLoadPermissionsBtn").addEventListener("click", async () => {
    try {
      await loadRiskPermissions();
    } catch (err) {
      eventLog("risk_load_permissions_error", { error: String(err) });
    }
  });
  $("riskSavePermissionBtn").addEventListener("click", async () => {
    try {
      const cfg = requireConfig();
      const apiKeyId = Number($("riskApiKeyId").value || 0);
      const accountId = requireAccountId("riskPermAccountId", "risk_permission");
      if (!Number.isFinite(apiKeyId) || apiKeyId <= 0) throw new Error("risk api_key_id is required");
      const body = {
        account_id: accountId,
        can_read: $("riskCanRead").checked,
        can_trade: $("riskCanTrade").checked,
        can_close_position: $("riskCanClosePosition").checked,
        can_risk_manage: $("riskCanRiskManage").checked,
        can_block_new_positions: $("riskCanBlockNewPositions").checked,
        can_block_account: $("riskCanBlockAccount").checked,
        restrict_to_strategies: $("riskRestrictStrategies").checked,
        strategy_ids: parseCsvIntList($("riskStrategyIdsCsv").value),
      };
      const out = await apiRequest(`/admin/api-keys/${apiKeyId}/permissions`, {
        method: "PUT",
        body,
      }, cfg);
      eventLog("risk_save_permission", { api_key_id: apiKeyId, account_id: accountId, out });
      await loadRiskPermissions(cfg);
    } catch (err) {
      eventLog("risk_save_permission_error", { error: String(err) });
    }
  });

  const adminCreateUserForm = $("adminCreateUserApiKeyForm");
  const adminRoleSelect = adminCreateUserForm.querySelector("select[name='role']");
  const adminPermsTextarea = adminCreateUserForm.querySelector("textarea[name='permissions']");
  const syncPermissionsPreset = () => {
    const role = String(adminRoleSelect.value || "trader").trim();
    adminPermsTextarea.value = JSON.stringify(presetPermissionsForRole(role), null, 2);
  };
  adminRoleSelect.addEventListener("change", syncPermissionsPreset);
  syncPermissionsPreset();
}

function switchTab(tab) {
  const allowedTabs = new Set([
    "login",
    "user",
    "commands",
    "positions",
    "postTrading",
    "system",
    "strategies",
    "risk",
    "admin",
    "adminUsers",
    "adminApiKeys",
    "adminStatus",
    "adminOms",
  ]);
  const nextTab = allowedTabs.has(tab) ? tab : "login";
  localStorage.setItem(STORAGE.activeMenu, nextTab);
  const isLogin = nextTab === "login";
  const isUser = nextTab === "user";
  const isCommands = nextTab === "commands";
  const isPositions = nextTab === "positions";
  const isPostTrading = nextTab === "postTrading";
  const isSystem = nextTab === "system";
  const isStrategies = nextTab === "strategies";
  const isRisk = nextTab === "risk";
  const isAdmin = nextTab === "admin";
  const isAdminUsers = nextTab === "adminUsers";
  const isAdminApiKeys = nextTab === "adminApiKeys";
  const isAdminStatus = nextTab === "adminStatus";
  const isAdminOms = nextTab === "adminOms";
  const setMenuActive = (id, active) => {
    const node = $(id);
    node.classList.toggle("active", active);
    node.setAttribute("variant", "neutral");
  };
  setMenuActive("tabLoginBtn", isLogin);
  setMenuActive("tabUserBtn", isUser);
  setMenuActive("tabCommandsBtn", isCommands);
  setMenuActive("tabPositionsBtn", isPositions);
  setMenuActive("tabPostTradingOrdersBtn", isPostTrading);
  setMenuActive("tabSystemBtn", isSystem);
  setMenuActive("tabStrategiesBtn", isStrategies);
  setMenuActive("tabRiskBtn", isRisk);
  setMenuActive("tabAdminBtn", isAdmin);
  setMenuActive("tabAdminUsersBtn", isAdminUsers);
  setMenuActive("tabAdminApiKeysBtn", isAdminApiKeys);
  setMenuActive("tabAdminStatusBtn", isAdminStatus);
  setMenuActive("tabAdminOmsBtn", isAdminOms);
  $("tabPostTradingGroupBtn").classList.toggle("active", isPostTrading);
  const inAdminGroup = isAdmin || isAdminUsers || isAdminApiKeys || isAdminStatus || isAdminOms;
  $("tabAdminGroupBtn").classList.toggle("active", inAdminGroup);
  $("adminSubmenu").classList.toggle("is-hidden", !inAdminGroup);
  $("postTradingSubmenu").classList.toggle("is-hidden", !isPostTrading);
  $("loginPanel").classList.toggle("is-hidden", !isLogin);
  $("userPanel").classList.toggle("is-hidden", !isUser);
  $("commandsPanel").classList.toggle("is-hidden", !isCommands);
  $("positionsPanel").classList.toggle("is-hidden", !isPositions);
  $("postTradingPanel").classList.toggle("is-hidden", !isPostTrading);
  $("systemPanel").classList.toggle("is-hidden", !isSystem);
  $("strategiesPanel").classList.toggle("is-hidden", !isStrategies);
  $("riskPanel").classList.toggle("is-hidden", !isRisk);
  $("adminPanel").classList.toggle("is-hidden", !(isAdmin || isAdminUsers || isAdminApiKeys || isAdminStatus));
  $("adminOmsPanel").classList.toggle("is-hidden", !isAdminOms);
  $("adminStatusSection").classList.toggle("is-hidden", !isAdminStatus);
  $("adminAccountsSection").classList.toggle("is-hidden", !isAdmin);
  $("adminUsersSection").classList.toggle("is-hidden", !isAdminUsers);
  $("adminApiKeysSection").classList.toggle("is-hidden", !isAdminApiKeys);
  // Keep navigation predictable: every menu change starts at top.
  window.scrollTo({ top: 0, left: 0, behavior: "auto" });
  const mainContent = document.querySelector(".main-content");
  if (mainContent && typeof mainContent.scrollTo === "function") {
    mainContent.scrollTo({ top: 0, left: 0, behavior: "auto" });
  }
  if (isAdmin) {
    Promise.all([loadAdminAccounts(), loadCcxtExchanges(), loadAdminSystemStatus()]).catch((err) => {
      eventLog("admin_load_accounts_error", { error: String(err) });
    });
  }
  if (isAdminStatus) {
    loadAdminSystemStatus().catch((err) => {
      eventLog("admin_system_status_load_error", { error: String(err) });
    });
  }
  if (isAdminUsers) {
    Promise.all([loadAdminUsers()]).catch((err) => {
      eventLog("admin_load_users_error", { error: String(err) });
    });
  }
  if (isAdminApiKeys) {
    Promise.all([loadAdminUsersKeys()]).catch((err) => {
      eventLog("admin_load_users_keys_error", { error: String(err) });
    });
  }
  if (isAdminOms) {
    loadAdminOms(true).catch((err) => {
      eventLog("admin_oms_load_error", { error: String(err) });
    });
  }
  if (isUser) {
    Promise.all([loadUserProfile(), loadUserApiKeys()]).catch((err) => {
      eventLog("user_load_error", { error: String(err) });
    });
  }
  if (isPostTrading) {
    loadPostTradingPreview(true).catch((err) => {
      eventLog("post_trading_load_error", { error: String(err) });
    });
  }
  if (isCommands || isStrategies) {
    loadTradeStrategies().catch((err) => {
      eventLog("trade_load_strategies_error", { error: String(err) });
    });
  }
  if (isRisk) {
    loadAccountsByApiKey().catch((err) => {
      eventLog("risk_load_accounts_error", { error: String(err) });
    });
  }
}

function bootstrapDefaults() {
  const urls = loadHistory(STORAGE.baseUrls);
  const keys = loadHistory(STORAGE.apiKeys);
  const accounts = loadHistory(STORAGE.accountIds);
  const strategies = loadHistory(STORAGE.strategyIds);
  const savedTheme = String(localStorage.getItem(STORAGE.themeMode) || "system").trim();
  applyThemeMode(savedTheme, false);
  const savedDensity = String(localStorage.getItem(STORAGE.densityMode) || "normal").trim();
  applyDensityMode(savedDensity, false);
  const savedLanguage = String(localStorage.getItem(STORAGE.language) || "pt-BR").trim();
  state.locale = I18N[savedLanguage] ? savedLanguage : "pt-BR";
  renderLanguageOptions(state.locale);
  applyLanguageTexts();
  if (urls[0]) $("baseUrl").value = String(urls[0]);
  else $("baseUrl").value = "http://127.0.0.1:8000";
  if (keys[0]) $("apiKey").value = String(keys[0]);
  const accountDefault = accounts[0] ? String(accounts[0]) : "1";
  $("sendAccountId").value = accountDefault;
  $("ccxtAccountId").value = accountDefault;
  $("cancelOrderAccountId").value = accountDefault;
  $("closePositionAccountId").value = accountDefault;
  $("riskAccountId").value = accountDefault;
  $("riskStrategyAccountId").value = accountDefault;
  $("riskPermAccountId").value = accountDefault;
  // Keep selectors empty on first paint; populate from /oms/accounts
  // so options always include labels ("id - name"), not just cached ids.
  renderViewAccountsOptions([]);
  renderStrategyAccountsOptions([]);
  const strategyDefault = strategies[0] !== undefined ? String(strategies[0]) : "0";
  $("sendStrategyId").value = strategyDefault;
  $("ccxtStrategyId").value = strategyDefault;
  const exchangeSelect = $("adminExchangeIdSelect");
  if (exchangeSelect.options.length === 0) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "";
    exchangeSelect.appendChild(opt);
    exchangeSelect.value = "";
  }
  applyDefaultHistoryDates();
  for (const key of OMS_PAGER_KEYS) {
    const sizeNode = document.getElementById(`${key}PageSize`);
    const size = Math.max(1, Number(sizeNode?.value || state.omsPageSize));
    state.omsPager[key] = { page: 1, pageSize: size, total: 0, totalPages: 1 };
    updatePagerInfo(key);
  }
  renderAllHistories();
}

function adminOmsCurrentView() {
  return String(state.adminOmsView || "open_orders");
}

function setPostTradingMessage(kind, text) {
  const node = document.getElementById("postTradingMessage");
  if (!node) return;
  if (!text) {
    node.textContent = "";
    node.classList.add("is-hidden");
    node.classList.remove("notice-success", "notice-error", "notice-info");
    return;
  }
  node.textContent = String(text);
  node.classList.remove("is-hidden", "notice-success", "notice-error", "notice-info");
  if (kind === "success") node.classList.add("notice-success");
  else if (kind === "error") node.classList.add("notice-error");
  else node.classList.add("notice-info");
  uiLog(kind, String(text), { source: "post_trading" });
}

function postTradingAccountIdsCsv() {
  const manual = String(document.getElementById("postTradingAccountIds")?.value || "").trim();
  if (manual) return manual;
  const ids = getSelectedViewAccountIds();
  if (ids.length > 0) return ids.join(",");
  return "";
}

function postTradingFilters(resetPage = false) {
  const pager = state.omsPager.postTrading;
  const page = resetPage ? 1 : Math.max(1, Number(pager?.page || 1));
  const pageSize = Math.max(1, Number(pager?.pageSize || state.omsPageSize));
  const startDate = String(document.getElementById("postTradingStartDate")?.value || "").trim();
  const endDate = String(document.getElementById("postTradingEndDate")?.value || "").trim();
  const reconciledNode = document.getElementById("postTradingReconciled");
  const reconciledValues = reconciledNode
    ? [...reconciledNode.selectedOptions].map((opt) => String(opt.value || "").trim().toLowerCase()).filter((x) => !!x)
    : ["true", "false"];
  const statusesCsv = String(document.getElementById("postTradingOrderStatuses")?.value || "").trim();
  const reconciledSet = new Set(reconciledValues);
  const hasTrue = reconciledSet.has("true");
  const hasFalse = reconciledSet.has("false");
  const reconciled = hasTrue && !hasFalse ? true : hasFalse && !hasTrue ? false : undefined;
  const orderStatuses = statusesCsv
    ? statusesCsv.split(",").map((x) => String(x || "").trim()).filter((x) => !!x)
    : [];
  return {
    account_ids: postTradingAccountIdsCsv(),
    start_date: startDate || null,
    end_date: endDate || null,
    ...(reconciled === undefined ? {} : { reconciled }),
    reconciled_values: reconciledValues,
    order_statuses: orderStatuses,
    kinds: ["order"],
    preview: true,
    page,
    page_size: pageSize,
  };
}

async function loadPostTradingPreview(resetPage = false) {
  const cfg = requireConfig();
  const body = postTradingFilters(resetPage);
  const out = await apiRequest("/oms/reassign", { method: "POST", body }, cfg);
  let items = Array.isArray(out.items) ? out.items : [];
  const selectedReconciled = new Set((body.reconciled_values || []).map((x) => String(x || "").toLowerCase()));
  if (selectedReconciled.size > 0 && selectedReconciled.size < 2) {
    items = items.filter((row) => {
      const v = row?.reconciled;
      if (v === null || v === undefined) return false;
      const key = Boolean(v) ? "true" : "false";
      return selectedReconciled.has(key);
    });
  }
  const normalizedItems = items.map((row) => {
    const currentStrategy = Number(row?.strategy_id || 0);
    return {
      ...row,
      target_strategy_id: currentStrategy > 0 ? currentStrategy : null,
    };
  });
  state.tables.postTrading.setData(normalizedItems);
  state.postTradingLastPreview = out;
  setPagerFromTotal(
    "postTrading",
    Number((out.deals_total || 0) + (out.orders_total || 0)),
    Number(out.page || body.page),
    Number(out.page_size || body.page_size),
  );
  setPostTradingMessage(
    "info",
    t("post_trading.preview_result", "Preview: deals={deals} orders={orders}")
      .replace("{deals}", String(out.deals_total || 0))
      .replace("{orders}", String(out.orders_total || 0)),
  );
}

async function runPostTradingReconcile() {
  const cfg = requireConfig();
  const scope = String(document.getElementById("postTradingReconcileScope")?.value || "short").trim().toLowerCase();
  const startDate = String(document.getElementById("postTradingStartDate")?.value || "").trim();
  const endDate = String(document.getElementById("postTradingEndDate")?.value || "").trim();
  const body = {
    account_ids: postTradingAccountIdsCsv(),
    scope,
  };
  if (scope === "period") {
    if (!startDate || !endDate) throw new Error("period_requires_date_range");
    body.start_date = startDate;
    body.end_date = endDate;
  }
  const out = await apiRequest("/oms/reconcile", { method: "POST", body }, cfg);
  setPostTradingMessage(
    "success",
    t("post_trading.reconcile_ok", "Reconciliação disparada: {count} conta(s).")
      .replace("{count}", String(out?.triggered_count || 0)),
  );
  eventLog("post_trading_reconcile", out);
}

async function applyPostTradingReassign() {
  const table = state.tables.postTrading;
  if (!table) return;
  const selected = table.getSelectedData ? table.getSelectedData() : [];
  if (!Array.isArray(selected) || selected.length === 0) {
    throw new Error("post_trading_select_rows");
  }
  const cfg = requireConfig();
  const base = postTradingFilters(false);
  const groups = new Map();
  for (const row of selected) {
    const orderId = Number(row?.id || 0);
    const targetStrategyId = Number(row?.target_strategy_id || 0);
    if (!Number.isFinite(orderId) || orderId <= 0) continue;
    if (!Number.isFinite(targetStrategyId) || targetStrategyId <= 0) {
      throw new Error("post_trading_target_strategy_required");
    }
    const key = String(targetStrategyId);
    if (!groups.has(key)) groups.set(key, { targetStrategyId, orderIds: [] });
    groups.get(key).orderIds.push(orderId);
  }
  if (groups.size === 0) throw new Error("post_trading_select_rows");
  let dealsUpdated = 0;
  let ordersUpdated = 0;
  for (const group of groups.values()) {
    const out = await apiRequest("/oms/reassign", {
      method: "POST",
      body: {
        ...base,
        preview: false,
        target_strategy_id: Number(group.targetStrategyId),
        order_ids: [...new Set(group.orderIds)],
      },
    }, cfg);
    dealsUpdated += Number(out?.deals_updated || 0);
    ordersUpdated += Number(out?.orders_updated || 0);
  }
  setPostTradingMessage(
    "success",
    t("post_trading.apply_result", "Applied: deals_updated={deals} orders_updated={orders}")
      .replace("{deals}", String(dealsUpdated))
      .replace("{orders}", String(ordersUpdated)),
  );
  eventLog("post_trading_apply", { deals_updated: dealsUpdated, orders_updated: ordersUpdated, groups: groups.size });
  await loadPostTradingPreview(false);
}

function adminOmsEntityByView(view) {
  if (view === "open_orders" || view === "history_orders") return "orders";
  if (view === "open_positions" || view === "history_positions") return "positions";
  return "deals";
}

function adminOmsDateRange() {
  const start = String(document.getElementById("adminOmsStartDate")?.value || "").trim();
  const end = String(document.getElementById("adminOmsEndDate")?.value || "").trim();
  if ((start && !end) || (!start && end)) throw new Error("start_date and end_date must be together");
  return { start, end };
}

function adminOmsAccountIdsCsv() {
  const manual = String(document.getElementById("adminOmsAccountIds")?.value || "").trim();
  if (manual) return manual;
  const ids = getSelectedViewAccountIds();
  if (ids.length > 0) return ids.join(",");
  return "";
}

function adminOmsDangerUnlocked() {
  return Boolean(document.getElementById("adminOmsUnsafeToggle")?.checked);
}

function confirmAdminOmsAction(message) {
  return new Promise((resolve) => {
    const dialog = document.getElementById("adminOmsConfirmDialog");
    const text = document.getElementById("adminOmsConfirmText");
    const okBtn = document.getElementById("adminOmsConfirmOkBtn");
    const cancelBtn = document.getElementById("adminOmsConfirmCancelBtn");
    if (!dialog || !okBtn || !cancelBtn) {
      resolve(false);
      return;
    }
    if (text) text.textContent = String(message || "Confirm?");
    const cleanup = () => {
      okBtn.removeEventListener("click", onOk);
      cancelBtn.removeEventListener("click", onCancel);
      dialog.open = false;
    };
    const onOk = () => {
      cleanup();
      resolve(true);
    };
    const onCancel = () => {
      cleanup();
      resolve(false);
    };
    okBtn.addEventListener("click", onOk);
    cancelBtn.addEventListener("click", onCancel);
    dialog.open = true;
  });
}

function normalizeAdminOmsRowForPayload(row) {
  const out = {};
  for (const [k, v] of Object.entries(row || {})) {
    if (k.startsWith("_")) continue;
    if (v === undefined) continue;
    out[k] = v;
  }
  return out;
}

function nowUtcSqlDateTime() {
  const d = new Date();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  return `${y}-${m}-${day} ${hh}:${mm}:${ss}`;
}

function ensureAdminOmsInsertDefaults(row) {
  const out = { ...(row || {}) };
  const nowTs = nowUtcSqlDateTime();
  const fillIfEmpty = (key) => {
    const raw = out[key];
    const text = String(raw ?? "").trim().toLowerCase();
    if (raw === null || raw === undefined || text === "" || text === "null") {
      out[key] = nowTs;
    }
  };
  for (const key of ["created_at", "updated_at", "opened_at", "executed_at"]) {
    fillIfEmpty(key);
  }
  return out;
}

function assertAdminOmsMutateSuccess(out) {
  const results = Array.isArray(out?.results) ? out.results : [];
  const failed = results.filter((item) => item && item.ok === false);
  if (failed.length === 0) return;
  const first = failed[0] || {};
  const msg = String(first.error || "unknown_error");
  throw new Error(msg);
}

function adminOmsResultIdsText(out) {
  const results = Array.isArray(out?.results) ? out.results : [];
  const ids = results
    .filter((item) => item && item.ok === true && Number(item.id || 0) > 0)
    .map((item) => Number(item.id))
    .filter((id, idx, arr) => arr.indexOf(id) === idx);
  if (ids.length === 0) return "";
  return ` IDs: ${ids.join(", ")}`;
}

async function loadAdminOms(resetPage = false) {
  const cfg = requireConfig();
  const view = adminOmsCurrentView();
  const pager = state.omsPager.adminOms;
  const page = resetPage ? 1 : Math.max(1, Number(pager?.page || 1));
  const pageSize = Math.max(1, Number(pager?.pageSize || state.omsPageSize));
  const { start, end } = adminOmsDateRange();
  const accountIds = adminOmsAccountIdsCsv();
  const query = new URLSearchParams({
    account_ids: accountIds,
    page: String(page),
    page_size: String(pageSize),
  });
  if (start && end) {
    query.set("start_date", start);
    query.set("end_date", end);
  }
  const out = await apiRequest(`/admin/oms/${encodeURIComponent(view)}?${query.toString()}`, {}, cfg);
  const table = state.tables.adminOms;
  if (table) {
    const cols = adminOmsColumnsForView(view);
    table.setColumns(normalizeTabulatorColumns(cols));
    table.setData(Array.isArray(out.items) ? out.items : []);
  }
  setPagerFromTotal("adminOms", Number(out.total || 0), Number(out.page || page), Number(out.page_size || pageSize));
}

function addAdminOmsRow() {
  const view = adminOmsCurrentView();
  const table = state.tables.adminOms;
  if (!table) return;
  const fallbackAccountId = Number((getSelectedViewAccountIds()[0] || state.availableAccountIds[0] || 0));
  const base = {
    account_id: fallbackAccountId > 0 ? fallbackAccountId : "",
    created_at: nowUtcSqlDateTime(),
  };
  if (view.includes("orders")) {
    Object.assign(base, {
      symbol: "BTC/USDT",
      side: "buy",
      order_type: "market",
      status: "SUBMITTED",
      qty: "0",
      strategy_id: 0,
      position_id: 0,
      reason: "admin",
      filled_qty: "0",
    });
  } else if (view.includes("positions")) {
    Object.assign(base, {
      symbol: "BTC/USDT",
      side: "buy",
      qty: "0",
      avg_price: "0",
      strategy_id: 0,
      state: "open",
      reason: "admin",
    });
  } else {
    Object.assign(base, {
      order_id: null,
      position_id: 0,
      symbol: "BTC/USDT",
      side: "buy",
      qty: "0",
      price: "0",
      strategy_id: 0,
      reason: "admin",
      reconciled: true,
    });
  }
  const added = table.addData([base], true);
  if (added && typeof added.then === "function") {
    added.then((rows) => {
      if (Array.isArray(rows) && rows[0] && typeof rows[0].select === "function") rows[0].select();
    }).catch(() => {});
    return;
  }
  if (Array.isArray(added) && added[0] && typeof added[0].select === "function") added[0].select();
}

async function saveSelectedAdminOmsRows() {
  const cfg = requireConfig();
  const table = state.tables.adminOms;
  if (!table) return;
  const rows = table.getSelectedData ? table.getSelectedData() : [];
  if (!Array.isArray(rows) || rows.length === 0) throw new Error("admin_oms_select_rows");
  if (!adminOmsDangerUnlocked()) throw new Error("admin_oms_unlock_required");
  const ok = await confirmAdminOmsAction(
    t("admin.oms_confirm_save", `Save ${rows.length} row(s) to database?`).replace("{count}", String(rows.length)),
  );
  if (!ok) return;
  const view = adminOmsCurrentView();
  const entity = adminOmsEntityByView(view);
  const operations = rows.map((row) => {
    let payload = normalizeAdminOmsRowForPayload(row);
    const id = Number(payload.id || 0);
    if (id <= 0) payload = ensureAdminOmsInsertDefaults(payload);
    return {
      op: id > 0 ? "update" : "insert",
      row: payload,
    };
  });
  const out = await apiRequest(`/admin/oms/${entity}/mutate`, {
    method: "POST",
    body: operations,
  }, cfg);
  assertAdminOmsMutateSuccess(out);
  eventLog("admin_oms_save_selected", out);
  uiLog("info", "Admin OMS save selected", { entity, count: operations.length });
  setAdminOmsMessage(
    "success",
    t("admin.oms_save_success", `Saved successfully (${operations.length} row(s)).`).replace("{count}", String(operations.length))
      + adminOmsResultIdsText(out),
  );
  await loadAdminOms(false);
}

async function deleteSelectedAdminOmsRows() {
  const cfg = requireConfig();
  const table = state.tables.adminOms;
  if (!table) return;
  const rows = table.getSelectedData ? table.getSelectedData() : [];
  if (!Array.isArray(rows) || rows.length === 0) throw new Error("admin_oms_select_rows");
  if (!adminOmsDangerUnlocked()) throw new Error("admin_oms_unlock_required");
  const view = adminOmsCurrentView();
  const entity = adminOmsEntityByView(view);
  const operations = rows
    .map((row) => normalizeAdminOmsRowForPayload(row))
    .filter((row) => Number(row.id || 0) > 0)
    .map((row) => ({ op: "delete", row: { id: Number(row.id), account_id: Number(row.account_id || 0) || null } }));
  if (operations.length === 0) throw new Error("only rows with id can be deleted");
  const ok = await confirmAdminOmsAction(t("admin.oms_confirm_delete", `DELETE ${operations.length} row(s) from database?`).replace("{count}", String(operations.length)));
  if (!ok) return;
  const out = await apiRequest(`/admin/oms/${entity}/mutate`, {
    method: "POST",
    body: operations,
  }, cfg);
  assertAdminOmsMutateSuccess(out);
  eventLog("admin_oms_delete_selected", out);
  uiLog("warn", "Admin OMS delete selected", { entity, count: operations.length });
  setAdminOmsMessage(
    "success",
    t("admin.oms_delete_success", `Deleted successfully (${operations.length} row(s)).`).replace("{count}", String(operations.length))
      + adminOmsResultIdsText(out),
  );
  await loadAdminOms(false);
}

setupTables();
bootstrapDefaults();
bindForms();
loadUiLogsFromStorage().catch(() => {});
{
  const savedTab = String(localStorage.getItem(STORAGE.activeMenu) || "login").trim();
  switchTab(savedTab);
}
status("ready", false);
loadAccountsByApiKey().catch((err) => {
  eventLog("accounts_error", { error: String(err) });
});





