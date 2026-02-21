import I18N from "./i18n.json";

const HISTORY_LIMIT = 12;
const STORAGE = {
  baseUrls: "simple_front.base_urls",
  apiKeys: "simple_front.api_keys",
  accountIds: "simple_front.account_ids",
  strategyIds: "simple_front.strategy_ids",
  language: "simple_front.language",
};

const state = {
  wsConnections: new Map(),
  pingTimer: null,
  connected: false,
  eventSeq: 0,
  tables: {},
  availableAccountIds: [],
  accountLabels: new Map(),
  subscribedAccountIds: [],
  locale: "pt-BR",
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
  const node = $("status");
  node.textContent = text;
  node.style.background = isConnected ? "#caefd6" : "#d4e3f5";
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
  for (const id of ids) {
    const option = document.createElement("option");
    option.value = String(id);
    const label = state.accountLabels.get(id) || "";
    option.textContent = label ? `${id} - ${label}` : String(id);
    option.selected = previouslySelected.has(id);
    node.appendChild(option);
  }
}

function renderStrategyAccountsOptions(ids) {
  for (const nodeId of ["strategyAccountIds", "tradeStrategyAccountIds"]) {
    const node = $(nodeId);
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

function selectedStrategyAccountIds() {
  return [...$("strategyAccountIds").selectedOptions]
    .map((opt) => Number(opt.value))
    .filter((n) => Number.isFinite(n) && n > 0);
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

function collectFormAccountIds() {
  const ids = [
    parseAccountId($("sendAccountId").value),
    parseAccountId($("ccxtAccountId").value),
    parseAccountId($("cancelAllAccountId").value),
    parseAccountId($("riskAccountId").value),
    parseAccountId($("riskStrategyAccountId").value),
    parseAccountId($("riskPermAccountId").value),
  ].filter((n) => Number.isFinite(n) && n > 0);
  return [...new Set(ids)];
}

function makeTable(id, columns) {
  const Tabulator = window.Tabulator;
  if (!Tabulator) throw new Error("Tabulator not loaded");
  return new Tabulator(`#${id}`, {
    data: [],
    layout: "fitDataStretch",
    maxHeight: "300px",
    placeholder: "sem dados",
    columns,
  });
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

function append(table, row, max = 400) {
  table.addData([row], true);
  const rows = table.getData();
  if (rows.length > max) {
    table.setData(rows.slice(0, max));
  }
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

async function fetchAccountSnapshot(accountId, cfg) {
  const [openOrders, historyOrders, deals, openPositions, historyPositions] = await Promise.all([
    apiRequest(`/position/orders/open?account_id=${accountId}`, {}, cfg),
    apiRequest(`/position/orders/history?account_id=${accountId}`, {}, cfg),
    apiRequest(`/position/deals?account_id=${accountId}`, {}, cfg),
    apiRequest(`/position/positions/open?account_id=${accountId}`, {}, cfg),
    apiRequest(`/position/positions/history?account_id=${accountId}`, {}, cfg),
  ]);
  return {
    openOrders: openOrders.items || [],
    historyOrders: historyOrders.items || [],
    deals: deals.items || [],
    openPositions: openPositions.items || [],
    historyPositions: historyPositions.items || [],
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

async function refreshTables() {
  const cfg = requireConfig();
  const accountIds = await resolveViewAccountIds(cfg);
  const snapshots = await Promise.all(accountIds.map((accountId) => fetchAccountSnapshot(accountId, cfg)));

  const openOrders = mergeById(snapshots.flatMap((x) => x.openOrders));
  const historyOrders = mergeById(snapshots.flatMap((x) => x.historyOrders));
  const deals = mergeById(snapshots.flatMap((x) => x.deals));
  const openPositions = mergeById(snapshots.flatMap((x) => x.openPositions));
  const historyPositions = mergeById(snapshots.flatMap((x) => x.historyPositions));

  state.tables.openOrders.setData(openOrders);
  state.tables.historyOrders.setData(historyOrders);
  state.tables.deals.setData(deals);
  state.tables.openPositions.setData(openPositions);
  state.tables.historyPositions.setData(historyPositions);

  status(`connected ${state.wsConnections.size} ws | viewing ${accountIds.length} account(s)`, state.connected);
}

function routeWs(msg, accountId = null) {
  const namespace = msg.namespace || "system";
  const eventName = msg.event || "event";
  const payload = { ...(msg.payload || {}) };
  const resolvedAccountId = payload.account_id || accountId;
  eventLog(`${namespace}:${eventName}`, { account_id: resolvedAccountId, payload: msg.payload || {} });

  if (!payload.account_id && resolvedAccountId) payload.account_id = resolvedAccountId;

  if (namespace === "position") {
    if (payload.order_id || payload.order_type || payload.exchange_order_id) {
      const orderRow = { ...payload };
      if (!orderRow.id && payload.order_id) orderRow.id = payload.order_id;
      const closedStatuses = new Set(["FILLED", "CANCELED", "REJECTED", "CLOSED"]);
      if (closedStatuses.has(String(orderRow.status || "").toUpperCase())) {
        removeByKey(state.tables.openOrders, orderRow.id, "id");
      } else {
        upsert(state.tables.openOrders, orderRow);
      }
    }
    if (payload.exchange_trade_id || payload.position_id) {
      append(state.tables.deals, payload);
    }
    if (payload.state || payload.avg_price || payload.side) {
      const positionRow = { ...payload };
      if (!positionRow.id && payload.position_id) positionRow.id = payload.position_id;
      const isClosed =
        String(positionRow.state || "").toLowerCase() === "closed" ||
        String(positionRow.qty || "") === "0";
      if (isClosed) {
        removeByKey(state.tables.openPositions, positionRow.id, "id");
      } else {
        upsert(state.tables.openPositions, positionRow);
      }
    }
  }

  if (namespace === "ccxt") {
    const base = payload.result || payload || {};
    if (!base.account_id) base.account_id = accountId;
    if (base.exchange_trade_id || eventName.includes("trade")) {
      append(state.tables.ccxtTrades, base);
    } else if (base.exchange_order_id || base.clientOrderId || eventName.includes("order")) {
      append(state.tables.ccxtOrders, base);
    } else if (base.order || base.trade) {
      append(state.tables.ccxtOrders, base.order || {});
      append(state.tables.ccxtTrades, base.trade || {});
    }
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

function baseCols() {
  return [
    { title: "id", field: "id", width: 84 },
    { title: "account_id", field: "account_id", width: 96 },
    { title: "symbol", field: "symbol", width: 120 },
    { title: "side", field: "side", width: 90 },
    { title: "qty", field: "qty", width: 110 },
    { title: "status/state", field: "status", width: 120 },
    { title: "price", field: "price", width: 110 },
    { title: "updated/at", field: "updated_at", width: 175 },
  ];
}

function setupTables() {
  state.tables.openPositions = makeTable("openPositionsTable", [
    { title: "id", field: "id", width: 84 },
    { title: "account_id", field: "account_id", width: 96 },
    { title: "symbol", field: "symbol", width: 120 },
    { title: "side", field: "side", width: 90 },
    { title: "qty", field: "qty", width: 110 },
    { title: "avg_price", field: "avg_price", width: 130 },
    { title: "stop_loss", field: "stop_loss", width: 120 },
    { title: "stop_gain", field: "stop_gain", width: 120 },
    { title: "state", field: "state", width: 90 },
    { title: "comment", field: "comment", width: 180 },
    { title: "opened_at", field: "opened_at", width: 170 },
    { title: "updated_at", field: "updated_at", width: 170 },
  ]);
  state.tables.openOrders = makeTable("openOrdersTable", [
    { title: "id", field: "id", width: 84 },
    { title: "account_id", field: "account_id", width: 110 },
    { title: "symbol", field: "symbol", width: 120 },
    { title: "side", field: "side", width: 80 },
    { title: "order_type", field: "order_type", width: 100 },
    { title: "status", field: "status", width: 110 },
    { title: "qty", field: "qty", width: 100 },
    { title: "stop_loss", field: "stop_loss", width: 120 },
    { title: "stop_gain", field: "stop_gain", width: 120 },
    { title: "filled_qty", field: "filled_qty", width: 100 },
    { title: "exchange_order_id", field: "exchange_order_id", width: 140 },
    { title: "comment", field: "comment", width: 180 },
    { title: "updated_at", field: "updated_at", width: 170 },
  ]);
  state.tables.historyPositions = makeTable("historyPositionsTable", baseCols());
  state.tables.historyOrders = makeTable("historyOrdersTable", baseCols());
  state.tables.deals = makeTable("dealsTable", [
    { title: "id", field: "id", width: 84 },
    { title: "account_id", field: "account_id", width: 96 },
    { title: "order_id", field: "order_id", width: 84 },
    { title: "position_id", field: "position_id", width: 90 },
    { title: "symbol", field: "symbol", width: 120 },
    { title: "side", field: "side", width: 80 },
    { title: "qty", field: "qty", width: 100 },
    { title: "price", field: "price", width: 120 },
    { title: "comment", field: "comment", width: 180 },
    { title: "exchange_trade_id", field: "exchange_trade_id", width: 130 },
    { title: "executed_at", field: "executed_at", width: 170 },
  ]);
  state.tables.ccxtTrades = makeTable("ccxtTradesTable", [
    { title: "account_id", field: "account_id", width: 96 },
    { title: "exchange_trade_id", field: "exchange_trade_id", width: 140 },
    { title: "exchange_order_id", field: "exchange_order_id", width: 140 },
    { title: "symbol", field: "symbol", width: 120 },
    { title: "side", field: "side", width: 80 },
    { title: "qty", field: "qty", width: 100 },
    { title: "price", field: "price", width: 120 },
    { title: "observed_at", field: "observed_at", width: 170 },
    { title: "raw", field: "raw_json", widthGrow: 1 },
  ]);
  state.tables.ccxtOrders = makeTable("ccxtOrdersTable", [
    { title: "account_id", field: "account_id", width: 96 },
    { title: "exchange_order_id", field: "exchange_order_id", width: 140 },
    { title: "client_order_id", field: "client_order_id", width: 130 },
    { title: "symbol", field: "symbol", width: 120 },
    { title: "side", field: "side", width: 80 },
    { title: "status", field: "status", width: 110 },
    { title: "price", field: "price", width: 120 },
    { title: "amount", field: "amount", width: 100 },
    { title: "observed_at", field: "observed_at", width: 170 },
    { title: "raw", field: "raw_json", widthGrow: 1 },
  ]);
  state.tables.events = makeTable("eventsTable", [
    { title: "seq", field: "seq", width: 70 },
    { title: "at", field: "at", width: 185 },
    { title: "kind", field: "kind", width: 170 },
    { title: "payload", field: "payload", widthGrow: 1 },
  ]);
  state.tables.tradeStrategies = makeTable("tradeStrategiesTable", [
    { title: "strategy_id", field: "strategy_id", width: 100 },
    { title: "name", field: "name", width: 220 },
    { title: "status", field: "status", width: 100 },
    {
      title: "account_ids",
      field: "account_ids",
      widthGrow: 1,
      formatter: (cell) => {
        const arr = cell.getValue();
        return Array.isArray(arr) ? arr.join(", ") : "";
      },
    },
  ]);
  state.tables.adminStrategies = makeTable("adminStrategiesTable", [
    { title: "strategy_id", field: "strategy_id", width: 100 },
    { title: "name", field: "name", editor: "input", width: 220 },
    { title: "status", field: "status", width: 110 },
    {
      title: "account_ids",
      field: "account_ids",
      widthGrow: 1,
      formatter: (cell) => {
        const arr = cell.getValue();
        return Array.isArray(arr) ? arr.join(", ") : "";
      },
    },
    {
      title: "save",
      field: "_save",
      hozAlign: "center",
      formatter: () => "Save",
      cellClick: async (_ev, cell) => {
        const row = cell.getRow().getData();
        try {
          const cfg = requireConfig();
          const out = await apiRequest(`/admin/strategies/${row.strategy_id}`, {
            method: "PATCH",
            body: { name: String(row.name || "").trim() || null },
          }, cfg);
          eventLog("admin_update_strategy_name", out);
          await loadStrategies(cfg);
        } catch (err) {
          eventLog("admin_update_strategy_name_error", { error: String(err), strategy_id: row.strategy_id });
        }
      },
    },
    {
      title: "toggle",
      field: "_toggle",
      hozAlign: "center",
      formatter: (cell) => {
        const row = cell.getRow().getData();
        return row.status === "active" ? "Disable" : "Enable";
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
          eventLog("admin_update_strategy_status", out);
          await loadStrategies(cfg);
        } catch (err) {
          eventLog("admin_update_strategy_status_error", { error: String(err), strategy_id: row.strategy_id });
        }
      },
    },
  ]);
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
      formatter: () => "Save",
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
    { title: "api_key_status", field: "api_key_status", width: 110 },
    { title: "created_at", field: "created_at", width: 180 },
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

async function loadStrategies(cfgOverride = null) {
  const cfg = cfgOverride || requireConfig();
  const res = await apiRequest("/admin/strategies", {}, cfg);
  state.tables.adminStrategies.setData(res.items || []);
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

async function loadTradeStrategies(cfgOverride = null) {
  const cfg = cfgOverride || requireConfig();
  const res = await apiRequest("/strategies", {}, cfg);
  const items = res.items || [];
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
  const res = await apiRequest("/meta/ccxt/exchanges", {}, cfg);
  const node = $("adminExchangeIdSelect");
  const current = String(node.value || "").trim();
  node.innerHTML = "";
  const items = Array.isArray(res.items) ? res.items : [];
  for (const ex of items) {
    const opt = document.createElement("option");
    opt.value = String(ex);
    opt.textContent = String(ex);
    node.appendChild(opt);
  }
  if (current && items.includes(current)) node.value = current;
  else if (items.includes("binance")) node.value = "binance";
  else if (items[0]) node.value = String(items[0]);
}

async function loadAccountsByApiKey(cfgOverride = null) {
  const cfg = cfgOverride || requireConfig();
  const resAccounts = await apiRequest("/position/accounts", {}, cfg);
  const items = (resAccounts.items || []).map((item) => ({
    account_id: Number(item.account_id),
    label: String(item.label || "").trim(),
  }));
  const ids = items.map((item) => item.account_id).filter((n) => Number.isFinite(n) && n > 0);
  state.accountLabels.clear();
  items.forEach((item) => {
    if (Number.isFinite(item.account_id) && item.account_id > 0 && item.label) {
      state.accountLabels.set(item.account_id, item.label);
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
    if (!$("ccxtAccountId").value) $("ccxtAccountId").value = String(ids[0]);
    if (!$("cancelAllAccountId").value) $("cancelAllAccountId").value = String(ids[0]);
    if (!$("riskAccountId").value) $("riskAccountId").value = String(ids[0]);
    if (!$("riskStrategyAccountId").value) $("riskStrategyAccountId").value = String(ids[0]);
    if (!$("riskPermAccountId").value) $("riskPermAccountId").value = String(ids[0]);
  }
  eventLog("accounts_loaded", { ids, labels: Object.fromEntries(state.accountLabels.entries()) });
}

function bindForms() {
  const applyLoginMode = () => {
    const mode = $("loginMode").value;
    $("loginApiKeyFields").classList.toggle("is-hidden", mode !== "api_key");
    $("loginUserPassFields").classList.toggle("is-hidden", mode !== "user_password");
  };
  $("loginMode").addEventListener("change", applyLoginMode);
  applyLoginMode();

  $("loginForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const mode = $("loginMode").value;
      const loginBaseUrl = $("baseUrl").value.trim().replace(/\/+$/, "");
      if (!loginBaseUrl) throw new Error("base_url is required");
      if (mode === "api_key") {
        const key = $("apiKey").value.trim();
        if (!key) throw new Error("api_key is required");
        $("baseUrl").value = loginBaseUrl;
        $("apiKey").value = key;
        persistCurrentValues();
        await loadAccountsByApiKey({ baseUrl: loginBaseUrl, apiKey: key, viewAccountIds: [] });
        await loadTradeStrategies({ baseUrl: loginBaseUrl, apiKey: key, viewAccountIds: [] });
        eventLog("login_api_key_ok", { base_url: loginBaseUrl });
      } else {
        const userName = $("loginUserName").value.trim();
        const password = $("loginPassword").value;
        if (!userName || !password) throw new Error("user_name e password sao obrigatorios");
        const apiKeyIdText = $("loginApiKeyId").value.trim();
        const payload = { user_name: userName, password };
        if (apiKeyIdText) payload.api_key_id = Number(apiKeyIdText);
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
        persistCurrentValues();
        await loadAccountsByApiKey({ baseUrl: loginBaseUrl, apiKey: token, viewAccountIds: [] });
        await loadTradeStrategies({ baseUrl: loginBaseUrl, apiKey: token, viewAccountIds: [] });
        eventLog("login_user_password_ok", {
          user_id: json.user_id,
          role: json.role,
          api_key_id: json.api_key_id,
          expires_at: json.expires_at,
        });
      }
      switchTab("trade");
    } catch (err) {
      eventLog("login_error", { error: String(err) });
    }
  });

  $("toggleApiKeyBtn").addEventListener("click", () => {
    const input = $("apiKey");
    const btn = $("toggleApiKeyBtn");
    const nextType = input.type === "password" ? "text" : "password";
    input.type = nextType;
    btn.textContent = nextType === "password" ? "mostrar" : "ocultar";
  });

  $("connectBtn").addEventListener("click", async () => {
    try {
      await connectWs();
    } catch (err) {
      status(String(err), false);
    }
  });
  $("disconnectBtn").addEventListener("click", () => disconnectWs());
  $("refreshBtn").addEventListener("click", async () => {
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
  $("clearOpenPositionsBtn").addEventListener("click", () => state.tables.openPositions.clearData());
  $("clearOpenOrdersBtn").addEventListener("click", () => state.tables.openOrders.clearData());
  $("clearHistoryPositionsBtn").addEventListener("click", () => state.tables.historyPositions.clearData());
  $("clearHistoryOrdersBtn").addEventListener("click", () => state.tables.historyOrders.clearData());
  $("clearDealsBtn").addEventListener("click", () => state.tables.deals.clearData());
  $("clearCcxtTradesBtn").addEventListener("click", () => state.tables.ccxtTrades.clearData());
  $("clearCcxtOrdersBtn").addEventListener("click", () => state.tables.ccxtOrders.clearData());
  $("clearEventsBtn").addEventListener("click", () => state.tables.events.clearData());
  $("selectAllAccountsBtn").addEventListener("click", () => {
    for (const opt of $("viewAccountsSelect").options) opt.selected = true;
  });
  $("clearAccountsBtn").addEventListener("click", () => {
    for (const opt of $("viewAccountsSelect").options) opt.selected = false;
  });
  $("strategyAccountsAllBtn").addEventListener("click", () => {
    for (const opt of $("strategyAccountIds").options) opt.selected = true;
  });
  $("strategyAccountsClearBtn").addEventListener("click", () => {
    for (const opt of $("strategyAccountIds").options) opt.selected = false;
  });
  $("tradeStrategyAccountsAllBtn").addEventListener("click", () => {
    for (const opt of $("tradeStrategyAccountIds").options) opt.selected = true;
  });
  $("tradeStrategyAccountsClearBtn").addEventListener("click", () => {
    for (const opt of $("tradeStrategyAccountIds").options) opt.selected = false;
  });
  $("tabLoginBtn").addEventListener("click", () => switchTab("login"));
  $("tabTradeBtn").addEventListener("click", () => switchTab("trade"));
  $("tabRiskBtn").addEventListener("click", () => switchTab("risk"));
  $("tabAdminBtn").addEventListener("click", () => switchTab("admin"));

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
          .map((item) => (item && typeof item === "object" ? String(item.symbol || "") : ""))
          .filter((s) => s.includes("/"));
      } else if (result && typeof result === "object") {
        symbols = Object.keys(result).filter((s) => String(s).includes("/"));
      }
      symbols = [...new Set(symbols)].sort();
      renderHistory("symbolHistory", symbols.slice(0, 2000));
      if (!$("sendSymbol").value && symbols[0]) $("sendSymbol").value = symbols[0];
      eventLog("load_symbols", { account_id: accountId, count: symbols.length });
    } catch (err) {
      eventLog("load_symbols_error", { error: String(err) });
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
          position_id: 0,
          reduce_only: fd.get("reduce_only") === "on",
        },
      };
      const price = String(fd.get("price") || "").trim();
      if (price) body.payload.price = price;
      const stopLoss = String(fd.get("stop_loss") || "").trim();
      const stopGain = String(fd.get("stop_gain") || "").trim();
      const comment = String(fd.get("comment") || "").trim();
      if (stopLoss) body.payload.stop_loss = stopLoss;
      if (stopGain) body.payload.stop_gain = stopGain;
      if (comment) body.payload.comment = comment;
      const out = await apiRequest("/position/commands", { method: "POST", body }, cfg);
      eventLog("send_order", out);
    } catch (err) {
      eventLog("send_order_error", { error: String(err) });
    }
  });

  $("cancelOrderForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const fd = new FormData(ev.currentTarget);
      const csv = String(fd.get("order_ids_csv") || "").trim();
      if (!csv) throw new Error("order_ids_csv is required");
      const ids = parseCsvIntList(csv);
      if (ids.length === 0) throw new Error("nenhum order_id válido no CSV");
      const body = {
        command: "cancel_order",
        payload: {
          order_id: ids.length === 1 ? ids[0] : null,
          order_ids: ids,
          order_ids_csv: csv,
        },
      };
      const out = await apiRequest("/position/commands", { method: "POST", body }, cfg);
      eventLog("cancel_order", { count: ids.length, out });
    } catch (err) {
      eventLog("cancel_order_error", { error: String(err) });
    }
  });
  $("cancelAllBtn").addEventListener("click", async () => {
    try {
      const cfg = requireConfig();
      const accountId = requireAccountId("cancelAllAccountId", "cancel_all_orders");
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
      const out = await apiRequest("/position/commands", { method: "POST", body }, cfg);
      eventLog("cancel_all_orders", { account_id: accountId, strategy_ids: strategyIds, out });
    } catch (err) {
      eventLog("cancel_all_orders_error", { error: String(err) });
    }
  });

  $("changeOrderForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const fd = new FormData(ev.currentTarget);
      const orderId = Number(fd.get("order_id"));
      const payload = { order_id: orderId };
      const newQty = String(fd.get("new_qty") || "").trim();
      const newPrice = String(fd.get("new_price") || "").trim();
      if (newQty) payload.new_qty = newQty;
      if (newPrice) payload.new_price = newPrice;
      const body = { command: "change_order", payload };
      const out = await apiRequest("/position/commands", { method: "POST", body }, cfg);
      eventLog("change_order", out);
    } catch (err) {
      eventLog("change_order_error", { error: String(err) });
    }
  });

  $("positionChangeForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const fd = new FormData(ev.currentTarget);
      const positionId = Number(fd.get("position_id"));
      const payload = { position_id: positionId };
      const stopLoss = String(fd.get("stop_loss") || "").trim();
      const stopGain = String(fd.get("stop_gain") || "").trim();
      const comment = String(fd.get("comment") || "").trim();
      const useStopLoss = $("positionChangeUseStopLoss").checked;
      const useStopGain = $("positionChangeUseStopGain").checked;
      const useComment = $("positionChangeUseComment").checked;
      if (useStopLoss) payload.stop_loss = stopLoss ? stopLoss : null;
      if (useStopGain) payload.stop_gain = stopGain ? stopGain : null;
      if (useComment) payload.comment = comment ? comment : null;
      if (!useStopLoss && !useStopGain && !useComment) {
        throw new Error("marque ao menos um campo para alterar");
      }
      const body = { command: "position_change", payload };
      const out = await apiRequest("/position/commands", { method: "POST", body }, cfg);
      eventLog("position_change", out);
    } catch (err) {
      eventLog("position_change_error", { error: String(err) });
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
        eventLog("ccxt_call", { strategy_id: strategyId, func, out });
        const result = out.result || {};
        result.account_id = result.account_id || accountId;
        if (result.exchange_trade_id || func.includes("trade")) append(state.tables.ccxtTrades, result);
        if (result.exchange_order_id || func.includes("order")) append(state.tables.ccxtOrders, result);
      }
    } catch (err) {
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
          role: String(fd.get("role") || "trade").trim(),
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

  $("adminCreateStrategyForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    try {
      const cfg = requireConfig();
      const fd = new FormData(ev.currentTarget);
      const accountIds = selectedStrategyAccountIds();
      const out = await apiRequest("/admin/strategies", {
        method: "POST",
        body: {
          name: String(fd.get("name") || "").trim(),
          account_ids: accountIds,
        },
      }, cfg);
      eventLog("admin_create_strategy", out);
      await loadStrategies(cfg);
    } catch (err) {
      eventLog("admin_create_strategy_error", { error: String(err) });
    }
  });
  $("loadStrategiesBtn").addEventListener("click", async () => {
    try {
      await loadStrategies();
      eventLog("admin_load_strategies", { ok: true });
    } catch (err) {
      eventLog("admin_load_strategies_error", { error: String(err) });
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
        },
      }, cfg);
      eventLog("trade_create_strategy", out);
      await loadTradeStrategies(cfg);
      await loadStrategies(cfg);
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
      const out = await apiRequest(`/position/risk/${accountId}/allow_new_positions`, {
        method: "POST",
        body: { allow_new_positions: true },
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
      const out = await apiRequest(`/position/risk/${accountId}/allow_new_positions`, {
        method: "POST",
        body: { allow_new_positions: false },
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
      const out = await apiRequest(`/position/risk/${accountId}/status`, {
        method: "POST",
        body: { status: "active" },
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
      const out = await apiRequest(`/position/risk/${accountId}/status`, {
        method: "POST",
        body: { status: "blocked" },
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
      if (!Number.isFinite(strategyId) || strategyId < 0) throw new Error("strategy_id inválido");
      const out = await apiRequest(`/position/risk/${accountId}/strategies/allow_new_positions`, {
        method: "POST",
        body: { strategy_id: strategyId, allow_new_positions: true },
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
      if (!Number.isFinite(strategyId) || strategyId < 0) throw new Error("strategy_id inválido");
      const out = await apiRequest(`/position/risk/${accountId}/strategies/allow_new_positions`, {
        method: "POST",
        body: { strategy_id: strategyId, allow_new_positions: false },
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
}

function switchTab(tab) {
  const isLogin = tab === "login";
  const isTrade = tab === "trade";
  const isRisk = tab === "risk";
  const isAdmin = tab === "admin";
  $("tabLoginBtn").classList.toggle("active", isLogin);
  $("tabTradeBtn").classList.toggle("active", isTrade);
  $("tabRiskBtn").classList.toggle("active", isRisk);
  $("tabAdminBtn").classList.toggle("active", isAdmin);
  $("loginPanel").classList.toggle("is-hidden", !isLogin);
  $("tradePanel").classList.toggle("is-hidden", !isTrade);
  $("riskPanel").classList.toggle("is-hidden", !isRisk);
  $("adminPanel").classList.toggle("is-hidden", !isAdmin);
  if (isAdmin) {
    Promise.all([
      loadStrategies(),
      loadAdminAccounts(),
      loadAdminUsers(),
      loadAdminUsersKeys(),
      loadCcxtExchanges(),
    ]).catch((err) => {
      eventLog("admin_load_error", { error: String(err) });
    });
  }
  if (isTrade) {
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
  if (urls[0]) $("baseUrl").value = String(urls[0]);
  else $("baseUrl").value = "http://127.0.0.1:8000";
  if (keys[0]) $("apiKey").value = String(keys[0]);
  const accountDefault = accounts[0] ? String(accounts[0]) : "1";
  $("sendAccountId").value = accountDefault;
  $("ccxtAccountId").value = accountDefault;
  $("cancelAllAccountId").value = accountDefault;
  $("riskAccountId").value = accountDefault;
  $("riskStrategyAccountId").value = accountDefault;
  $("riskPermAccountId").value = accountDefault;
  // Keep selectors empty on first paint; populate from /position/accounts
  // so options always include labels ("id - name"), not just cached ids.
  renderViewAccountsOptions([]);
  renderStrategyAccountsOptions([]);
  const strategyDefault = strategies[0] !== undefined ? String(strategies[0]) : "0";
  $("sendStrategyId").value = strategyDefault;
  $("ccxtStrategyId").value = strategyDefault;
  const exchangeSelect = $("adminExchangeIdSelect");
  if (exchangeSelect.options.length === 0) {
    const opt = document.createElement("option");
    opt.value = "binance";
    opt.textContent = "binance";
    exchangeSelect.appendChild(opt);
    exchangeSelect.value = "binance";
  }
  renderAllHistories();
}

setupTables();
bootstrapDefaults();
bindForms();
switchTab("trade");
status("ready", false);
loadAccountsByApiKey().catch((err) => {
  eventLog("accounts_error", { error: String(err) });
});



