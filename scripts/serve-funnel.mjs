import http from "node:http";
import path from "node:path";
import fs from "node:fs/promises";
import { fileURLToPath } from "node:url";
import url from "url";
import { TERMINATION_RULES } from "../src/config/terminationMap.mjs";
import { isTestInstanceName } from "../src/config/testInstances.mjs";
import { fetchRawRange } from "./fetch-raw.mjs";
import { buildDailyFromRawRange } from "./build-daily-from-raw.mjs";
import {
  getCardPlacementPage,
  getMerchantSitesPage,
  getSessionsPage,
  getFinancialInstitutionsPage,
  loginWithSdk,
} from "../src/api.mjs";
import {
  groupSessionsBySource,
  computeSourceKpis,
  buildDailySeries,
  buildMerchantSeries,
} from "../src/lib/analytics/sources.mjs";
import { fetchGaRowsForDay } from "../src/ga.mjs";
import { loadInstances } from "../src/utils/config.mjs";
const { URLSearchParams } = url;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Resolve repo root:  scripts/  -> repo/
const ROOT = path.resolve(path.join(__dirname, ".."));
const PUBLIC_DIR = path.join(ROOT, "public");
const DATA_DIR = path.join(ROOT, "data");
const DAILY_DIR = path.join(DATA_DIR, "daily");
const RAW_DIR = path.join(ROOT, "raw");
const RAW_PLACEMENTS_DIR = path.join(RAW_DIR, "placements");
const FI_REGISTRY_FILE = path.join(ROOT, "fi_registry.json");
const INSTANCES_FILES = [
  path.join(ROOT, "secrets", "instances.json"),
];
const GA_CREDENTIALS = [
  {
    name: "prod",
    label: "Production",
    file: path.join(ROOT, "secrets", "ga-service-account.json"),
    envProperty: "GA_PROPERTY_ID",
    defaultProperty: "328054560",
  },
  {
    name: "test",
    label: "Test",
    file: path.join(ROOT, "secrets", "ga-test.json"),
    envProperty: "GA_TEST_PROPERTY_ID",
    defaultProperty: process.env.GA_TEST_PROPERTY_ID || "",
  },
];
// Backwards-compatible constant (older endpoints).
const GA_SERVICE_ACCOUNT_FILE = GA_CREDENTIALS[0].file;
const PORT = 8787;
const FI_ALL_VALUE = "__all__";
const PARTNER_ALL_VALUE = "__all_partners__";
const INSTANCE_ALL_VALUE = "__all_instances__";

const updateClients = new Set();

let currentUpdateJob = {
  running: false,
  startedAt: null,
  finishedAt: null,
  startDate: null,
  endDate: null,
  lastMessage: null,
  error: null,
  forceRaw: false,
};

// ========== SERVER LOGS CAPTURE ==========
const MAX_LOG_LINES = 2000;
const serverLogs = [];

function captureLog(level, ...args) {
  const timestamp = new Date().toISOString();
  const message = args.map(arg =>
    typeof arg === 'object' ? JSON.stringify(arg) : String(arg)
  ).join(' ');

  serverLogs.push({
    timestamp,
    level,
    message
  });

  // Keep only last MAX_LOG_LINES
  if (serverLogs.length > MAX_LOG_LINES) {
    serverLogs.shift();
  }
}

// Intercept console methods
const originalLog = console.log;
const originalError = console.error;
const originalWarn = console.warn;

console.log = (...args) => {
  captureLog('info', ...args);
  originalLog(...args);
};

console.error = (...args) => {
  captureLog('error', ...args);
  originalError(...args);
};

console.warn = (...args) => {
  captureLog('warn', ...args);
  originalWarn(...args);
};

console.log('Server logs capture initialized');
// ========== END SERVER LOGS CAPTURE ==========

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function yesterdayIsoDate() {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().slice(0, 10);
}

function isoAddDays(isoDate, deltaDays) {
  const d = new Date(`${isoDate}T00:00:00Z`);
  if (Number.isNaN(d)) return isoDate;
  d.setUTCDate(d.getUTCDate() + deltaDays);
  return d.toISOString().slice(0, 10);
}

function defaultUpdateRange() {
  const endDate = todayIsoDate();
  const startDate = isoAddDays(endDate, -29);
  return { startDate, endDate };
}

function currentUpdateSnapshot() {
  const defaults = defaultUpdateRange();
  return {
    running: currentUpdateJob.running,
    startedAt: currentUpdateJob.startedAt,
    finishedAt: currentUpdateJob.finishedAt,
    startDate: currentUpdateJob.startDate || defaults.startDate,
    endDate: currentUpdateJob.endDate || defaults.endDate,
    lastMessage: currentUpdateJob.lastMessage,
    error: currentUpdateJob.error,
    forceRaw: currentUpdateJob.forceRaw || false,
    defaultRange: defaults,
  };
}

function normalizeUpdateRange(startDate, endDate) {
  const isIso = (value) => typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value);
  const validEnd = isIso(endDate) ? endDate : todayIsoDate();
  let validStart = isIso(startDate) ? startDate : isoAddDays(validEnd, -29);
  // ensure start <= end
  if (new Date(`${validStart}T00:00:00Z`) > new Date(`${validEnd}T00:00:00Z`)) {
    validStart = isoAddDays(validEnd, -29);
  }
  return { startDate: validStart, endDate: validEnd };
}

function sseSend(res, event, data) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

function broadcastUpdate(event, data) {
  for (const res of updateClients) {
    try {
      sseSend(res, event, data);
    } catch {
      // Ignore write errors; connection cleanup handled on close.
    }
  }
}

async function startUpdateJobIfNeeded(range = {}) {
  if (currentUpdateJob.running) {
    return;
  }

  const { startDate, endDate } = normalizeUpdateRange(
    range.startDate,
    range.endDate
  );

  currentUpdateJob = {
    running: true,
    startedAt: new Date().toISOString(),
    finishedAt: null,
    startDate,
    endDate,
    lastMessage: `Starting update for ${startDate} → ${endDate}`,
    error: null,
    forceRaw: Boolean(range.forceRaw),
  };

  broadcastUpdate("init", {
    startedAt: currentUpdateJob.startedAt,
    startDate,
    endDate,
    message: currentUpdateJob.lastMessage,
  });

  try {
    broadcastUpdate("progress", {
      phase: "raw",
      message: `Fetching raw for ${startDate} → ${endDate}${range.forceRaw ? " (forced refetch)" : ""}...`,
    });

    await fetchRawRange({
      startDate,
      endDate,
      onStatus: (message) =>
        broadcastUpdate("progress", { phase: "raw", message }),
      forceRaw: Boolean(range.forceRaw),
      strict: true,
    });

    broadcastUpdate("progress", {
      phase: "daily",
      message: `Rebuilding daily rollups for ${startDate} → ${endDate}...`,
    });

    await buildDailyFromRawRange({ startDate, endDate });

    currentUpdateJob.running = false;
    currentUpdateJob.finishedAt = new Date().toISOString();
    currentUpdateJob.lastMessage = "Update completed.";

    broadcastUpdate("done", {
      finishedAt: currentUpdateJob.finishedAt,
      startDate,
      endDate,
      message: currentUpdateJob.lastMessage,
    });
  } catch (err) {
    currentUpdateJob.running = false;
    currentUpdateJob.finishedAt = new Date().toISOString();
    currentUpdateJob.error = err?.message || String(err);
    currentUpdateJob.lastMessage = `Update failed: ${currentUpdateJob.error}`;

    const failures = Array.isArray(err?.failures) ? err.failures : [];
    const instanceNames = failures
      .map((f) => f?.instanceName)
      .filter(Boolean);
    const uniqueNames = Array.from(new Set(instanceNames));
    const cancelMessage = uniqueNames.length
      ? `Refresh cancelled — please fix credentials for: ${uniqueNames.join(", ")}`
      : `Refresh cancelled — ${currentUpdateJob.error}`;

    broadcastUpdate("job_error", {
      finishedAt: currentUpdateJob.finishedAt,
      startDate,
      endDate,
      error: currentUpdateJob.error,
      message: cancelMessage,
      failures,
    });
  }
}

const mime = (ext) =>
  ({
    ".html": "text/html; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".mjs": "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".txt": "text/plain; charset=utf-8",
  }[ext] || "application/octet-stream");

const send = (res, code, body, type) => {
  res.statusCode = code;
  if (type) res.setHeader("Content-Type", type);
  if (typeof body === "object" && !(body instanceof Uint8Array)) {
    res.setHeader("Content-Type", type || "application/json; charset=utf-8");
    res.end(JSON.stringify(body, null, 2));
  } else {
    res.end(body);
  }
};

const readRequestBody = (req) =>
  new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      try {
        resolve(Buffer.concat(chunks).toString("utf8"));
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });

async function fileExists(fp) {
  try {
    await fs.access(fp);
    return true;
  } catch {
    return false;
  }
}

async function serveFile(res, fp) {
  try {
    const buf = await fs.readFile(fp);
    res.writeHead(200, {
      "Content-Type": mime(path.extname(fp)),
      "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
      Pragma: "no-cache",
      Expires: "0",
      "Surrogate-Control": "no-store",
    });
    res.end(buf);
  } catch (e) {
    send(res, 500, { error: e.message, file: fp });
  }
}

async function pickUiEntry() {
  const heatmap = path.join(PUBLIC_DIR, "heatmap.html");
  const funnel = path.join(PUBLIC_DIR, "funnel.html");
  const landing = path.join(PUBLIC_DIR, "index.html");
  if (await fileExists(landing)) return landing;
  if (await fileExists(heatmap)) return heatmap;
  if (await fileExists(funnel)) return funnel;
  // last-ditch inline page so you always see *something*
  return null;
}

async function listDaily() {
  try {
    const files = await fs.readdir(DAILY_DIR);
    return files.filter((f) => f.endsWith(".json")).sort();
  } catch {
    return [];
  }
}

async function loadDaily(dateStr) {
  const fp = path.join(DAILY_DIR, `${dateStr}.json`);
  const raw = await fs.readFile(fp, "utf8");
  return JSON.parse(raw);
}

function isoOnly(d) {
  return new Date(d).toISOString().slice(0, 10);
}
function parseIso(value, fallback) {
  if (!value || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return fallback;
  return value;
}
function daysBetween(start, end) {
  const out = [];
  let cur = new Date(`${start}T00:00:00Z`);
  const stop = new Date(`${end}T00:00:00Z`);
  // guard
  if (Number.isNaN(cur) || Number.isNaN(stop) || cur > stop) return out;
  while (cur <= stop) {
    out.push(cur.toISOString().slice(0, 10));
    cur = new Date(cur.getTime() + 86400000);
  }
  return out;
}
// site-health color from pct (0..100) or null if no-signal
function colorFromHealth(pct) {
  if (pct === null) return "#e5e7eb"; // gray-200 (no signal)
  if (pct >= 80) return "#22c55e"; // green-500
  if (pct >= 50) return "#f59e0b"; // amber-500
  return "#ef4444"; // red-500
}

async function loadInstanceMetaMap() {
  const map = new Map();
  try {
    const raw = await fs.readFile(FI_REGISTRY_FILE, "utf8");
    const json = JSON.parse(raw);
    const normalizeIntegration = (value) => {
      if (!value) return "UNKNOWN";
      const upper = value.toString().trim().toUpperCase();
      if (upper === "SSO") return "SSO";
      if (upper === "NON-SSO") return "NON-SSO";
      if (upper === "CARDSAVR" || upper === "CARD-SAVR") return "CardSavr";
      if (upper === "TEST") return "TEST";
      return "UNKNOWN";
    };
    for (const [key, entry] of Object.entries(json || {})) {
      const fiName = entry.fi_name || key.split("__")[0] || key;
      const integration = normalizeIntegration(entry.integration_type);
      const partner = entry.partner || "Unknown";
      const instValue = entry.instance || null;
      if (!instValue) continue;
      const inst = instValue.toString().trim().toLowerCase();
      if (!inst) continue;
      map.set(inst, { fi: fiName, integration, partner });
    }
  } catch {
    // if registry missing, fall back to unknown metadata
  }
  return map;
}
// Extract a best-effort placement date for day-bucketing:
function placementDay(p) {
  const keys = [
    "completed_on",
    "account_linked_on",
    "job_ready_on",
    "job_created_on",
    "created_on",
  ];
  for (const k of keys) {
    const v = p?.[k];
    if (v) {
      const t = new Date(v);
      if (!Number.isNaN(t)) return t.toISOString().slice(0, 10);
    }
  }
  return null;
}

async function readInstancesFile() {
  for (const candidate of INSTANCES_FILES) {
    try {
      const raw = await fs.readFile(candidate, "utf8");
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) {
        throw Object.assign(new Error("secrets/instances.json must be an array"), {
          status: 400,
        });
      }
      return { entries: parsed, path: candidate };
    } catch (err) {
      if (err.code === "ENOENT") {
        // try next candidate
        continue;
      }
      throw err;
    }
  }
  return { entries: [], path: INSTANCES_FILES[0] };
}

const normalizeInstanceEntry = (entry = {}) => {
  const cleaned = (value) =>
    value === null || value === undefined ? "" : value.toString().trim();
  const next = {
    name: cleaned(entry.name),
    CARDSAVR_INSTANCE: cleaned(entry.CARDSAVR_INSTANCE),
    USERNAME: cleaned(entry.USERNAME),
    PASSWORD: cleaned(entry.PASSWORD),
    API_KEY: cleaned(entry.API_KEY),
    APP_NAME: cleaned(entry.APP_NAME),
  };
  if (!next.name) {
    throw Object.assign(new Error("Instance name is required"), {
      status: 400,
    });
  }
  if (!next.CARDSAVR_INSTANCE) {
    throw Object.assign(new Error("CARDSAVR_INSTANCE is required"), {
      status: 400,
    });
  }
  return next;
};

async function writeInstancesFile(entries) {
  const sorted = [...entries].sort((a, b) =>
    (a?.name || "").localeCompare(b?.name || "")
  );
  let target = INSTANCES_FILES[0];
  for (const candidate of INSTANCES_FILES) {
    try {
      await fs.access(candidate);
      target = candidate;
      break;
    } catch {
      // missing, keep searching
    }
  }
  // Ensure the directory exists before writing
  const dir = path.dirname(target);
  await fs.mkdir(dir, { recursive: true });
  await fs.writeFile(target, JSON.stringify(sorted, null, 2) + "\n", "utf8");
  return { entries: sorted, path: target };
}

function getGaCredentialConfig(name) {
  const key = (name || "").toString().trim().toLowerCase();
  const cfg = GA_CREDENTIALS.find((c) => c.name === key) || null;
  if (!cfg) {
    throw Object.assign(new Error("Unknown GA credential name"), { status: 400 });
  }
  return cfg;
}

function validateGaServiceAccountJson(obj) {
  if (!obj || typeof obj !== "object") {
    throw Object.assign(new Error("Missing JSON object"), { status: 400 });
  }
  const type = (obj.type || "").toString();
  const clientEmail = (obj.client_email || "").toString();
  const privateKey = (obj.private_key || "").toString();
  if (type !== "service_account") {
    throw Object.assign(new Error("Invalid GA credential: expected type=service_account"), {
      status: 400,
    });
  }
  if (!clientEmail || !privateKey) {
    throw Object.assign(new Error("Invalid GA credential: missing client_email or private_key"), {
      status: 400,
    });
  }
  return obj;
}

async function readGaCredentialSummary(name) {
  const cfg = getGaCredentialConfig(name);
  try {
    const raw = await fs.readFile(cfg.file, "utf8");
    const obj = JSON.parse(raw || "{}");
    const stat = await fs.stat(cfg.file).catch(() => null);
    return {
      name: cfg.name,
      label: cfg.label,
      exists: true,
      path: cfg.file,
      updatedAt: stat ? stat.mtime.toISOString() : null,
      summary: {
        type: obj?.type || null,
        projectId: obj?.project_id || null,
        clientEmail: obj?.client_email || null,
        hasPrivateKey: !!obj?.private_key,
      },
    };
  } catch (err) {
    if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) {
      return {
        name: cfg.name,
        label: cfg.label,
        exists: false,
        path: cfg.file,
        updatedAt: null,
        summary: null,
      };
    }
    throw err;
  }
}

async function readGaCredentialContent(name) {
  const cfg = getGaCredentialConfig(name);
  const summary = await readGaCredentialSummary(name);
  if (!summary.exists) return { ...summary, json: null, jsonText: "" };
  const raw = await fs.readFile(cfg.file, "utf8");
  const obj = JSON.parse(raw || "{}");
  return { ...summary, json: obj, jsonText: JSON.stringify(obj, null, 2) };
}

async function writeGaCredentialFile(name, payload) {
  if (!payload || typeof payload !== "object") {
    throw Object.assign(new Error("Payload must be a JSON object"), { status: 400 });
  }
  const cfg = getGaCredentialConfig(name);

  let obj = null;
  if (payload.json && typeof payload.json === "object") {
    obj = payload.json;
  } else if (typeof payload.jsonText === "string") {
    obj = JSON.parse(payload.jsonText || "{}");
  } else {
    obj = payload;
  }
  validateGaServiceAccountJson(obj);

  await fs.mkdir(path.dirname(cfg.file), { recursive: true });
  await fs.writeFile(cfg.file, JSON.stringify(obj, null, 2) + "\n", "utf8");
  return readGaCredentialContent(cfg.name);
}

async function deleteGaCredentialFile(name) {
  const cfg = getGaCredentialConfig(name);
  try {
    await fs.unlink(cfg.file);
  } catch (err) {
    if (err && err.code === "ENOENT") return readGaCredentialSummary(cfg.name);
    throw err;
  }
  return readGaCredentialSummary(cfg.name);
}

function pickSs01Instance(instances = []) {
  const lowerName = (v) => (v || "").toString().trim().toLowerCase();
  const match = instances.find(
    (entry) =>
      lowerName(entry?.name) === "ss01" ||
      lowerName(entry?.CARDSAVR_INSTANCE || "").includes("ss01")
  );
  return match || instances[0] || null;
}

async function fetchMerchantSitesFromSs01() {
  const { entries } = await readInstancesFile();
  const ss01 = pickSs01Instance(entries);
  if (!ss01) {
    throw new Error("No ss01 instance credentials found.");
  }

  const { session } = await loginWithSdk(ss01);
  const sites = [];
  let pagingMeta = null;
  let guard = 0;
  while (guard < 200) {
    const headers = pagingMeta
      ? { "x-cardsavr-paging": JSON.stringify(pagingMeta) }
      : {};
    const resp = await getMerchantSitesPage(session, headers);
    const rows =
      Array.isArray(resp?.body) ||
      Array.isArray(resp?.merchant_sites) ||
      Array.isArray(resp?.items)
        ? resp.body || resp.merchant_sites || resp.items
        : Array.isArray(resp)
        ? resp
        : [];
    sites.push(
      ...rows.map((r) => ({
        id: r.id,
        name: r.name || r.display_name || "",
        host: r.host || r.hostname || "",
        tags: Array.isArray(r.tags) ? r.tags : [],
        tier: r.tier ?? null,
      }))
    );

    const pagingHeader = resp?.headers?.get
      ? resp.headers.get("x-cardsavr-paging")
      : resp?.headers?.["x-cardsavr-paging"];

    if (!pagingHeader) break;
    try {
      pagingMeta = JSON.parse(pagingHeader);
    } catch {
      break;
    }
    const total = Number(pagingMeta.total_results) || rows.length;
    const pageLen = Number(pagingMeta.page_length) || rows.length || 25;
    const totalPages = pageLen > 0 ? Math.ceil(total / pageLen) : 1;
    const nextPage = (Number(pagingMeta.page) || pagingMeta.page || 1) + 1;
    if (nextPage > totalPages) break;
    pagingMeta.page = nextPage;
    if (!pagingMeta.page_length) pagingMeta.page_length = pageLen;
    guard += 1;
  }

  return sites;
}

async function fetchAllFinancialInstitutions(progressCallback = null) {
  const { entries } = await readInstancesFile();

  const allFis = [];
  const instanceStatuses = {};
  const instanceNames = entries.map(e => e.name);
  const totalInstances = entries.length;
  let currentInstanceIndex = 0;

  for (const inst of entries) {
    currentInstanceIndex++;
    if (progressCallback) {
      progressCallback({
        type: 'progress',
        current: currentInstanceIndex,
        total: totalInstances,
        instance: inst.name,
        fisLoaded: allFis.length
      });
    }
    try {
      console.log(`Fetching FIs from ${inst.name}...`);
      const { session } = await loginWithSdk(inst);

      let pagingMeta = null;
      let guard = 0;
      let lastPage = 0;

      while (guard < 200) {
        let resp;
        try {
          if (pagingMeta) {
            console.log(`[${inst.name}] Fetching page with paging:`, JSON.stringify(pagingMeta));
            // Pass paging as second parameter (header), not as query params
            resp = await session.getFinancialInstitutions({}, pagingMeta);
          } else {
            console.log(`[${inst.name}] Fetching first page (no paging)`);
            resp = await session.getFinancialInstitutions({});
          }
        } catch (err) {
          console.error(`[${inst.name}] SDK call failed:`, err);
          throw err;
        }

        // Check for pagination header first
        const pagingHeader = resp?.headers?.get
          ? resp.headers.get("x-cardsavr-paging")
          : resp?.headers?.["x-cardsavr-paging"];

        let currentPage = 1;
        if (pagingHeader) {
          try {
            const parsedPaging = JSON.parse(pagingHeader);
            currentPage = Number(parsedPaging.page) || 1;
          } catch {
            // ignore parse error
          }
        }

        // Check if API returned same page as last time (ignoring our page request)
        if (lastPage > 0 && currentPage === lastPage) {
          console.log(`[${inst.name}] API returned page ${currentPage} again (ignoring pagination), stopping without adding duplicates`);
          break;
        }

        // Track the page we just received
        lastPage = currentPage;

        // Normalize response structure
        const rows = Array.isArray(resp?.body)
          ? resp.body
          : Array.isArray(resp?.financial_institutions)
          ? resp.financial_institutions
          : Array.isArray(resp)
          ? resp
          : [];

        // Enrich each FI with instance name and send progress update
        for (const fi of rows) {
          allFis.push({ ...fi, _instance: inst.name });
          // Send progress update after each FI added
          if (progressCallback) {
            progressCallback({
              type: 'progress',
              current: currentInstanceIndex,
              total: totalInstances,
              instance: inst.name,
              fisLoaded: allFis.length
            });
          }
        }

        console.log(`[${inst.name}] Fetched ${rows.length} FIs (total so far: ${allFis.filter(f => f._instance === inst.name).length})`);

        if (!pagingHeader) {
          console.log(`[${inst.name}] No paging header found, stopping pagination`);
          break;
        }

        console.log(`[${inst.name}] Paging header:`, pagingHeader);

        try {
          pagingMeta = JSON.parse(pagingHeader);
        } catch {
          break;
        }

        const total = Number(pagingMeta.total_results) || rows.length;
        const pageLen = Number(pagingMeta.page_length) || rows.length || 25;
        const totalPages = pageLen > 0 ? Math.ceil(total / pageLen) : 1;
        const nextPage = currentPage + 1;

        console.log(`[${inst.name}] Paging: page ${currentPage}, total_results=${total}, page_length=${pageLen}, totalPages=${totalPages}`);

        if (nextPage > totalPages) {
          console.log(`[${inst.name}] Reached last page, stopping pagination`);
          break;
        }

        // Create new paging object for next request
        pagingMeta = {
          ...pagingMeta,
          page: nextPage,
          page_length: pagingMeta.page_length || pageLen
        };
        guard += 1;
      }

      instanceStatuses[inst.name] = 'success';
      console.log(`✅ Fetched FIs from ${inst.name}: ${allFis.filter(f => f._instance === inst.name).length} records`);

    } catch (err) {
      console.error(`❌ Failed to fetch FIs from ${inst.name}:`, err.message);
      instanceStatuses[inst.name] = 'error';
    }
  }

  return {
    fis: allFis,
    instances: instanceNames,
    instanceStatuses,
    fetchedAt: new Date().toISOString(),
    totalCount: allFis.length
  };
}

async function readPlacementDay(day) {
  try {
    const fp = path.join(RAW_PLACEMENTS_DIR, `${day}.json`);
    const raw = await fs.readFile(fp, "utf8");
    const data = JSON.parse(raw);
    console.log(`📂 Read placements for ${day}: ${data.placements?.length || 0} records`);
    return data;
  } catch {
    return null;
  }
}

async function readSessionDay(day) {
  try {
    const fp = path.join(RAW_DIR, "sessions", `${day}.json`);
    const raw = await fs.readFile(fp, "utf8");
    const data = JSON.parse(raw);
    console.log(`📂 Read sessions for ${day}: ${data.sessions?.length || 0} records`);
    return data;
  } catch {
    return null;
  }
}

async function listRawDays(type = "sessions") {
  const dir = path.join(RAW_DIR, type);
  try {
    const files = await fs.readdir(dir);
    return files
      .filter((f) => /^\d{4}-\d{2}-\d{2}\.json$/.test(f))
      .map((f) => f.replace(/\.json$/, ""))
      .sort();
  } catch {
    return [];
  }
}

function normalizeFiKey(value) {
  return value ? value.toString().trim().toLowerCase() : "";
}

function normalizeIntegration(value) {
  if (!value) return "UNKNOWN";
  const upper = value.toString().trim().toUpperCase();
  if (upper === "NON-SSO" || upper === "NON_SSO" || upper.includes("NONSSO")) return "NON-SSO";
  if (upper.includes("SSO")) return "SSO";
  if (upper.includes("CARDSAVR") || upper.includes("CARD-SAVR")) return "CardSavr";
  if (upper === "TEST") return "TEST";
  return "UNKNOWN";
}

function canonicalInstance(value) {
  if (!value) return "";
  return value.toString().trim().toLowerCase().replace(/[^a-z0-9]/g, "");
}

function normalizeInstanceKey(value) {
  const normalized = canonicalInstance(value);
  return normalized || "unknown";
}

function makeFiInstanceKey(fiKey, instanceValue) {
  return `${normalizeFiKey(fiKey)}__${normalizeInstanceKey(instanceValue)}`;
}

function normalizeFiInstanceKey(value) {
  if (!value) return "";
  const raw = value.toString().trim();
  if (!raw) return "";
  if (!raw.includes("__")) {
    return makeFiInstanceKey(raw, "unknown");
  }
  const [fiPart, instPart] = raw.split("__");
  return makeFiInstanceKey(fiPart, instPart);
}

function formatInstanceDisplay(value) {
  if (!value) return "unknown";
  const base = value
    .toString()
    .trim()
    .toLowerCase()
    .replace(/[\s_]+/g, "-");
  return base || "unknown";
}

function medianFromFrequencyMap(freqMap, totalCount) {
  if (!totalCount) return null;
  const lowerIndex = Math.floor((totalCount - 1) / 2);
  const upperIndex = Math.floor(totalCount / 2);
  const sortedKeys = Array.from(freqMap.keys()).sort((a, b) => a - b);
  let cursor = 0;
  let lowerValue = null;
  let upperValue = null;
  for (const key of sortedKeys) {
    const count = freqMap.get(key) || 0;
    if (!count) continue;
    const start = cursor;
    const end = cursor + count - 1;
    if (lowerValue === null && lowerIndex >= start && lowerIndex <= end) {
      lowerValue = key;
    }
    if (upperValue === null && upperIndex >= start && upperIndex <= end) {
      upperValue = key;
    }
    if (lowerValue !== null && upperValue !== null) break;
    cursor += count;
  }
  if (lowerValue === null || upperValue === null) return null;
  return (lowerValue + upperValue) / 2;
}

async function loadFiRegistrySafe() {
  try {
    const raw = await fs.readFile(FI_REGISTRY_FILE, "utf8");
    const data = JSON.parse(raw);
    const count = Object.keys(data).length;
    console.log(`📂 Loaded FI registry: ${count} FIs`);
    return data;
  } catch (err) {
    console.warn(`⚠️  Failed to load FI registry: ${err.message}`);
    return {};
  }
}

function buildFiMetaMap(fiRegistry = {}) {
  const map = new Map();
  for (const entry of Object.values(fiRegistry)) {
    if (!entry || typeof entry !== "object") continue;
    const fiKey = normalizeFiKey(entry.fi_lookup_key || entry.fi_name);
    if (!fiKey) continue;
    const integration = normalizeIntegration(entry.integration_type);
    const partner = entry.partner || "Unknown";
    map.set(fiKey, {
      fi: entry.fi_name || fiKey,
      integration,
      partner,
    });
  }
  return map;
}

function mapPlacementToJob(placement, fiFallback, instanceFallback) {
  const termination = (placement?.termination_type || placement?.termination || placement?.status || "UNKNOWN")
    .toString()
    .trim()
    .toUpperCase() || "UNKNOWN";
  const terminationRule = TERMINATION_RULES[termination] || TERMINATION_RULES.UNKNOWN;
  const created = placement.job_created_on || placement.created_on || null;
  const completed =
    placement.completed_on ||
    placement.account_linked_on ||
    placement.last_updated_on ||
    null;
  let durationMs = null;
  if (created && completed) {
    const start = new Date(created);
    const end = new Date(completed);
    if (!Number.isNaN(start) && !Number.isNaN(end)) {
      durationMs = end - start;
    }
  }
  const instance =
    placement._instance ||
    placement.instance ||
    placement.instance_name ||
    placement.org_name ||
    instanceFallback ||
    "";
  const fi = normalizeFiKey(
    placement.fi_lookup_key ||
      placement.financial_institution_lookup_key ||
      placement.fi_name ||
      fiFallback
  );
  const jobId =
    placement.id ||
    placement.result_id ||
    placement.place_card_on_single_site_job_id ||
    placement.job_id ||
    null;
  const merchant =
    placement.merchant_site_hostname ||
    (placement.merchant_site_id
      ? `merchant_${placement.merchant_site_id}`
      : "unknown");
  return {
    id: jobId,
    merchant,
    termination,
    termination_label: terminationRule?.label || termination,
    severity: terminationRule?.severity || "unknown",
    status: placement.status || "",
    status_message: placement.status_message || "",
    created_on: created || null,
    ready_on: placement.job_ready_on || null,
    completed_on: completed,
    duration_ms: durationMs,
    instance: formatInstanceDisplay(instance),
    fi_key: fi || fiFallback || "",
    source_integration: placement.source?.integration || null,
    is_success:
      termination === "BILLABLE" ||
      (placement.status || "").toString().toUpperCase() === "SUCCESSFUL",
  };
}

function mapSessionToTroubleshootEntry(session, placementMap, fiMeta, instanceMeta) {
  const agentId =
    session.agent_session_id ||
    session.session_id ||
    session.id ||
    session.cuid ||
    null;
  const instanceRaw =
    session._instance || session.instance || session.instance_name || session.org_name || "";
  const instanceDisplay = formatInstanceDisplay(instanceRaw || "unknown");
  const normalizedInstance = canonicalInstance(instanceDisplay);
  const instanceLookup = instanceMeta.get(instanceDisplay.toLowerCase());
  const fiFromInstance = instanceLookup?.fi || null;
  const fiLookupRaw =
    session.financial_institution_lookup_key ||
    session.fi_lookup_key ||
    session.fi_name ||
    null;
  const fiKey = normalizeFiKey(
    fiLookupRaw || fiFromInstance || session.fi_name || null
  );
  const fiEntry = fiMeta.get(fiKey);
  const partner = instanceLookup?.partner || fiEntry?.partner || "Unknown";
  const placementsRaw = agentId ? placementMap.get(agentId) || [] : [];
  const jobs = placementsRaw
    .map((pl) => mapPlacementToJob(pl, fiKey, instanceDisplay))
    .sort((a, b) => {
      if (!a.created_on || !b.created_on) return 0;
      return a.created_on.localeCompare(b.created_on);
    });
  const jobIntegrationRaw = jobs.find((j) => j.source_integration)?.source_integration || null;
  const jobIntegrationNormalized = normalizeIntegration(jobIntegrationRaw);
  const sourceIntegrationRaw = session.source?.integration || null;
  const sourceIntegrationNormalized = normalizeIntegration(sourceIntegrationRaw);
  let integrationNormalized = sourceIntegrationNormalized;
  let integrationRaw = sourceIntegrationRaw;
  if (integrationNormalized === "UNKNOWN" && jobIntegrationRaw) {
    integrationNormalized = jobIntegrationNormalized;
    integrationRaw = jobIntegrationRaw;
  }
  if (integrationNormalized === "UNKNOWN" && fiEntry?.integration) {
    integrationNormalized = normalizeIntegration(fiEntry.integration);
    if (!integrationRaw) integrationRaw = fiEntry.integration;
  }
  const displayIntegration =
    integrationNormalized !== "UNKNOWN"
      ? integrationNormalized
      : integrationRaw
      ? integrationRaw.toString()
      : "UNKNOWN";
  const totalJobs = session.total_jobs ?? jobs.length;
  const successfulJobs = session.successful_jobs ?? jobs.filter((j) => j.is_success).length;
  const failedJobs =
    session.failed_jobs ??
    (Number.isFinite(totalJobs) ? Math.max(0, totalJobs - successfulJobs) : jobs.length - successfulJobs);

  return {
    id: session.id || session.session_id || agentId || session.cuid || null,
    cuid: session.cuid || null,
    agent_session_id: agentId,
    fi_key: fiKey || fiFromInstance || "unknown_fi",
    fi_lookup_key: fiLookupRaw || null,
    fi_name: fiEntry?.fi || session.fi_name || fiKey || "Unknown FI",
    partner,
    integration: integrationNormalized,
    integration_raw: integrationRaw || null,
    integration_display: displayIntegration || integrationNormalized || "UNKNOWN",
    instance: instanceDisplay,
    is_test: isTestInstanceName(instanceDisplay),
    created_on: session.created_on || null,
    closed_on: session.closed_on || null,
    total_jobs: totalJobs,
    successful_jobs: successfulJobs,
    failed_jobs: failedJobs,
    clickstream: Array.isArray(session.clickstream)
      ? session.clickstream.map((step) => ({
          url: step.url || "",
          page_title: step.page_title || "",
          at: step.timestamp || step.time || null,
        }))
      : [],
    jobs,
    source: {
      integration: session.source?.integration || null,
      device: session.source?.device || null,
    },
    placements_raw: placementsRaw,
  };
}

function buildTroubleshootPayload(date, sessionsRaw, placementsRaw, fiMeta, instanceMeta) {
  const placementMap = new Map();
  const placements = Array.isArray(placementsRaw?.placements) ? placementsRaw.placements : [];
  for (const pl of placements) {
    const key =
      pl.agent_session_id ||
      pl.session_id ||
      pl.cardholder_session_id ||
      pl.cuid ||
      null;
    if (!key) continue;
    const list = placementMap.get(key) || [];
    list.push(pl);
    placementMap.set(key, list);
  }

  const sessions = Array.isArray(sessionsRaw?.sessions) ? sessionsRaw.sessions : [];
  const rows = sessions.map((s) =>
    mapSessionToTroubleshootEntry(s, placementMap, fiMeta, instanceMeta)
  );

  const totals = summarizeTroubleshootSessions(rows);

  return {
    date,
    totals,
    sessions: rows,
    placements: placements.length,
  };
}

function summarizeTroubleshootSessions(rows = []) {
  return rows.reduce(
    (acc, row) => {
      acc.sessions += 1;
      const jobCount = Array.isArray(row.jobs) ? row.jobs.length : 0;
      if (jobCount > 0) acc.sessions_with_jobs += 1;
      const successes = row.jobs.filter((j) => j.is_success).length;
      if (successes > 0) acc.sessions_with_success += 1;
      acc.jobs += jobCount;
      acc.jobs_success += successes;
      acc.jobs_failure += Math.max(0, jobCount - successes);
      for (const job of row.jobs) {
        const term = job.termination || "UNKNOWN";
        acc.by_termination[term] = (acc.by_termination[term] || 0) + 1;
      }
      return acc;
    },
    {
      sessions: 0,
      sessions_with_jobs: 0,
      sessions_with_success: 0,
      jobs: 0,
      jobs_success: 0,
      jobs_failure: 0,
      by_termination: {},
    }
  );
}

async function loadTroubleshootRange(startDate, endDate) {
  const days = daysBetween(startDate, endDate);
  const sessions = [];
  const placements = [];
  for (const day of days) {
    const s = await readSessionDay(day);
    if (s?.sessions) sessions.push(...s.sessions);
    const p = await readPlacementDay(day);
    if (p?.placements) placements.push(...p.placements);
  }
  return { sessions, placements };
}

async function buildTroubleshootOptions() {
  const [days, fiRegistry] = await Promise.all([
    listRawDays("sessions"),
    loadFiRegistrySafe(),
  ]);
  const fiMeta = buildFiMetaMap(fiRegistry);
  const fiOptions = Array.from(fiMeta.entries()).map(([key, entry]) => ({
    key,
    label: entry.fi || key,
    partner: entry.partner || "Unknown",
    integration: entry.integration || "UNKNOWN",
  }));
  const partnerSet = new Set(fiOptions.map((fi) => fi.partner || "Unknown"));
  const integrationSet = new Set(fiOptions.map((fi) => fi.integration || "UNKNOWN"));
  const instanceSet = new Set();
  for (const entry of Object.values(fiRegistry)) {
    const primary = entry.instance ? formatInstanceDisplay(entry.instance) : null;
    if (primary) instanceSet.add(primary);
  }
  return {
    days,
    defaultDate: days[days.length - 1] || todayIsoDate(),
    fi: fiOptions.sort((a, b) => a.label.localeCompare(b.label)),
    partners: Array.from(partnerSet).sort(),
    integrations: Array.from(integrationSet)
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b)),
    instances: Array.from(instanceSet).sort(),
  };
}

function createPlacementStore() {
  return {
    total: 0,
    billable: 0,
    siteFailures: 0,
    userFlowIssues: 0,
    daily: Object.create(null),
  };
}

function ensureDailyEntry(store, day) {
  if (!store.daily[day]) {
    store.daily[day] = {
      billable: 0,
      siteFailures: 0,
      userFlowIssues: 0,
      total: 0,
    };
  }
  return store.daily[day];
}

function summarizeStore(store, days) {
  const dayCells = days.map((day) => {
    const entry = store.daily[day] || {
      billable: 0,
      siteFailures: 0,
      userFlowIssues: 0,
      total: 0,
    };
    const billable = entry.billable || 0;
    const siteFailures = entry.siteFailures || 0;
    const userFlowIssues = entry.userFlowIssues || 0;
    const total = entry.total || 0;
    const denom = billable + siteFailures;
    const pct =
      denom > 0 ? Number(((billable / denom) * 100).toFixed(1)) : null;
    return {
      day,
      billable,
      siteFail: siteFailures,
      siteFailures,
      userFlowIssues,
      total,
      pct,
    };
  });

  const overallDenom = (store.billable || 0) + (store.siteFailures || 0);
  const overallHealthPct =
    overallDenom > 0
      ? Number(((store.billable / overallDenom) * 100).toFixed(1))
      : null;

  return {
    total: store.total || 0,
    billable: store.billable || 0,
    siteFailures: store.siteFailures || 0,
    userFlowIssues: store.userFlowIssues || 0,
    overallHealthPct,
    days: dayCells,
  };
}

async function buildGlobalMerchantHeatmap(startIso, endIso) {
  // Build day list
  const days = daysBetween(startIso, endIso);
  const slices = [];
  const instanceMeta = await loadInstanceMetaMap();

  for (const day of days) {
    const raw = await readPlacementDay(day);
    if (!raw || raw.error || !Array.isArray(raw.placements)) continue;

    for (const pl of raw.placements) {
      const merchant =
        pl.merchant_site_hostname ||
        (pl.merchant_site_id ? `merchant_${pl.merchant_site_id}` : "UNKNOWN");
      const instanceName =
        pl._instance ||
        pl.instance ||
        pl.instance_name ||
        pl.org_name ||
        "";
      const isTestInstance = isTestInstanceName(instanceName);
      const meta = instanceMeta.get(instanceName?.toLowerCase?.() || "");
      const fiKey = normalizeFiKey(
        pl.fi_lookup_key || pl.fi_name || meta?.fi || "unknown_fi"
      );

      const term = (pl.termination_type || "").toString().toUpperCase();
      const status = (pl.status || "").toString().toUpperCase();
      const rule =
        TERMINATION_RULES[term] ||
        TERMINATION_RULES[status] ||
        TERMINATION_RULES.UNKNOWN;

      const dKey = placementDay(pl) || day;

      const slice = {
        day: dKey,
        merchant,
        fi: fiKey || "unknown_fi",
        is_test: isTestInstance,
        total: 1,
        billable: 0,
        siteFailures: 0,
        userFlowIssues: 0,
      };
      if (rule.includeInHealth) {
        if (rule.severity === "success") {
          slice.billable = 1;
        } else {
          slice.siteFailures = 1;
        }
      } else if (rule.includeInUx) {
        slice.userFlowIssues = 1;
      } else {
        slice.siteFailures = 1;
      }
      slices.push(slice);
    }
  }

  return { start: startIso, end: endIso, days, slices };
}

const server = http.createServer(async (req, res) => {
  const parsedUrl = new URL(req.url, "http://localhost");
  const pathname = decodeURIComponent(parsedUrl.pathname);
  const search = parsedUrl.search;
  const queryParams = new URLSearchParams(search || "");

  // Log all HTTP requests (except asset/static files to reduce noise)
  const skipLogging = pathname.match(/\.(css|js|png|jpg|jpeg|gif|ico|svg|woff|woff2|ttf|eot)$/);
  if (!skipLogging && pathname !== "/server-logs") {
    const queryStr = search ? search : '';
    console.log(`${req.method} ${pathname}${queryStr}`);
  }

  if (pathname === "/run-update/status") {
    return send(res, 200, currentUpdateSnapshot());
  }

  if (pathname === "/run-update/stream") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });
    res.write("retry: 5000\n\n");

    updateClients.add(res);

    sseSend(res, "snapshot", currentUpdateSnapshot());

    if (!currentUpdateJob.running) {
      const qsStart = queryParams.get("start") || queryParams.get("startDate");
      const qsEnd = queryParams.get("end") || queryParams.get("endDate");
      const forceRaw = queryParams.get("forceRaw") === "true";
      const autoRefetch = queryParams.get("autoRefetch") === "1";

      if (autoRefetch) {
        console.log("[SSE] Auto-refetch triggered for incomplete dates");
      }

      startUpdateJobIfNeeded({ startDate: qsStart, endDate: qsEnd, forceRaw }).catch((err) => {
        console.error("Update job failed:", err);
      });
    }

    req.on("close", () => {
      updateClients.delete(res);
    });

    return;
  }

  // Check raw data metadata status
  if (pathname === "/api/check-raw-data") {
    const qsStart = queryParams.get("start");
    const qsEnd = queryParams.get("end");

    if (!qsStart || !qsEnd) {
      return send(res, 400, { error: "Missing start or end date" });
    }

    try {
      const { checkRawDataStatus } = await import("../src/lib/rawStorage.mjs");
      const dailySet = new Set((await listDaily()).map((f) => f.replace(/\.json$/i, "")));
      const datesToRefetch = [];
      const reasons = {};

      const start = new Date(qsStart);
      const end = new Date(qsEnd);

      // Enumerate dates in range
      for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
        const dateStr = d.toISOString().split("T")[0];

        // Check all three types (sessions, placements, ga)
        const sessionStatus = checkRawDataStatus("sessions", dateStr);
        const placementStatus = checkRawDataStatus("placements", dateStr);
        const gaStatus = checkRawDataStatus("ga", dateStr);
        const dailyMissing = !dailySet.has(dateStr);

        const needsRefetch =
          sessionStatus.needsRefetch ||
          placementStatus.needsRefetch ||
          gaStatus.needsRefetch ||
          dailyMissing;

        if (needsRefetch) {
          datesToRefetch.push(dateStr);
          reasons[dateStr] = {
            sessions: sessionStatus.reason,
            placements: placementStatus.reason,
            ga: gaStatus.reason,
            daily: dailyMissing ? "Daily rollup missing" : "Daily rollup present",
          };
        }
      }

      return send(res, 200, { datesToRefetch, reasons });
    } catch (err) {
      console.error("[API] check-raw-data error:", err);
      return send(res, 500, { error: err.message });
    }
  }

  // Diagnostics
  if (pathname === "/__diag") {
    const diag = {
      now: new Date().toISOString(),
      root: ROOT,
      public_dir: PUBLIC_DIR,
      data_dir: DATA_DIR,
      daily_dir: DAILY_DIR,
      public_files: await (async () => {
        try {
          return await fs.readdir(PUBLIC_DIR);
        } catch {
          return "(missing)";
        }
      })(),
      daily_sample: (await listDaily()).slice(0, 5),
      requested: pathname,
    };
    return send(res, 200, diag);
  }

  // JSON helpers
  if (pathname === "/list-daily") {
    const days = await listDaily();
    return send(res, 200, { files: days, days });
  }
  if (pathname === "/data-freshness") {
    try {
      const [rawSessionDays, rawPlacementDays, dailyDays] = await Promise.all([
        listRawDays("sessions"),
        listRawDays("placements"),
        listDaily(),
      ]);
      const latest = (arr = []) => (arr.length ? arr[arr.length - 1] : null);
      const rawLatest = latest(
        rawSessionDays.length && rawPlacementDays.length
          ? rawSessionDays.filter((d) => rawPlacementDays.includes(d))
          : rawSessionDays.length
          ? rawSessionDays
          : rawPlacementDays
      );
      const dailyLatest = latest(dailyDays);
      const today = todayIsoDate();
      const age = (iso) => {
        if (!iso) return null;
        const ms = new Date(`${today}T00:00:00Z`) - new Date(`${iso}T00:00:00Z`);
        return Math.floor(ms / 86400000);
      };
      return send(res, 200, {
        rawLatest,
        rawAgeDays: age(rawLatest),
        dailyLatest,
        dailyAgeDays: age(dailyLatest),
      });
    } catch (err) {
      return send(res, 500, { error: err?.message || "Unable to load freshness" });
    }
  }
  if (pathname === "/merchant-sites") {
    try {
      const sites = await fetchMerchantSitesFromSs01();
      return send(res, 200, { count: sites.length, sites });
    } catch (err) {
      const message = err?.message || "Unable to load merchant sites";
      console.error("merchant-sites fetch failed", err);
      return send(res, 500, { error: message });
    }
  }
  if (pathname === "/fi-api-data") {
    try {
      console.log("Fetching FI data from all instances...");
      const data = await fetchAllFinancialInstitutions();
      console.log(`✅ FI API data fetch complete: ${data.totalCount} FIs total`);
      return send(res, 200, data);
    } catch (err) {
      const message = err?.message || "Unable to load FI API data";
      console.error("FI API data fetch failed", err);
      return send(res, 500, { error: message });
    }
  }

  if (pathname === "/fi-api-data-stream") {
    try {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
      });

      const sendEvent = (data) => {
        res.write(`data: ${JSON.stringify(data)}\n\n`);
      };

      console.log("Streaming FI data from all instances...");
      const data = await fetchAllFinancialInstitutions(sendEvent);
      console.log(`✅ FI API data fetch complete: ${data.totalCount} FIs total`);

      // Send final data
      sendEvent({ type: 'complete', data });
      res.end();
    } catch (err) {
      const message = err?.message || "Unable to load FI API data";
      console.error("FI API data fetch failed", err);
      res.write(`data: ${JSON.stringify({ type: 'error', error: message })}\n\n`);
      res.end();
    }
    return;
  }

  if (pathname === "/server-logs") {
    try {
      const query = new URLSearchParams(parsedUrl.searchParams);
      const limit = parseInt(query.get('limit')) || 500;
      const level = query.get('level') || null;

      let logs = serverLogs.slice();

      // Filter by level if specified
      if (level && level !== 'all') {
        logs = logs.filter(log => log.level === level);
      }

      // Return most recent logs (last N)
      const recentLogs = logs.slice(-limit);

      return send(res, 200, {
        logs: recentLogs,
        totalCount: serverLogs.length,
        maxLines: MAX_LOG_LINES
      });
    } catch (err) {
      console.error("Server logs fetch failed", err);
      return send(res, 500, { error: err.message });
    }
  }

  // Placement details endpoint for expandable breakdown
  if (pathname === "/api/placement-details") {
    try {
      const query = new URLSearchParams(parsedUrl.searchParams);
      const type = query.get('type'); // 'success', 'system', or 'ux'
      const startDate = query.get('startDate');
      const endDate = query.get('endDate');
      const fiFilter = query.get('fi') || '__all__';
      const partnerFilter = query.get('partner') || '__all_partners__';
      const integrationFilter = query.get('integration') || '(all)';
      const instanceFilter = query.get('instance') || 'All';
      const includeTest = query.get('includeTest') === 'true';
      const limit = parseInt(query.get('limit')) || 50;
      const showAll = query.get('showAll') === 'true';

      // Validate required params
      if (!type || !startDate || !endDate) {
        return send(res, 400, { error: 'Missing required parameters: type, startDate, endDate' });
      }

      if (!['success', 'system', 'ux', 'nojobs'].includes(type)) {
        return send(res, 400, { error: 'Invalid type. Must be success, system, ux, or nojobs' });
      }

      // Load FI registry for integration type lookups
      let fiRegistry = {};
      try {
        const raw = await fs.readFile(FI_REGISTRY_FILE, "utf8");
        fiRegistry = JSON.parse(raw);
      } catch (err) {
        console.warn("Could not load FI registry:", err.message);
      }

      // Get date range
      const dates = [];
      const start = new Date(`${startDate}T00:00:00Z`);
      const end = new Date(`${endDate}T00:00:00Z`);
      for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
        dates.push(d.toISOString().split('T')[0]);
      }

      // Handle "nojobs" type separately - show sessions without jobs
      if (type === 'nojobs') {
        const instanceMeta = await loadInstanceMetaMap();
        const fiMeta = buildFiMetaMap(fiRegistry);
        const noJobSessions = [];

        for (const date of dates) {
          const sessionData = await readSessionDay(date);
          if (!sessionData?.sessions) continue;

          for (const session of sessionData.sessions) {
            // Skip sessions that have jobs
            const totalJobs = session.total_jobs ?? 0;
            if (totalJobs > 0) continue;

            // Apply filters
            const instanceRaw = session._instance || session.instance || session.instance_name || session.org_name || "";
            const instanceDisplay = formatInstanceDisplay(instanceRaw || "unknown");

            // Test instance filter
            if (!includeTest && isTestInstanceName(instanceDisplay)) continue;

            // Instance filter
            if (instanceFilter !== 'All') {
              const normalizedInstance = canonicalInstance(instanceDisplay);
              const normalizedFilter = canonicalInstance(instanceFilter);
              if (normalizedInstance !== normalizedFilter) continue;
            }

            // FI filter
            const fiKey = normalizeFiKey(session.financial_institution_lookup_key || session.fi_lookup_key || session.fi_name || '');
            if (fiFilter !== '__all__') {
              const allowedFis = fiFilter.split(',').map(f => normalizeFiKey(f.trim()));
              if (!allowedFis.includes(fiKey)) continue;
            }

            // Integration filter
            const fiEntry = fiMeta.get(fiKey);
            const integration = normalizeIntegration(session.source?.integration || fiEntry?.integration || 'UNKNOWN');
            if (integrationFilter !== '(all)') {
              if (integration !== normalizeIntegration(integrationFilter)) continue;
            }

            // Determine last page visited
            const clickstream = Array.isArray(session.clickstream) ? session.clickstream : [];
            const lastPage = clickstream.length > 0 ? clickstream[clickstream.length - 1] : null;
            const lastUrl = lastPage?.url || lastPage?.page_title || 'Unknown';

            // Calculate session duration
            const createdOn = session.created_on ? new Date(session.created_on) : null;
            const closedOn = session.closed_on ? new Date(session.closed_on) : null;
            const durationMs = (createdOn && closedOn) ? closedOn - createdOn : null;

            noJobSessions.push({
              date,
              sessionId: session.agent_session_id || session.id || session.cuid,
              cuid: session.cuid,
              fi: fiEntry?.fi || session.fi_name || fiKey || 'Unknown',
              fiKey,
              integration,
              instance: instanceDisplay,
              lastPage: lastUrl,
              clickstreamLength: clickstream.length,
              clickstream,
              createdOn: session.created_on,
              closedOn: session.closed_on,
              durationMs,
              _rawSession: session
            });
          }
        }

        // Group by last page visited
        const pageGroups = {};
        for (const sess of noJobSessions) {
          const page = sess.lastPage;
          if (!pageGroups[page]) {
            pageGroups[page] = [];
          }
          pageGroups[page].push(sess);
        }

        // Sort pages by frequency
        const sortedPages = Object.keys(pageGroups).sort((a, b) => {
          return pageGroups[b].length - pageGroups[a].length;
        });

        const resultLimit = showAll ? Infinity : limit;
        const results = [];
        let totalCount = 0;

        for (const page of sortedPages) {
          const sessions = pageGroups[page];
          totalCount += sessions.length;

          if (results.length < resultLimit) {
            results.push({
              page: page,
              count: sessions.length,
              sessions: sessions
            });
          }
        }

        return send(res, 200, {
          type: 'nojobs',
          total: totalCount,
          pageCount: sortedPages.length,
          showing: results.length,
          hasMore: results.length < sortedPages.length,
          results
        });
      }

      // Collect all placements matching criteria
      const allPlacements = [];

      // Load sessions for each date to match with placements
      const sessionsByDate = {};
      for (const date of dates) {
        const sessionData = await readSessionDay(date);
        if (sessionData?.sessions) {
          // Index sessions by agent_session_id for fast lookup
          sessionsByDate[date] = {};
          for (const session of sessionData.sessions) {
            const sessionId = session.agent_session_id || session.id;
            if (sessionId) {
              sessionsByDate[date][sessionId] = session;
            }
          }
        }
      }

      for (const date of dates) {
        const placementFile = path.join(RAW_PLACEMENTS_DIR, `${date}.json`);

        try {
          const raw = await fs.readFile(placementFile, 'utf8');
          const data = JSON.parse(raw);
          const placements = data.placements || [];

          for (const placement of placements) {
            // Apply filters
            const fiKey = normalizeFiKey(placement.fi_lookup_key || placement.fi_name || '');
            const instance = placement._instance || '';
            const terminationType = placement.termination_type || 'UNKNOWN';

            // Test instance filter
            if (!includeTest && isTestInstanceName(instance)) {
              continue;
            }

            // Instance filter
            if (instanceFilter !== 'All') {
              const normalizedInstance = canonicalInstance(formatInstanceDisplay(instance));
              const normalizedFilter = canonicalInstance(instanceFilter);
              if (normalizedInstance !== normalizedFilter) {
                continue;
              }
            }

            // FI filter (handle comma-separated list)
            if (fiFilter !== '__all__') {
              const allowedFis = fiFilter.split(',').map(f => normalizeFiKey(f.trim()));
              if (!allowedFis.includes(fiKey)) {
                continue;
              }
            }

            // Categorize by termination type (but don't filter yet - we need all types for counts)
            const rule = TERMINATION_RULES[terminationType] || TERMINATION_RULES.UNKNOWN;
            let placementType = 'system'; // default

            if (rule.severity === 'success') {
              placementType = 'success';
            } else if (rule.includeInUx) {
              placementType = 'ux';
            } else if (rule.includeInHealth && rule.severity !== 'success') {
              placementType = 'system';
            }
            // Note: We're NOT filtering by type here - we collect all placements to show full counts

            // Derive integration type
            let integrationType = 'NON-SSO';
            if (placement.source?.integration) {
              const srcInt = placement.source.integration.toString().toLowerCase();
              if (srcInt.includes('sso')) integrationType = 'SSO';
              else if (srcInt.includes('cardsavr')) integrationType = 'CardSavr';
            } else if (fiRegistry[fiKey]) {
              const regInt = (fiRegistry[fiKey].integration_type || '').toString().toLowerCase();
              if (regInt === 'sso') integrationType = 'SSO';
              else if (regInt === 'cardsavr') integrationType = 'CardSavr';
            }

            // Integration filter
            if (integrationFilter !== '(all)') {
              const normalizedInt = integrationType.toUpperCase().replace(/[^A-Z]/g, '');
              const filterInt = integrationFilter.toUpperCase().replace(/[^A-Z]/g, '');
              if (normalizedInt !== filterInt) {
                continue;
              }
            }

            // Find matching session
            const sessionId = placement.agent_session_id;
            const matchingSession = sessionId && sessionsByDate[date]?.[sessionId];

            // Add to results with necessary fields + raw data + session
            allPlacements.push({
              merchant: placement.merchant_site_hostname || 'Unknown',
              fi: placement.fi_name || 'Unknown',
              instance: instance || 'unknown',
              integration: integrationType,
              terminationType: terminationType,
              placementType: placementType, // success, system, or ux
              status: placement.status || '',
              statusMessage: placement.status_message || '',
              jobId: placement.id || placement.place_card_on_single_site_job_id || '',
              createdOn: placement.job_created_on || placement.created_on || '',
              completedOn: placement.completed_on || '',
              timeElapsed: placement.time_elapsed || 0,
              date: date,
              _raw: placement, // Include full raw placement object
              _session: matchingSession || null, // Include matching session if found
            });
          }
        } catch (err) {
          // Skip missing files
          if (err.code !== 'ENOENT') {
            console.error(`Error reading placements for ${date}:`, err);
          }
        }
      }

      // Group by merchant and count all placement types
      const merchantGroups = {};
      for (const placement of allPlacements) {
        const merchant = placement.merchant;
        if (!merchantGroups[merchant]) {
          merchantGroups[merchant] = {
            allPlacements: [],
            successCount: 0,
            systemCount: 0,
            uxCount: 0
          };
        }
        merchantGroups[merchant].allPlacements.push(placement);

        // Count this placement based on its placementType
        if (placement.placementType === 'success') {
          merchantGroups[merchant].successCount++;
        } else if (placement.placementType === 'ux') {
          merchantGroups[merchant].uxCount++;
        } else {
          merchantGroups[merchant].systemCount++;
        }
      }

      // Debug: Log first merchant's counts
      const firstMerchant = Object.keys(merchantGroups)[0];
      if (firstMerchant) {
        console.log(`[DEBUG] First merchant "${firstMerchant}":`, {
          total: merchantGroups[firstMerchant].allPlacements.length,
          success: merchantGroups[firstMerchant].successCount,
          system: merchantGroups[firstMerchant].systemCount,
          ux: merchantGroups[firstMerchant].uxCount,
          sampleTypes: merchantGroups[firstMerchant].allPlacements.slice(0, 5).map(p => p.placementType)
        });
      }

      // Sort merchants by frequency (most common first) based on current type's count
      const sortedMerchants = Object.keys(merchantGroups).sort((a, b) => {
        const countA = type === 'success' ? merchantGroups[a].successCount :
                       type === 'ux' ? merchantGroups[a].uxCount :
                       merchantGroups[a].systemCount;
        const countB = type === 'success' ? merchantGroups[b].successCount :
                       type === 'ux' ? merchantGroups[b].uxCount :
                       merchantGroups[b].systemCount;
        return countB - countA;
      });

      // Build response with top 50 or all
      const resultLimit = showAll ? Infinity : limit;
      const results = [];
      let totalCount = 0;

      for (const merchant of sortedMerchants) {
        const group = merchantGroups[merchant];

        // Filter placements to only show the requested type
        const typedPlacements = group.allPlacements.filter(p => p.placementType === type);
        const typeCount = typedPlacements.length;

        // Skip merchants with zero of the requested type
        if (typeCount === 0) continue;

        totalCount += typeCount;

        if (results.length < resultLimit) {
          results.push({
            merchant: merchant,
            count: typeCount, // Count for the requested type
            successCount: group.successCount,
            systemCount: group.systemCount,
            uxCount: group.uxCount,
            placements: typedPlacements, // Show all placements for this merchant
          });
        }
      }

      return send(res, 200, {
        type,
        total: totalCount,
        merchantCount: sortedMerchants.length,
        showing: results.length,
        hasMore: results.length < sortedMerchants.length,
        results,
      });

    } catch (err) {
      console.error('Placement details fetch failed:', err);
      return send(res, 500, { error: err.message });
    }
  }

  // Data version endpoint for cache invalidation
  if (pathname === "/api/data-version") {
    try {
      // Get list of available daily files
      const files = await fs.readdir(DAILY_DIR).catch(() => []);
      const dailyFiles = files.filter(f => f.endsWith('.json')).sort();

      // Create version from file list + file stats.
      // This invalidates the cache when daily files are rewritten (e.g. force refresh),
      // not just when files are added/removed.
      const statParts = [];
      for (const name of dailyFiles) {
        try {
          const stat = await fs.stat(path.join(DAILY_DIR, name));
          statParts.push(`${name}:${stat.size}:${stat.mtimeMs}`);
        } catch {
          statParts.push(`${name}:?`);
        }
      }
      const fileListHash = statParts.join('|');
      let version = 0;
      for (let i = 0; i < fileListHash.length; i++) {
        version = ((version << 5) - version) + fileListHash.charCodeAt(i);
        version = version & version; // Convert to 32bit integer
      }

      return send(res, 200, {
        version: Math.abs(version),
        fileCount: dailyFiles.length,
        dateRange: dailyFiles.length > 0 ? {
          start: dailyFiles[0].replace('.json', ''),
          end: dailyFiles[dailyFiles.length - 1].replace('.json', '')
        } : null
      });
    } catch (err) {
      console.error('Data version check failed:', err);
      return send(res, 500, { error: err.message });
    }
  }

  if (pathname === "/fi-registry") {
    try {
      const raw = await fs.readFile(FI_REGISTRY_FILE, "utf8");
      return send(res, 200, JSON.parse(raw));
    } catch (err) {
      const status = err.code === "ENOENT" ? 404 : 500;
      return send(res, status, { error: "fi_registry.json not found" });
    }
  }
  if (pathname === "/fi-registry/update" && req.method === "POST") {
    try {
      const rawBody = await readRequestBody(req);
      const payload = JSON.parse(rawBody || "{}");
      if (!payload || typeof payload !== "object") {
        return send(res, 400, { error: "Invalid payload" });
      }
      const { key, updates } = payload;
      if (!key || typeof updates !== "object" || Array.isArray(updates)) {
        return send(res, 400, { error: "Missing key or updates" });
      }
      const raw = await fs.readFile(FI_REGISTRY_FILE, "utf8").catch((err) => {
        if (err.code === "ENOENT") {
          throw Object.assign(new Error("fi_registry.json not found"), { status: 404 });
        }
        throw err;
      });
      const registry = JSON.parse(raw);
      if (!registry[key]) {
        return send(res, 404, { error: "Registry entry not found", key });
      }

      const normalizeIntegration = (value) => {
        if (!value) return "non-sso";
        const rawVal = value.toString().trim().toLowerCase();
        if (rawVal === "sso") return "sso";
        if (rawVal === "cardsavr" || rawVal === "card-savr") return "cardsavr";
        if (rawVal === "test") return "test";
        if (rawVal === "unknown") return "unknown";
        return "non-sso";
      };
      const normalizeCardholders = (value) => {
        if (value === null || value === undefined || value === "") return null;
        const cleaned = value.toString().replace(/,/g, "").trim();
        if (!cleaned) return null;
        const num = Number(cleaned);
        if (!Number.isFinite(num) || num < 0) {
          throw Object.assign(new Error("Cardholder total must be a positive number"), {
            status: 400,
          });
        }
        return String(Math.round(num));
      };
      const normalizeAsOf = (value) => {
        if (!value) return null;
        const str = value.toString().trim();
        if (!/^\d{4}-\d{2}-\d{2}$/.test(str)) {
          throw Object.assign(new Error("cardholder_as_of must be YYYY-MM-DD"), {
            status: 400,
          });
        }
        return str;
      };
      const normalizeSource = (value) => {
        if (!value) return null;
        return value.toString().trim();
      };
      const normalizeFreeText = (value) => {
        if (value === undefined) return undefined;
        const str = value === null ? "" : value.toString().trim();
        return str === "" ? null : str;
      };
      const normalizeFiName = (value) => {
        if (value === undefined) return undefined;
        const str = value === null ? "" : value.toString().trim();
        if (!str) {
          throw Object.assign(new Error("fi_name is required"), { status: 400 });
        }
        return str;
      };
      const normalizeFiLookupKey = (value, fallback) => {
        const raw = value === undefined ? fallback : value;
        if (raw === undefined) return undefined;
        const str = raw === null ? "" : raw.toString().trim();
        if (!str) {
          throw Object.assign(new Error("fi_lookup_key is required"), { status: 400 });
        }
        return str;
      };
      const normalizePartner = (value) => {
        if (!value) return null;
        const rawVal = value.toString().trim().toLowerCase();
        const canonical =
          {
            alkami: "Alkami",
            "digital-onboarding": "DigitalOnboarding",
            digitalonboarding: "DigitalOnboarding",
            pscu: "PSCU",
            marquis: "Marquis",
            msu: "MSU",
            advancial: "Advancial",
            "advancial-prod": "Advancial",
            cardsavr: "CardSavr",
            direct: "Direct",
          }[rawVal] || rawVal;
        return canonical
          .replace(/(^|\s|-)([a-z])/g, (m, p1, p2) => p1 + p2.toUpperCase());
      };
      const canonicalLookupKey = (value) =>
        value ? value.toString().trim().toLowerCase() : "";
      const canonicalInstance = (value) =>
        value ? value.toString().trim().toLowerCase() : "";

      const next = { ...registry[key] };
      if ("integration_type" in updates) {
        next.integration_type = normalizeIntegration(updates.integration_type);
      }
      if ("fi_name" in updates) {
        const fiName = normalizeFiName(updates.fi_name);
        if (fiName !== undefined) next.fi_name = fiName;
      }
      if ("fi_lookup_key" in updates) {
        const fiLookup = normalizeFiLookupKey(updates.fi_lookup_key, next.fi_lookup_key);
        if (fiLookup !== undefined) next.fi_lookup_key = fiLookup;
      }
      if ("partner" in updates) {
        next.partner = normalizePartner(updates.partner);
      }
      if ("cardholder_total" in updates) {
        next.cardholder_total = normalizeCardholders(updates.cardholder_total);
      }
      if ("cardholder_source" in updates) {
        next.cardholder_source = normalizeSource(updates.cardholder_source);
      }
      if ("cardholder_as_of" in updates) {
        next.cardholder_as_of = normalizeAsOf(updates.cardholder_as_of);
      }
      if ("core_vendor" in updates) {
        next.core_vendor = normalizeFreeText(updates.core_vendor);
      }
      if ("core_product" in updates) {
        next.core_product = normalizeFreeText(updates.core_product);
      }
      if ("debit_processor" in updates) {
        next.debit_processor = normalizeFreeText(updates.debit_processor);
      }
      if ("credit_processor" in updates) {
        next.credit_processor = normalizeFreeText(updates.credit_processor);
      }

      const targetLookup = canonicalLookupKey(next.fi_lookup_key || next.fi_name || key);
      const targetInstance = canonicalInstance(
        next.instance || (Array.isArray(next.instances) ? next.instances[0] : "")
      );
      for (const [otherKey, otherEntry] of Object.entries(registry)) {
        if (otherKey === key) continue;
        const otherLookup = canonicalLookupKey(
          otherEntry?.fi_lookup_key || otherEntry?.fi_name || otherKey
        );
        const otherInstance = canonicalInstance(
          otherEntry?.instance ||
            (Array.isArray(otherEntry?.instances) ? otherEntry.instances[0] : "")
        );
        if (
          targetLookup &&
          otherLookup &&
          targetInstance &&
          otherInstance &&
          targetLookup === otherLookup &&
          targetInstance === otherInstance
        ) {
          return send(res, 409, {
            error: "Duplicate fi_lookup_key for this instance.",
            conflict: {
              key: otherKey,
              fi_lookup_key: otherEntry?.fi_lookup_key || null,
              instance:
                otherEntry?.instance ||
                (Array.isArray(otherEntry?.instances) ? otherEntry.instances[0] : null),
            },
          });
        }
      }

      registry[key] = next;
      await fs.writeFile(
        FI_REGISTRY_FILE,
        JSON.stringify(registry, null, 2) + "\n",
        "utf8"
      );
      return send(res, 200, { key, entry: next });
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err.message || "Unable to update registry" });
    }
  }
  if (pathname === "/fi-registry/delete" && req.method === "POST") {
    try {
      const rawBody = await readRequestBody(req);
      const payload = JSON.parse(rawBody || "{}");
      const key = payload?.key;
      if (!key) {
        return send(res, 400, { error: "Missing key" });
      }
      const raw = await fs.readFile(FI_REGISTRY_FILE, "utf8").catch((err) => {
        if (err.code === "ENOENT") {
          throw Object.assign(new Error("fi_registry.json not found"), { status: 404 });
        }
        throw err;
      });
      const registry = JSON.parse(raw);
      if (!registry[key]) {
        return send(res, 404, { error: "Registry entry not found", key });
      }
      delete registry[key];
      await fs.writeFile(
        FI_REGISTRY_FILE,
        JSON.stringify(registry, null, 2) + "\n",
        "utf8"
      );
      return send(res, 200, { deleted: key, registrySize: Object.keys(registry).length });
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err.message || "Unable to delete registry entry" });
    }
  }

  // New endpoint: Reload FI registry from instances
  if (pathname === "/fi-registry/reload-from-instances" && req.method === "POST") {
    try {
      const instances = await loadInstances(ROOT);
      if (!instances || !instances.length) {
        return send(res, 400, { error: "No instances configured" });
      }

      // Read existing registry
      let registry = {};
      try {
        const raw = await fs.readFile(FI_REGISTRY_FILE, "utf8");
        registry = JSON.parse(raw || "{}");
      } catch (err) {
        if (err.code !== "ENOENT") throw err;
      }

      const makeKey = (fi, inst) => {
        const normFi = (fi || "").toString().trim().toLowerCase();
        const normInst = (inst || "unknown").toString().trim().toLowerCase() || "unknown";
        return `${normFi}__${normInst}`;
      };

      const existingKeys = new Set(Object.keys(registry).map(k => k.toLowerCase()));
      let newCount = 0;
      const errors = [];

      // Fetch FIs from each instance
      for (const instance of instances) {
        try {
          console.log(`Fetching FIs from instance: ${instance.name}`);
          const { session } = await loginWithSdk(instance);

          // Fetch all pages of FIs
          let allFis = [];
          let pagingHeader = {};
          let hasMore = true;

          while (hasMore) {
            const result = await getFinancialInstitutionsPage(session, pagingHeader);
            const fis = result.rows || [];
            allFis = allFis.concat(fis);

            // Check if there are more pages
            if (result.raw && result.raw.headers && result.raw.headers["x-cardsavr-paging"]) {
              const pagingJson = JSON.parse(result.raw.headers["x-cardsavr-paging"]);
              hasMore = pagingJson.is_last_page === false;
              if (hasMore) {
                pagingHeader = { "x-cardsavr-paging": JSON.stringify({ page: (pagingJson.page || 0) + 1 }) };
              }
            } else {
              hasMore = false;
            }
          }

          console.log(`Found ${allFis.length} FIs in instance ${instance.name}`);

          // Add new FIs to registry
          for (const fi of allFis) {
            const fiName = fi.name || fi.lookup_key || "Unknown";
            const fiLookupKey = fi.lookup_key || fi.name || "";
            if (!fiLookupKey) continue;

            const key = makeKey(fiLookupKey, instance.name);

            // Skip if already exists
            if (existingKeys.has(key)) continue;

            // Determine integration type (guess based on instance name or FI metadata)
            const guessedIntegration = /dev|test/i.test(instance.name) ? "TEST" : "NON-SSO";

            registry[key] = {
              fi_name: fiName,
              fi_lookup_key: fiLookupKey.toLowerCase(),
              instance: instance.name,
              integration_type: guessedIntegration,
              partner: "Unknown",
              sources: ["api"],
              first_seen: new Date().toISOString().slice(0, 10),
            };

            existingKeys.add(key);
            newCount++;
          }
        } catch (err) {
          console.error(`Error fetching FIs from ${instance.name}:`, err);
          errors.push({ instance: instance.name, error: err.message });
        }
      }

      // Save updated registry
      await fs.writeFile(
        FI_REGISTRY_FILE,
        JSON.stringify(registry, null, 2) + "\n",
        "utf8"
      );

      return send(res, 200, {
        success: true,
        newCount,
        totalCount: Object.keys(registry).length,
        errors: errors.length > 0 ? errors : undefined,
      });
    } catch (err) {
      console.error("Error reloading registry from instances:", err);
      const status = err?.status || 500;
      return send(res, status, { error: err.message || "Unable to reload registry from instances" });
    }
  }

  if (pathname === "/troubleshoot/options") {
    try {
      const opts = await buildTroubleshootOptions();
      return send(res, 200, opts);
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err?.message || "Unable to load options" });
    }
  }
  if (pathname === "/troubleshoot/day") {
    const startParam =
      queryParams.get("start") ||
      queryParams.get("startDate") ||
      queryParams.get("date") ||
      queryParams.get("day");
    const endParam = queryParams.get("end") || queryParams.get("endDate") || startParam;
    const isoRe = /^\d{4}-\d{2}-\d{2}$/;
    if (!startParam || !isoRe.test(startParam)) {
      return send(res, 400, { error: "start date query param must be YYYY-MM-DD" });
    }
    if (!endParam || !isoRe.test(endParam)) {
      return send(res, 400, { error: "end date query param must be YYYY-MM-DD" });
    }
    const startDate = startParam;
    const endDate = endParam;
    if (new Date(`${startDate}T00:00:00Z`) > new Date(`${endDate}T00:00:00Z`)) {
      return send(res, 400, { error: "start date must be on or before end date" });
    }
    const includeTests = queryParams.get("includeTests") === "true";
    const fiFilter = queryParams.get("fi") || FI_ALL_VALUE;
    const partnerFilter = queryParams.get("partner") || PARTNER_ALL_VALUE;
    const instanceFilter = queryParams.get("instance") || INSTANCE_ALL_VALUE;
    const rawIntegrationFilter = queryParams.get("integration") || "(all)";
    const integrationFilter =
      rawIntegrationFilter === "(all)" ? "(all)" : normalizeIntegration(rawIntegrationFilter);
    try {
      const [rangeData, fiRegistry, instanceMeta] = await Promise.all([
        loadTroubleshootRange(startDate, endDate),
        loadFiRegistrySafe(),
        loadInstanceMetaMap(),
      ]);
      if (!rangeData.sessions.length && !rangeData.placements.length) {
        return send(res, 404, { error: "No raw data found for date range", startDate, endDate });
      }
      const fiMeta = buildFiMetaMap(fiRegistry);
      const payload = buildTroubleshootPayload(
        `${startDate} → ${endDate}`,
        { sessions: rangeData.sessions },
        { placements: rangeData.placements },
        fiMeta,
        instanceMeta
      );
      const filteredSessions = payload.sessions.filter((row) => {
        if (!includeTests && row.is_test) return false;
        if (fiFilter && fiFilter !== FI_ALL_VALUE) {
          if (normalizeFiKey(row.fi_key) !== normalizeFiKey(fiFilter)) return false;
        }
        if (integrationFilter !== "(all)" && row.integration !== integrationFilter) {
          return false;
        }
        if (partnerFilter && partnerFilter !== PARTNER_ALL_VALUE) {
          if ((row.partner || "Unknown") !== partnerFilter) return false;
        }
        if (instanceFilter && instanceFilter !== INSTANCE_ALL_VALUE) {
          if (canonicalInstance(row.instance) !== canonicalInstance(instanceFilter)) return false;
        }
        return true;
      });
      const totals = summarizeTroubleshootSessions(filteredSessions);
      return send(res, 200, {
        date: payload.date,
        startDate,
        endDate,
        totals,
        sessions: filteredSessions,
        placements: payload.placements,
        filters: {
          fi: fiFilter,
          integration: rawIntegrationFilter,
          partner: partnerFilter,
          instance: instanceFilter,
          includeTests,
        },
      });
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err?.message || "Unable to load troubleshooting data" });
    }
  }
  if (pathname === "/instances") {
    try {
      const { entries, path: foundAt } = await readInstancesFile();
      return send(res, 200, { instances: entries, path: foundAt });
    } catch (err) {
      console.error("instances load failed", err);
      const status = err?.status || 500;
      return send(res, status, { error: err.message || "Unable to read instances" });
    }
  }
  if (pathname === "/ga/service-account" && req.method === "GET") {
    try {
      const data = await readGaCredentialSummary("prod");
      return send(res, 200, data);
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err?.message || "Unable to read GA credential" });
    }
  }
  if (pathname === "/ga/service-account" && req.method === "POST") {
    try {
      const rawBody = await readRequestBody(req);
      const payload = rawBody ? JSON.parse(rawBody) : {};
      const saved = await writeGaCredentialFile("prod", payload);
      return send(res, 200, saved);
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err?.message || "Unable to save GA credential" });
    }
  }
  if (pathname === "/ga/service-account/delete" && req.method === "POST") {
    try {
      const saved = await deleteGaCredentialFile("prod");
      return send(res, 200, saved);
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err?.message || "Unable to delete GA credential" });
    }
  }
  if (pathname === "/ga/service-account/test" && req.method === "POST") {
    try {
      const rawBody = await readRequestBody(req);
      const payload = rawBody ? JSON.parse(rawBody) : {};
      const date =
        payload?.date && /^\d{4}-\d{2}-\d{2}$/.test(payload.date)
          ? payload.date
          : yesterdayIsoDate();
      const propertyId = (payload?.propertyId || process.env.GA_PROPERTY_ID || "328054560").toString();

      const summary = await readGaCredentialSummary("prod");
      if (!summary.exists) {
        return send(res, 400, { ok: false, error: "GA credential not configured. Upload JSON first." });
      }
      const rows = await fetchGaRowsForDay({
        date,
        propertyId,
        keyFile: GA_SERVICE_ACCOUNT_FILE,
      });
      const fiSet = new Set((rows || []).map((r) => r && r.fi_key).filter(Boolean));
      return send(res, 200, {
        ok: true,
        date,
        propertyId,
        rows: Array.isArray(rows) ? rows.length : 0,
        fis: fiSet.size,
        sample: Array.isArray(rows) ? rows.slice(0, 3) : [],
      });
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { ok: false, error: err?.message || "GA test failed" });
    }
  }
  if (pathname === "/ga/credentials" && req.method === "GET") {
    try {
      const credentials = await Promise.all(GA_CREDENTIALS.map((c) => readGaCredentialSummary(c.name)));
      return send(res, 200, { credentials });
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err?.message || "Unable to list GA credentials" });
    }
  }
  if (pathname === "/ga/credential" && req.method === "GET") {
    try {
      const name = queryParams.get("name") || "";
      const data = await readGaCredentialContent(name);
      return send(res, 200, data);
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err?.message || "Unable to read GA credential" });
    }
  }
  if (pathname === "/ga/credential/save" && req.method === "POST") {
    try {
      const rawBody = await readRequestBody(req);
      const payload = rawBody ? JSON.parse(rawBody) : {};
      const name = payload?.name || "";
      const saved = await writeGaCredentialFile(name, payload);
      return send(res, 200, saved);
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err?.message || "Unable to save GA credential" });
    }
  }
  if (pathname === "/ga/credential/delete" && req.method === "POST") {
    try {
      const rawBody = await readRequestBody(req);
      const payload = rawBody ? JSON.parse(rawBody) : {};
      const name = payload?.name || "";
      const saved = await deleteGaCredentialFile(name);
      return send(res, 200, saved);
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err?.message || "Unable to delete GA credential" });
    }
  }
  if (pathname === "/ga/credential/test" && req.method === "POST") {
    try {
      const rawBody = await readRequestBody(req);
      const payload = rawBody ? JSON.parse(rawBody) : {};
      const name = payload?.name || "";
      const cfg = getGaCredentialConfig(name);
      const summary = await readGaCredentialSummary(cfg.name);
      if (!summary.exists) {
        return send(res, 400, { ok: false, error: "GA credential not configured. Upload or paste JSON first." });
      }
      const date =
        payload?.date && /^\d{4}-\d{2}-\d{2}$/.test(payload.date)
          ? payload.date
          : yesterdayIsoDate();
      const envPropertyId = process.env[cfg.envProperty] || "";
      const propertyId = (payload?.propertyId || envPropertyId || cfg.defaultProperty || "328054560").toString();

      const rows = await fetchGaRowsForDay({
        date,
        propertyId,
        keyFile: cfg.file,
      });
      const fiSet = new Set((rows || []).map((r) => r && r.fi_key).filter(Boolean));
      return send(res, 200, {
        ok: true,
        name: cfg.name,
        date,
        propertyId,
        rows: Array.isArray(rows) ? rows.length : 0,
        fis: fiSet.size,
        sample: Array.isArray(rows) ? rows.slice(0, 3) : [],
      });
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { ok: false, error: err?.message || "GA test failed" });
    }
  }
  if (pathname === "/instances/test" && req.method === "POST") {
    try {
      const rawBody = await readRequestBody(req);
      const payload = rawBody ? JSON.parse(rawBody) : {};
      const date =
        payload?.date && /^\d{4}-\d{2}-\d{2}$/.test(payload.date)
          ? payload.date
          : yesterdayIsoDate();

      const { entries } = await readInstancesFile();
      const normalized = entries.map(normalizeInstanceEntry);

      const results = [];
      const failures = [];
      for (const instance of normalized) {
        const instanceName = instance.name || "default";
        try {
          const { session } = await loginWithSdk(instance);
          await getSessionsPage(session, date, date, null);
          await getCardPlacementPage(session, date, date, null);
          results.push({ instanceName, ok: true });
        } catch (err) {
          const msg = err?.message || String(err);
          results.push({ instanceName, ok: false, error: msg });
          failures.push({ instanceName, error: msg });
        }
      }

      return send(res, 200, {
        ok: failures.length === 0,
        date,
        tested: results.length,
        failures: failures.length,
        failingInstances: failures.map((f) => f.instanceName),
        results,
      });
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err?.message || "Unable to test instances" });
    }
  }
	  if (pathname === "/sessions/jobs-stats") {
	    const startParam =
	      queryParams.get("start") ||
	      queryParams.get("startDate") ||
      queryParams.get("date");
    const endParam =
      queryParams.get("end") ||
      queryParams.get("endDate") ||
      startParam;
    const isoRe = /^\d{4}-\d{2}-\d{2}$/;
    if (!startParam || !isoRe.test(startParam)) {
      return send(res, 400, { error: "start date query param must be YYYY-MM-DD" });
    }
    if (!endParam || !isoRe.test(endParam)) {
      return send(res, 400, { error: "end date query param must be YYYY-MM-DD" });
    }
    if (new Date(`${startParam}T00:00:00Z`) > new Date(`${endParam}T00:00:00Z`)) {
      return send(res, 400, { error: "start date must be on or before end date" });
    }

	    const includeTests = queryParams.get("includeTests") === "true";
	    const partnerFilter = queryParams.get("partner") || "";
	    const instanceFilter = queryParams.get("instance") || "";
	    const integrationFilter = queryParams.get("integration") || "";
	    const fiInstancesParam =
	      queryParams.get("fiInstances") || queryParams.get("fi_instances") || "";
	    const fiInstanceSet = fiInstancesParam
	      ? new Set(
	          fiInstancesParam
	            .split(",")
	            .map((v) => normalizeFiInstanceKey(v))
	            .filter(Boolean)
	        )
	      : null;
	    const fiParam = queryParams.get("fi") || "";
	    const fiList = fiParam
	      ? fiParam
	          .split(",")
          .map((v) => normalizeFiKey(v))
          .filter(Boolean)
      : [];
    const fiSet = fiList.length ? new Set(fiList) : null;

    try {
      const [fiRegistry, instanceMeta] = await Promise.all([
        loadFiRegistrySafe(),
        loadInstanceMetaMap(),
      ]);
	      const fiMeta = buildFiMetaMap(fiRegistry);
	      const days = daysBetween(startParam, endParam);
	      const freq = new Map();
	      let daysWithSessionFiles = 0;
	      let sessionsScanned = 0;
	      let sessionsWithJobs = 0;
	      let totalJobs = 0;

      const placementMap = new Map(); // empty; integration still resolved via session.source + registry

	      for (const day of days) {
	        const sessionsRaw = await readSessionDay(day);
	        if (!sessionsRaw || sessionsRaw.error) continue;
	        daysWithSessionFiles += 1;
	        const sessions = Array.isArray(sessionsRaw.sessions) ? sessionsRaw.sessions : [];
	        for (const session of sessions) {
	          const entry = mapSessionToTroubleshootEntry(session, placementMap, fiMeta, instanceMeta);
	          sessionsScanned += 1;
	          if (!includeTests && entry.is_test) continue;
	          if (fiInstanceSet) {
	            const key = makeFiInstanceKey(entry.fi_key, entry.instance);
	            if (!fiInstanceSet.has(key)) continue;
	          }
	          if (fiSet && !fiSet.has(normalizeFiKey(entry.fi_key))) continue;
	          if (partnerFilter && partnerFilter !== "(all)" && entry.partner !== partnerFilter) continue;
	          if (integrationFilter && integrationFilter !== "(all)" && entry.integration !== normalizeIntegration(integrationFilter)) continue;
	          if (
	            instanceFilter &&
            instanceFilter !== "(all)" &&
            canonicalInstance(entry.instance) !== canonicalInstance(instanceFilter)
          ) {
            continue;
          }

          const jobs = Number(entry.total_jobs) || 0;
          if (jobs <= 0) continue;
          sessionsWithJobs += 1;
          totalJobs += jobs;
          freq.set(jobs, (freq.get(jobs) || 0) + 1);
        }
      }

      const median = medianFromFrequencyMap(freq, sessionsWithJobs);
	      return send(res, 200, {
	        startDate: startParam,
	        endDate: endParam,
	        includeTests,
	        filters: {
	          fiInstances: fiInstanceSet ? Array.from(fiInstanceSet) : null,
	          fi: fiList,
	          partner: partnerFilter || null,
	          integration: integrationFilter || null,
	          instance: instanceFilter || null,
	        },
	        daysWithSessionFiles,
	        sessionsScanned,
	        sessionsWithJobs,
	        totalJobs,
	        medianJobsPerSessionWithJobs: median,
	      });
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err?.message || "Unable to compute job stats" });
    }
  }
  if (pathname === "/instances/save" && req.method === "POST") {
    try {
      const rawBody = await readRequestBody(req);
      const payload = JSON.parse(rawBody || "{}");
      if (!payload || typeof payload !== "object") {
        return send(res, 400, { error: "Invalid payload" });
      }
      const { entry, originalName } = payload;
      if (!entry || typeof entry !== "object") {
        return send(res, 400, { error: "Missing entry" });
      }
      const normalized = normalizeInstanceEntry(entry);
      const { entries: current } = await readInstancesFile();
      const targetName = originalName || normalized.name;
      const existingIdx = current.findIndex((inst) => inst?.name === targetName);
      const conflict = current.findIndex(
        (inst, idx) => inst?.name === normalized.name && idx !== existingIdx
      );
      if (conflict >= 0) {
        return send(res, 409, { error: "An instance with that name already exists." });
      }

      if (existingIdx >= 0) {
        current[existingIdx] = normalized;
      } else {
        current.push(normalized);
      }

      const { entries: saved, path: savedPath } = await writeInstancesFile(current);
      return send(res, 200, { entry: normalized, instances: saved, path: savedPath });
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err.message || "Unable to save instance" });
    }
  }
  if (pathname === "/instances/delete" && req.method === "POST") {
    try {
      const rawBody = await readRequestBody(req);
      const payload = JSON.parse(rawBody || "{}");
      if (!payload || typeof payload !== "object" || !payload.name) {
        return send(res, 400, { error: "Missing instance name" });
      }
      const { entries: current } = await readInstancesFile();
      const idx = current.findIndex((inst) => inst?.name === payload.name);
      if (idx === -1) {
        return send(res, 404, { error: "Instance not found" });
      }
      current.splice(idx, 1);
      const { entries: saved, path: savedPath } = await writeInstancesFile(current);
      return send(res, 200, { deleted: payload.name, instances: saved, path: savedPath });
    } catch (err) {
      const status = err?.status || 500;
      return send(res, status, { error: err.message || "Unable to delete instance" });
    }
  }
  if (pathname === "/daily") {
    const dateStr = queryParams.get("date");
    if (!dateStr) {
      return send(res, 400, { error: "Missing date query param" });
    }
    try {
      const data = await loadDaily(dateStr);
      return send(res, 200, data);
    } catch (e) {
      return send(res, 404, { error: "daily not found", date: dateStr });
    }
  }
  if (pathname.startsWith("/daily/") && pathname.endsWith(".json")) {
    try {
      const dateStr = path.basename(pathname).replace(".json", "");
      return send(res, 200, await loadDaily(dateStr));
    } catch (e) {
      return send(res, 404, { error: "daily not found", path: pathname });
    }
  }

  /**
   * GET /merchant-heatmap?start=YYYY-MM-DD&end=YYYY-MM-DD
  * Returns { start, end, days: [iso...], slices: [{ day, merchant, fi, is_test, total, billable, siteFailures, userFlowIssues }] }
   */
  if (req.method === "GET" && pathname === "/merchant-heatmap") {
    const query = Object.fromEntries(queryParams.entries());
    // default = last 90 days
    const today = new Date();
    const endDefault = isoOnly(today);
    const startDefault = isoOnly(new Date(today.getTime() - 89 * 86400000));
    const start = parseIso(query.start, startDefault);
    const end = parseIso(query.end, endDefault);

    try {
      const payload = await buildGlobalMerchantHeatmap(start, end);
      res.writeHead(200, {
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
      });
      res.end(JSON.stringify(payload));
    } catch (e) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: e?.message || String(e) }));
    }
    return;
  }

  if (pathname === "/sources/summary") {
    const defaults = defaultUpdateRange();
    const start = parseIso(queryParams.get("start"), defaults.startDate);
    const end = parseIso(queryParams.get("end"), defaults.endDate);
    if (!start || !end) {
      return send(res, 400, { error: "start and end must be YYYY-MM-DD" });
    }
    if (new Date(`${start}T00:00:00Z`) > new Date(`${end}T00:00:00Z`)) {
      return send(res, 400, { error: "start date must be on or before end date" });
    }
    const rawFi = queryParams.get("fi");
    if (!rawFi) {
      return send(res, 400, { error: "fi query parameter is required" });
    }
    const fiKey = normalizeFiKey(rawFi);
    if (!fiKey) {
      return send(res, 400, { error: "Invalid fi value" });
    }
    const includeTests = queryParams.get("includeTests") === "true";
    const days = daysBetween(start, end);
    if (!days.length) {
      return send(res, 400, { error: "Invalid date range" });
    }
    const sessions = [];
    const placements = [];
    for (const day of days) {
      const daySessions = await readSessionDay(day);
      if (daySessions?.sessions) {
        sessions.push(...daySessions.sessions);
      }
      const dayPlacements = await readPlacementDay(day);
      if (dayPlacements?.placements) {
        placements.push(...dayPlacements.placements);
      }
    }
    const fiRegistry = await loadFiRegistrySafe();
    const fiMeta = buildFiMetaMap(fiRegistry);
    const instanceMeta = await loadInstanceMetaMap();
    const payload = buildTroubleshootPayload(
      `${start} → ${end}`,
      { sessions },
      { placements },
      fiMeta,
      instanceMeta
    );
    const normalizedSessions = Array.isArray(payload.sessions) ? payload.sessions : [];
    const filteredSessions = normalizedSessions.filter((session) => {
      if (!includeTests && session.is_test) return false;
      return session.fi_key === fiKey;
    });
    const grouped = groupSessionsBySource(filteredSessions);
    const kpis = computeSourceKpis(grouped);
    const daily = buildDailySeries(grouped, days);
    const merchants = buildMerchantSeries(filteredSessions);
    const fiEntry = fiMeta.get(fiKey);
    return send(res, 200, {
      start,
      end,
      fi: fiKey,
      fiName: fiEntry?.fi || fiKey,
      includeTests,
      counts: {
        sessions: filteredSessions.length,
        days: days.length,
      },
      days,
      kpis,
      daily,
      merchants,
    });
  }

  if (pathname === "/" || pathname === "/index.html") {
    const fp = path.join(PUBLIC_DIR, "index.html");
    if (await fileExists(fp)) return serveFile(res, fp);
  }

  if (pathname === "/heatmap" || pathname === "/heatmap.html") {
    const fp = path.join(PUBLIC_DIR, "heatmap.html");
    if (await fileExists(fp)) return serveFile(res, fp);
  }

  if (pathname === "/funnel" || pathname === "/funnel.html") {
    const fp = path.join(PUBLIC_DIR, "funnel.html");
    if (await fileExists(fp)) return serveFile(res, fp);
  }

  if (pathname === "/sources" || pathname === "/sources.html") {
    const fp = path.join(PUBLIC_DIR, "sources.html");
    if (await fileExists(fp)) return serveFile(res, fp);
  }

  if (pathname === "/troubleshoot" || pathname === "/troubleshoot.html") {
    const fp = path.join(PUBLIC_DIR, "troubleshoot.html");
    if (await fileExists(fp)) return serveFile(res, fp);
  }

  if (pathname === "/maintenance" || pathname === "/maintenance.html") {
    const fp = path.join(PUBLIC_DIR, "maintenance.html");
    if (await fileExists(fp)) return serveFile(res, fp);
  }

  // Serve static assets from public (CSS/JS/data/etc)
  const relPath = pathname.replace(/^\/+/, "");
  const staticCandidates = [path.join(PUBLIC_DIR, relPath)];
  if (relPath.startsWith("public/")) {
    staticCandidates.push(path.join(PUBLIC_DIR, relPath.slice("public/".length)));
  }
  for (const staticPath of staticCandidates) {
    if (staticPath.startsWith(PUBLIC_DIR) && (await fileExists(staticPath))) {
      return serveFile(res, staticPath);
    }
  }

  // UI entry (SPA fallback): "/" and any unknown path -> heatmap.html or funnel.html
  const entry = await pickUiEntry();
  if (entry) {
    return serveFile(res, entry);
  } else {
    // Inline notice if neither file exists
    const html = `<!doctype html>
<html><head><meta charset="utf-8"><title>SIS Server</title>
<style>body{background:#0b0f14;color:#e6edf3;font:16px/1.5 ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial}</style>
</head><body>
  <h1>SIS server running</h1>
  <p>Could not find <code>public/heatmap.html</code> or <code>public/funnel.html</code>.</p>
  <p>Public dir: <code>${PUBLIC_DIR}</code></p>
  <p>Visit <a href="/__diag">/__diag</a> to inspect paths.</p>
</body></html>`;
    return send(res, 200, html, "text/html; charset=utf-8");
  }
});

server.listen(PORT, () => {
  console.log(`> SIS server on http://localhost:${PORT}`);
  console.log(`> UI dir: ${PUBLIC_DIR}`);
  console.log(`> Data dir: ${DATA_DIR}`);
  console.log(`> Daily dir: ${DAILY_DIR}`);
});
