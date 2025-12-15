import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";

import {
  bucketGaRowsByFiForDay,
  buildDailyDocument,
  writeDailyFile,
} from "../src/lib/daily-rollups.mjs";
import { isTestInstanceName } from "../src/config/testInstances.mjs";
import { readRaw } from "../src/lib/rawStorage.mjs";
import { parseDateArgs } from "./fetch-raw.mjs";

const DAILY_OUTPUT_DIR = path.resolve("data/daily");
const FI_REGISTRY_PATH = path.resolve("fi_registry.json");
const CARDUPDATR_SUFFIX = ".cardupdatr.app";
const UNKNOWN_INSTANCE = "unknown";
const INSTANCE_DISPLAY_OVERRIDES = new Map([
  ["digitalonboarding", "digital-onboarding"],
]);

function normalizeFiKey(value) {
  if (!value) return "";
  return value.toString().trim().toLowerCase();
}

function formatInstanceDisplay(value) {
  if (!value) return UNKNOWN_INSTANCE;
  const base = value
    .toString()
    .trim()
    .toLowerCase()
    .replace(/[\s_]+/g, "-");
  const display = base || UNKNOWN_INSTANCE;
  return INSTANCE_DISPLAY_OVERRIDES.get(display) || display;
}

function canonicalInstance(value) {
  if (!value) return UNKNOWN_INSTANCE;
  const normalized = value.toString().trim().toLowerCase().replace(/[^a-z0-9]/g, "");
  return normalized || UNKNOWN_INSTANCE;
}

function adjustInstanceForFi(fiLookupKey, instanceValue) {
  const fiNorm = normalizeFiKey(fiLookupKey);
  const instanceNorm = canonicalInstance(instanceValue);
  if (fiNorm === "advancial-prod" && instanceNorm === "default") {
    return "advancial-prod";
  }
  return instanceValue;
}

function adjustFiLookupForInstance(fiLookupKey, instanceValue) {
  const fiNorm = normalizeFiKey(fiLookupKey);
  const instanceNorm = canonicalInstance(instanceValue);
  if (fiNorm === "default" && instanceNorm === "advancialprod") {
    return "advancial-prod";
  }
  return fiLookupKey;
}

function makeFiInstanceKey(fiKey, instance) {
  const fi = (fiKey || "").toString().toLowerCase();
  const inst = canonicalInstance(instance);
  return `${fi}__${inst}`;
}

function ensureInstanceDisplay(list = [], display) {
  if (!display) return list;
  const normalized = canonicalInstance(display);
  const exists = list.some((value) => canonicalInstance(value) === normalized);
  if (!exists) {
    list.push(display);
    list.sort((a, b) => a.localeCompare(b));
  }
  return list;
}

function readFiRegistry() {
  try {
    const raw = fs.readFileSync(FI_REGISTRY_PATH, "utf8");
    return JSON.parse(raw);
  } catch {
    console.warn("⚠️ fi_registry.json not found — continuing without registry metadata.");
    return {};
  }
}

function buildRegistryIndex(fiRegistry = {}) {
  const byLookup = new Map();
  const byName = new Map();

  for (const entry of Object.values(fiRegistry)) {
    if (!entry || typeof entry !== "object") continue;
    const lookup = (entry.fi_lookup_key || entry.fi_name || "")
      .toString()
      .toLowerCase();
    const name = (entry.fi_name || "")
      .toString()
      .toLowerCase();
    if (lookup) {
      byLookup.set(lookup, lookup);
    }
    if (name && lookup) {
      byName.set(name, lookup);
    }
  }

  return { byLookup, byName };
}

function resolveFiKey(preferred, fallbackName, registryIndex) {
  const normalizedPreferred = preferred
    ? preferred.toString().toLowerCase()
    : null;
  if (normalizedPreferred) {
    if (registryIndex.byLookup.has(normalizedPreferred)) {
      return registryIndex.byLookup.get(normalizedPreferred);
    }
    return normalizedPreferred;
  }

  const normalizedName = fallbackName
    ? fallbackName.toString().toLowerCase()
    : null;
  if (normalizedName && registryIndex.byName.has(normalizedName)) {
    return registryIndex.byName.get(normalizedName);
  }
  return normalizedName || null;
}

function resolveFiFromHost(host = "") {
  if (!host.endsWith(CARDUPDATR_SUFFIX)) return null;
  const prefix = host.slice(0, -CARDUPDATR_SUFFIX.length);
  if (!prefix) return null;
  const parts = prefix.split(".");
  if (parts.length === 1) {
    return {
      fi_key: parts[0],
      instance: parts[0],
    };
  }
  const fi_key = parts[0];
  const instance = parts[1] || parts[0];
  if (fi_key === "default" && instance === "advancial-prod") {
    return {
      fi_key: "advancial-prod",
      instance,
    };
  }
  return {
    fi_key,
    instance,
  };
}

function aggregateGaFromRaw(day, raw, registryIndex) {
  if (!raw || raw.error) {
    if (raw?.error) {
      console.warn(`[${day}] GA raw flagged error: ${raw.error}`);
    }
    return { byFi: {}, byInstance: {} };
  }

  const rows = Array.isArray(raw.rows) ? raw.rows : [];
  const byInstance = {};

  for (const originalRow of rows) {
    if (!originalRow || typeof originalRow !== "object") continue;
    const host = originalRow.host || originalRow.hostname || "";
    const parsedHost = resolveFiFromHost(host);
    const preferredKey =
      originalRow.fi_key ||
      originalRow.fi_lookup_key ||
      parsedHost?.fi_key ||
      null;
    const fallbackName = originalRow.fi_name || parsedHost?.fi_key || null;
    const fiKey = resolveFiKey(preferredKey, fallbackName, registryIndex);
    if (!fiKey) continue;
    const instanceRaw =
      originalRow.instance ||
      originalRow.host_instance ||
      parsedHost?.instance ||
      UNKNOWN_INSTANCE;
    const adjustedInstance = adjustInstanceForFi(fiKey, instanceRaw);
    const instanceDisplay = formatInstanceDisplay(adjustedInstance);
    const normalizedInstance = canonicalInstance(instanceDisplay);
    const isTest = isTestInstanceName(normalizedInstance);
    const fiInstanceKey = makeFiInstanceKey(fiKey, normalizedInstance);

    if (!byInstance[fiInstanceKey]) {
      byInstance[fiInstanceKey] = {
        fi_lookup_key: fiKey,
        instance: instanceDisplay,
        instance_norm: normalizedInstance,
        is_test: isTest,
        select_merchants: 0,
        user_data_collection: 0,
        credential_entry: 0,
      };
    }
    const bucket = byInstance[fiInstanceKey];
    if (isTest) {
      bucket.is_test = true;
    }
    const count = Number(
      originalRow.active_users ??
        originalRow.activeUsers ??
        originalRow.views ??
        originalRow.screenPageViews ??
        0
    );
    const pagePath =
      (originalRow.page || originalRow.pagePath || originalRow.pathname || "")
        .toString();
    const select = pagePath.startsWith("/select-merchants");
    const user = pagePath.startsWith("/user-data-collection");
    const cred = pagePath.startsWith("/credential-entry");
    if (select) bucket.select_merchants += count;
    else if (user) bucket.user_data_collection += count;
    else if (cred) bucket.credential_entry += count;
  }

  const byFi = {};
  for (const entry of Object.values(byInstance)) {
    const fiKey = entry.fi_lookup_key;
    if (!byFi[fiKey]) {
      byFi[fiKey] = {
        select_merchants: 0,
        user_data_collection: 0,
        credential_entry: 0,
        instances: [],
      };
    }
    const fiBucket = byFi[fiKey];
    fiBucket.select_merchants += entry.select_merchants;
    fiBucket.user_data_collection += entry.user_data_collection;
    fiBucket.credential_entry += entry.credential_entry;
    fiBucket.instances = ensureInstanceDisplay(fiBucket.instances, entry.instance);
  }

  return { byFi, byInstance };
}

function safeSessionFiKey(session, registryIndex) {
  const lookup =
    session.financial_institution_lookup_key ??
    session.fi_lookup_key ??
    null;
  const name =
    session.fi_name ||
    session.financial_institution ||
    session.financial_institution_name ||
    session.institution ||
    null;
  return (
    resolveFiKey(lookup, name, registryIndex) ||
    "unknown_fi"
  );
}

function aggregateSessionsFromRaw(raw, registryIndex) {
  if (!raw || raw.error) {
    if (raw?.error) {
      console.warn(`[${raw.date || "unknown"}] Sessions raw flagged error: ${raw.error}`);
    }
    return { byFi: {}, byInstance: {} };
  }

  const sessions = Array.isArray(raw.sessions) ? raw.sessions : [];
  const byInstance = {};

  for (const session of sessions) {
    if (!session || typeof session !== "object") continue;
    const fiKey = safeSessionFiKey(session, registryIndex);
    let instanceValue =
      session._instance ||
      session.instance ||
      session.instance_name ||
      session.org_name ||
      session.instance_slug ||
      UNKNOWN_INSTANCE;
    instanceValue = adjustInstanceForFi(fiKey, instanceValue);
    const adjustedFiKey = adjustFiLookupForInstance(fiKey, instanceValue);
    const fiLookup = (adjustedFiKey || fiKey || "unknown_fi").toString();
    const normalizedFi = normalizeFiKey(fiLookup);
    const instanceDisplay = formatInstanceDisplay(instanceValue);
    const normalizedInstance = canonicalInstance(instanceDisplay);
    const isTest = isTestInstanceName(normalizedInstance);
    const key = makeFiInstanceKey(normalizedFi, normalizedInstance);
    const totalJobs = Number(session.total_jobs) || 0;
    const successfulJobs = Number(session.successful_jobs) || 0;

    if (!byInstance[key]) {
      byInstance[key] = {
        fi_lookup_key: fiLookup,
        fi_lookup_norm: normalizedFi,
        instance: instanceDisplay,
        instance_norm: normalizedInstance,
        is_test: isTest,
        total_sessions: 0,
        sessions_with_jobs: 0,
        sessions_with_success: 0,
        total_jobs_sum: 0,
        successful_jobs_sum: 0,
      };
    }
    const bucket = byInstance[key];
    if (isTest) {
      bucket.is_test = true;
    }
    bucket.total_sessions += 1;
    if (totalJobs > 0) bucket.sessions_with_jobs += 1;
    if (successfulJobs > 0) bucket.sessions_with_success += 1;
    bucket.total_jobs_sum += totalJobs;
    bucket.successful_jobs_sum += successfulJobs;
  }

  const byFi = {};
  for (const entry of Object.values(byInstance)) {
    const fiKey = entry.fi_lookup_norm || entry.fi_lookup_key?.toString().toLowerCase();
    if (!fiKey) continue;
    if (!byFi[fiKey]) {
      byFi[fiKey] = {
        total_sessions: 0,
        sessions_with_jobs: 0,
        sessions_with_success: 0,
        total_jobs_sum: 0,
        successful_jobs_sum: 0,
      };
    }
    const fiBucket = byFi[fiKey];
    fiBucket.total_sessions += entry.total_sessions;
    fiBucket.sessions_with_jobs += entry.sessions_with_jobs;
    fiBucket.sessions_with_success += entry.sessions_with_success;
    fiBucket.total_jobs_sum += entry.total_jobs_sum || 0;
    fiBucket.successful_jobs_sum += entry.successful_jobs_sum || 0;
  }

  return { byFi, byInstance };
}

function safePlacementFiKey(placement, registryIndex) {
  const lookup =
    placement.fi_lookup_key ||
    placement.financial_institution_lookup_key ||
    null;
  const name =
    placement.fi_name ||
    placement.financial_institution ||
    placement.issuer_name ||
    null;
  return (
    resolveFiKey(lookup, name, registryIndex) ||
    "unknown_fi"
  );
}

function isSuccessfulPlacement(placement) {
  const status = (placement.status || "").toString().toUpperCase();
  const termination = (placement.termination_type || "")
    .toString()
    .toUpperCase();
  return status === "SUCCESSFUL" || termination === "BILLABLE";
}

function aggregatePlacementsFromRaw(raw, registryIndex) {
  if (!raw || raw.error) {
    if (raw?.error) {
      console.warn(
        `[${raw?.date || "unknown"}] Placements raw flagged error: ${raw.error}`
      );
    }
    return { byFi: {}, byInstance: {} };
  }

  const placements = Array.isArray(raw.placements) ? raw.placements : [];
  const byInstance = {};

  for (const placement of placements) {
    if (!placement || typeof placement !== "object") continue;
    const baseFiKey = safePlacementFiKey(placement, registryIndex).toString();
    let instanceValue =
      placement._instance ||
      placement.instance ||
      placement.instance_name ||
      placement.org_name ||
      UNKNOWN_INSTANCE;
    instanceValue = adjustInstanceForFi(baseFiKey, instanceValue);
    const adjustedFiKey = adjustFiLookupForInstance(baseFiKey, instanceValue);
    const fiKey = adjustedFiKey || baseFiKey;
    const normalizedFi = normalizeFiKey(fiKey);
    const instanceDisplay = formatInstanceDisplay(instanceValue);
    const normalizedInstance = canonicalInstance(instanceDisplay);
    const isTest = isTestInstanceName(normalizedInstance);
    const key = makeFiInstanceKey(normalizedFi, normalizedInstance);
    const termination = (
      placement.termination_type ||
      placement.termination ||
      placement.status ||
      "UNKNOWN"
    )
      .toString()
      .toUpperCase();

    if (!byInstance[key]) {
      byInstance[key] = {
        fi_lookup_key: fiKey,
        fi_lookup_norm: normalizedFi,
        instance: instanceDisplay,
        instance_norm: normalizedInstance,
        is_test: isTest,
        total_placements: 0,
        successful_placements: 0,
        by_termination: {},
      };
    }
    const bucket = byInstance[key];
    if (isTest) {
      bucket.is_test = true;
    }
    bucket.total_placements += 1;
    if (isSuccessfulPlacement(placement)) {
      bucket.successful_placements += 1;
    }
    bucket.by_termination[termination] =
      (bucket.by_termination[termination] || 0) + 1;
  }

  const byFi = {};
  for (const entry of Object.values(byInstance)) {
    const fiKey = entry.fi_lookup_norm || entry.fi_lookup_key?.toLowerCase();
    if (!fiKey) continue;
    if (!byFi[fiKey]) {
      byFi[fiKey] = {
        total_placements: 0,
        successful_placements: 0,
        by_termination: {},
      };
    }
    const fiBucket = byFi[fiKey];
    fiBucket.total_placements += entry.total_placements;
    fiBucket.successful_placements += entry.successful_placements;
    for (const [term, count] of Object.entries(entry.by_termination)) {
      fiBucket.by_termination[term] =
        (fiBucket.by_termination[term] || 0) + count;
    }
  }

  return { byFi, byInstance };
}

function enumerateRange(startDate, endDate) {
  if (startDate === endDate) return [startDate];
  const { dates } = parseDateArgs([startDate, endDate]);
  return dates;
}

function allSourcesMissing(...rawValues) {
  return rawValues.every((value) => !value);
}

export async function buildDailyFromRawRange({ startDate, endDate }) {
  const registry = readFiRegistry();
  const registryIndex = buildRegistryIndex(registry);
  const dates = enumerateRange(startDate, endDate);

  for (const day of dates) {
    const gaRaw = readRaw("ga", day);
    const sessionsRaw = readRaw("sessions", day);
    const placementsRaw = readRaw("placements", day);

    if (allSourcesMissing(gaRaw, sessionsRaw, placementsRaw)) {
      console.warn(`[${day}] No raw files found; skipping daily build.`);
      continue;
    }

    const { byFi: gaByFi, byInstance: gaByInstance } = aggregateGaFromRaw(
      day,
      gaRaw,
      registryIndex
    );
    const { byFi: sessionsByFi, byInstance: sessionsByInstance } =
      aggregateSessionsFromRaw(sessionsRaw, registryIndex);
    const { byFi: placementsByFi, byInstance: placementsByInstance } =
      aggregatePlacementsFromRaw(placementsRaw, registryIndex);

    const doc = buildDailyDocument({
      day,
      gaByFi,
      gaByInstance,
      sessionsByFi,
      sessionsByInstance,
      placementsByFi,
      placementsByInstance,
    });

    await writeDailyFile(DAILY_OUTPUT_DIR, day, doc);
    console.log(`[${day}] wrote ${path.join("data/daily", `${day}.json`)}`);
  }
}

const isDirectRun =
  process.argv[1] &&
  pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url;

if (isDirectRun) {
  try {
    const { startDate, endDate } = parseDateArgs(process.argv.slice(2));
    buildDailyFromRawRange({ startDate, endDate }).catch((err) => {
      console.error("build-daily-from-raw failed:", err);
      process.exitCode = 1;
    });
  } catch (err) {
    console.error(err.message || err);
    process.exitCode = 1;
  }
}
