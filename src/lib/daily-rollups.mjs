import fs from "node:fs/promises";
import path from "node:path";

const UNKNOWN_INSTANCE = "unknown";

function canonicalInstance(value) {
  if (!value) return UNKNOWN_INSTANCE;
  const normalized = value.toString().trim().toLowerCase().replace(/[^a-z0-9]/g, "");
  return normalized || UNKNOWN_INSTANCE;
}

function parseInstanceKey(key = "") {
  if (!key.includes("__")) {
    return { fi: key, instance: UNKNOWN_INSTANCE };
  }
  const [fi, instance] = key.split("__");
  return {
    fi,
    instance: instance || UNKNOWN_INSTANCE,
  };
}

function chooseInstanceDisplay(...values) {
  const candidates = values
    .map((value) => (value ? value.toString() : ""))
    .filter((value) => value && value !== UNKNOWN_INSTANCE);
  const preferred = candidates.find((value) => value.includes("-"));
  if (preferred) return preferred;
  return candidates[0] || UNKNOWN_INSTANCE;
}

export async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

export function bucketGaRowsByFiForDay(gaRows, day) {
  const out = {};
  for (const r of gaRows) {
    if (r.date !== day) continue;
    if (!r.fi_key) continue;
    out[r.fi_key] ??= {
      select_merchants: 0,
      user_data_collection: 0,
      credential_entry: 0,
      instances: [],
    };
    const bucket = out[r.fi_key];
    const views = r.views || 0;
    if (r.page?.startsWith("/select-merchants")) {
      bucket.select_merchants += views;
    } else if (r.page?.startsWith("/user-data-collection")) {
      bucket.user_data_collection += views;
    } else if (r.page?.startsWith("/credential-entry")) {
      bucket.credential_entry += views;
    }

    if (r.instance) {
      const inst = r.instance.toString().toLowerCase();
      if (inst && !bucket.instances.includes(inst)) {
        bucket.instances.push(inst);
      }
    }
  }
  return out;
}

export function bucketSisSessionsByFiForDay(sisSessionRows, day) {
  const out = {};
  for (const row of sisSessionRows) {
    if (row.date !== day) continue;
    if (!row.fi_lookup_key) continue;
    out[row.fi_lookup_key] = {
      total_sessions: row.total_sessions || 0,
      sessions_with_jobs: row.sessions_with_jobs || 0,
      sessions_with_success: row.sessions_with_success || 0,
    };
  }
  return out;
}

export function bucketSisPlacementsByFiForDay(sisPlacementRows, day) {
  const out = {};
  for (const row of sisPlacementRows) {
    if (row.date !== day) continue;
    if (!row.fi_lookup_key) continue;
    out[row.fi_lookup_key] ??= {
      total_placements: 0,
      successful_placements: 0,
      by_termination: {},
    };
    const c = row.count || 0;
    out[row.fi_lookup_key].total_placements += c;
    if (row.success) {
      out[row.fi_lookup_key].successful_placements += c;
    }
    const term = row.termination || "UNKNOWN";
    out[row.fi_lookup_key].by_termination[term] =
      (out[row.fi_lookup_key].by_termination[term] || 0) + c;
  }
  return out;
}

export function buildDailyDocument({
  day,
  gaByFi,
  gaByInstance = {},
  sessionsByFi,
  sessionsByInstance = {},
  placementsByFi,
  placementsByInstance = {},
}) {
  const allKeys = new Set([
    ...Object.keys(gaByFi),
    ...Object.keys(sessionsByFi),
    ...Object.keys(placementsByFi),
  ]);
  const fi = {};
  for (const key of allKeys) {
    const gRaw = gaByFi[key];
    const g = gRaw || {
      select_merchants: 0,
      user_data_collection: 0,
      credential_entry: 0,
    };
    const s = sessionsByFi[key] || {
      total_sessions: 0,
      sessions_with_jobs: 0,
      sessions_with_success: 0,
      job_distribution: {},
    };
    const p = placementsByFi[key] || {
      total_placements: 0,
      successful_placements: 0,
      by_termination: {},
    };
    const without_jobs = Math.max(
      0,
      (s.total_sessions || 0) - (s.sessions_with_jobs || 0)
    );
    const gaInstancesRaw = Array.isArray(gRaw?.instances)
      ? gRaw.instances.filter(Boolean)
      : [];
    const gaInstances = [];
    for (const inst of gaInstancesRaw) {
      const display = inst.toString();
      if (!display) continue;
      const normalized = canonicalInstance(display);
      const exists = gaInstances.some(
        (value) => canonicalInstance(value) === normalized
      );
      if (!exists) {
        gaInstances.push(display);
      }
    }

    fi[key] = {
      ga: {
        select_merchants: g.select_merchants || 0,
        user_data_collection: g.user_data_collection || 0,
        credential_entry: g.credential_entry || 0,
      },
      ga_instances: gaInstances,
      sessions: {
        total: s.total_sessions,
        with_jobs: s.sessions_with_jobs,
        with_success: s.sessions_with_success,
        without_jobs,
        total_jobs: s.total_jobs_sum || 0,
        successful_jobs: s.successful_jobs_sum || 0,
        job_distribution: s.job_distribution || {},
      },
      placements: p,
    };
  }

  const instanceKeys = new Set([
    ...Object.keys(gaByInstance),
    ...Object.keys(sessionsByInstance),
    ...Object.keys(placementsByInstance),
  ]);
  const fiInstances = {};
  for (const instanceKey of instanceKeys) {
    const gaEntry = gaByInstance[instanceKey];
    const sessionEntry = sessionsByInstance[instanceKey];
    const placementEntry = placementsByInstance[instanceKey];
    const parsed = parseInstanceKey(instanceKey);
    const fiLookupKey =
      gaEntry?.fi_lookup_key ||
      sessionEntry?.fi_lookup_key ||
      placementEntry?.fi_lookup_key ||
      parsed.fi ||
      "unknown_fi";
    const instanceValue = chooseInstanceDisplay(
      gaEntry?.instance,
      sessionEntry?.instance,
      placementEntry?.instance,
      parsed.instance
    );
    const g = gaEntry || {
      select_merchants: 0,
      user_data_collection: 0,
      credential_entry: 0,
    };
    const s = sessionEntry || {
      total_sessions: 0,
      sessions_with_jobs: 0,
      sessions_with_success: 0,
      job_distribution: {},
    };
    const p = placementEntry || {
      total_placements: 0,
      successful_placements: 0,
      by_termination: {},
    };
    const without_jobs = Math.max(
      0,
      (s.total_sessions || 0) - (s.sessions_with_jobs || 0)
    );
    const isTest =
      Boolean(gaEntry?.is_test) ||
      Boolean(sessionEntry?.is_test) ||
      Boolean(placementEntry?.is_test);

    fiInstances[instanceKey] = {
      fi_lookup_key: fiLookupKey,
      instance: instanceValue,
      is_test: isTest,
      ga: {
        select_merchants: g.select_merchants || 0,
        user_data_collection: g.user_data_collection || 0,
        credential_entry: g.credential_entry || 0,
      },
      sessions: {
        total: s.total_sessions,
        with_jobs: s.sessions_with_jobs,
        with_success: s.sessions_with_success,
        without_jobs,
        total_jobs: s.total_jobs_sum || 0,
        successful_jobs: s.successful_jobs_sum || 0,
        job_distribution: s.job_distribution || {},
      },
      placements: {
        total_placements: p.total_placements || 0,
        successful_placements: p.successful_placements || 0,
        by_termination: p.by_termination || {},
      },
    };
  }

  return {
    date: day,
    sources: {
      ga: Object.keys(gaByFi).length > 0,
      sis_sessions: Object.keys(sessionsByFi).length > 0,
      sis_placements: Object.keys(placementsByFi).length > 0,
    },
    fi,
    fi_instances: fiInstances,
  };
}

export async function writeDailyFile(baseDir, day, doc) {
  await ensureDir(baseDir);
  const filePath = path.join(baseDir, `${day}.json`);
  const tmpPath = path.join(
    baseDir,
    `.${day}.json.tmp-${process.pid}-${Date.now()}-${Math.random()
      .toString(16)
      .slice(2)}`
  );
  await fs.writeFile(tmpPath, JSON.stringify(doc, null, 2));
  await fs.rename(tmpPath, filePath);
  return filePath;
}
