// Pure helpers for Source Analysis metrics.

function normalizeDevice(value) {
  if (!value) return "unknown";
  const slug = value.toString().trim().toLowerCase();
  if (!slug) return "unknown";
  if (slug.includes("mobile") || slug.includes("phone")) return "mobile";
  if (slug.includes("desk") || slug.includes("computer") || slug.includes("web")) return "desktop";
  return "unknown";
}

function getSessionDay(session) {
  const key = session.created_on || session.closed_on || session.date || "";
  if (!key) return null;
  const parsed = new Date(key);
  if (Number.isNaN(parsed)) return null;
  return parsed.toISOString().slice(0, 10);
}

function safeNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
}

function percent(success, total) {
  if (!total) return null;
  return Number(((success / total) * 100).toFixed(1));
}

function classifySourceGroup(session) {
  const sourceIntegration = session.source?.integration;
  const integrationRaw = session.integration_raw;
  if (sourceIntegration === "CU2_SSO") return "cu2sso";
  if (integrationRaw === "CU2_SSO") return "cu2sso";
  if (session.integration === "SSO") return "sso";
  return "nonSso";
}

function computeDurationMs(session) {
  if (!session.created_on || !session.closed_on) return null;
  const start = new Date(session.created_on);
  const end = new Date(session.closed_on);
  if (Number.isNaN(start) || Number.isNaN(end)) return null;
  const diff = end - start;
  return diff >= 0 ? diff : null;
}

export function groupSessionsBySource(sessions = []) {
  const buckets = {
    cu2sso: [],
    sso: [],
    nonSso: [],
  };
  for (const session of sessions) {
    const groupKey = classifySourceGroup(session);
    buckets[groupKey] = buckets[groupKey] || [];
    buckets[groupKey].push(session);
  }
  return buckets;
}

function summarizeGroup(rows) {
  const stats = {
    sessions: 0,
    sessionsWithJobs: 0,
    totalJobs: 0,
    successfulJobs: 0,
    placements: 0,
    totalDurationMs: 0,
    durationCount: 0,
    devices: {
      desktop: 0,
      mobile: 0,
      unknown: 0,
    },
  };
  for (const session of rows) {
    stats.sessions += 1;
    const totalJobs = safeNumber(session.total_jobs);
    const successfulJobs = safeNumber(session.successful_jobs);
    if (totalJobs > 0) stats.sessionsWithJobs += 1;
    stats.totalJobs += totalJobs;
    stats.successfulJobs += successfulJobs;
    const placements = Array.isArray(session.placements_raw) ? session.placements_raw.length : 0;
    stats.placements += placements;
    const duration = computeDurationMs(session);
    if (duration !== null) {
      stats.totalDurationMs += duration;
      stats.durationCount += 1;
    }
    const device = normalizeDevice(session.source?.device);
    stats.devices[device] = (stats.devices[device] || 0) + 1;
  }
  return stats;
}

function buildDeviceSplit(devices, totalSessions) {
  const categories = ["desktop", "mobile", "unknown"];
  const split = {};
  for (const cat of categories) {
    const count = devices[cat] || 0;
    split[cat] = {
      count,
      pct: totalSessions ? Number(((count / totalSessions) * 100).toFixed(1)) : null,
    };
  }
  return split;
}

export function computeSourceKpis(grouped) {
  const keys = ["cu2sso", "sso", "nonSso"];
  const details = {};
  const totalSessionsAcross = keys.reduce(
    (sum, key) => sum + (grouped[key]?.length || 0),
    0
  );
  for (const key of keys) {
    const rows = grouped[key] || [];
    const stats = summarizeGroup(rows);
    const avgDurationMs =
      stats.durationCount > 0 ? stats.totalDurationMs / stats.durationCount : null;
    details[key] = {
      sessions: stats.sessions,
      sessionsWithJobs: stats.sessionsWithJobs,
      totalJobs: stats.totalJobs,
      successfulJobs: stats.successfulJobs,
      jobSuccessRate: percent(stats.successfulJobs, stats.totalJobs),
      placements: stats.placements,
      avgDurationMs,
      deviceSplit: buildDeviceSplit(stats.devices, stats.sessions),
      sessionSharePct: totalSessionsAcross
        ? Number(((stats.sessions / totalSessionsAcross) * 100).toFixed(1))
        : null,
    };
  }
  return details;
}

export function buildDailySeries(grouped, days = []) {
  const dayMap = Object.create(null);
  for (const day of days) {
    dayMap[day] = {
      cu2sso: { sessions: 0, totalJobs: 0, successfulJobs: 0 },
      sso: { sessions: 0, totalJobs: 0, successfulJobs: 0 },
    };
  }
  for (const groupKey of ["cu2sso", "sso"]) {
    const rows = grouped[groupKey] || [];
    for (const session of rows) {
      const day = getSessionDay(session);
      if (!day || !dayMap[day]) continue;
      const bucket = dayMap[day][groupKey];
      bucket.sessions += 1;
      const totalJobs = safeNumber(session.total_jobs);
      const successfulJobs = safeNumber(session.successful_jobs);
      bucket.totalJobs += totalJobs;
      bucket.successfulJobs += successfulJobs;
    }
  }
  return days.map((day) => {
    const entry = dayMap[day];
    return {
      date: day,
      cu2sso: {
        sessions: entry?.cu2sso?.sessions || 0,
        successPct: percent(entry?.cu2sso?.successfulJobs || 0, entry?.cu2sso?.totalJobs || 0),
      },
      sso: {
        sessions: entry?.sso?.sessions || 0,
        successPct: percent(entry?.sso?.successfulJobs || 0, entry?.sso?.totalJobs || 0),
      },
    };
  });
}

export function buildMerchantSeries(sessions = []) {
  const merchantMap = new Map();
  let index = 0;
  for (const session of sessions) {
    const groupKey = classifySourceGroup(session);
    if (groupKey !== "cu2sso" && groupKey !== "sso") continue;
    const sessionId = session.id || session.agent_session_id || session.cuid || `${session.fi_key}-${index}`;
    index += 1;
    const jobs = Array.isArray(session.jobs) ? session.jobs : [];
    for (const job of jobs) {
      const merchantLabel = job.merchant || "Unknown Merchant";
      const key = merchantLabel.toLowerCase();
      if (!merchantMap.has(key)) {
        merchantMap.set(key, {
          merchant: merchantLabel,
          cu2sso: { sessions: new Set(), jobs: 0, successes: 0 },
          sso: { sessions: new Set(), jobs: 0, successes: 0 },
        });
      }
      const entry = merchantMap.get(key);
      const bucket = entry[groupKey];
      bucket.sessions.add(sessionId);
      bucket.jobs += 1;
      if (job.is_success) bucket.successes += 1;
    }
  }
  const rows = [];
  for (const entry of merchantMap.values()) {
    const cu2Jobs = entry.cu2sso.jobs;
    const ssoJobs = entry.sso.jobs;
    if (!cu2Jobs && !ssoJobs) continue;
    rows.push({
      merchant: entry.merchant,
      cu2sso: {
        sessions: entry.cu2sso.sessions.size,
        jobs: cu2Jobs,
        successPct: percent(entry.cu2sso.successes, cu2Jobs),
      },
      sso: {
        sessions: entry.sso.sessions.size,
        jobs: ssoJobs,
        successPct: percent(entry.sso.successes, ssoJobs),
      },
      totalJobs: cu2Jobs + ssoJobs,
    });
  }
  rows.sort((a, b) => b.totalJobs - a.totalJobs || a.merchant.localeCompare(b.merchant));
  return rows;
}
