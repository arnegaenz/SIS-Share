import { google } from "googleapis";

const DEFAULT_PROPERTY_ID = process.env.GA_PROPERTY_ID;
const DEFAULT_KEY_FILE =
  process.env.GA_KEYFILE || "./secrets/ga-service-account.json";

if (!DEFAULT_PROPERTY_ID) {
  console.warn("⚠️  WARNING: GA_PROPERTY_ID not set in .env file. GA data fetching may fail.");
}
const UNKNOWN_INSTANCE = "unknown";

const CARDUPDATR_SUFFIX = ".cardupdatr.app";
const CARDUPDATR_PAGES = [
  "/select-merchants",
  "/user-data-collection",
  "/credential-entry",
];

function isCardupdatrPage(pathname = "") {
  if (!pathname) return false;
  return CARDUPDATR_PAGES.some((prefix) => pathname.startsWith(prefix));
}

function normalizeDate(value, fallback) {
  if (!value) return fallback;
  if (value.includes("-")) return value;
  if (value.length === 8) {
    return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
  }
  return fallback;
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

async function getAnalyticsClient({ keyFile }) {
  const auth = new google.auth.GoogleAuth({
    keyFile,
    scopes: ["https://www.googleapis.com/auth/analytics.readonly"],
  });
  return google.analyticsdata({ version: "v1beta", auth });
}

export async function fetchGaRowsForDay({
  date,
  propertyId = DEFAULT_PROPERTY_ID,
  keyFile = DEFAULT_KEY_FILE,
}) {
  if (!date) {
    throw new Error("fetchGaRowsForDay requires a date (YYYY-MM-DD)");
  }
  if (!propertyId) {
    throw new Error("GA_PROPERTY_ID is required. Please set it in your .env file.");
  }

  const analyticsData = await getAnalyticsClient({ keyFile });
  const response = await analyticsData.properties.runReport({
    property: `properties/${propertyId}`,
    requestBody: {
      dateRanges: [{ startDate: date, endDate: date }],
      dimensions: [
        { name: "date" },
        { name: "hostName" },
        { name: "pagePath" },
        { name: "hour" },
      ],
      metrics: [{ name: "screenPageViews" }, { name: "activeUsers" }],
      limit: 10000,
    },
  });

  const rows = response.data.rows || [];
  return rows
    .map((row) => {
      const host = row.dimensionValues?.[1]?.value || "";
      const page = row.dimensionValues?.[2]?.value || "";
      if (!isCardupdatrPage(page)) return null;

      const fi = resolveFiFromHost(host);
      if (!fi) return null;

      return {
        date, // always attribute to the SIS day we asked for
        host,
        page,
        hour: row.dimensionValues?.[3]?.value || "",
        views: Number(row.metricValues?.[0]?.value || "0"),
        active_users: Number(row.metricValues?.[1]?.value || "0"),
        fi_key: fi.fi_key.toLowerCase(),
        instance: fi.instance,
      };
    })
    .filter(Boolean);
}

export async function fetchGAFunnelByDay({
  startDate,
  endDate,
  propertyId = DEFAULT_PROPERTY_ID,
  keyFile = DEFAULT_KEY_FILE,
}) {
  const analyticsData = await getAnalyticsClient({ keyFile });

  const res = await analyticsData.properties.runReport({
    property: `properties/${propertyId}`,
    requestBody: {
      dateRanges: [
        {
          startDate,
          endDate,
        },
      ],
      dimensions: [
        { name: "date" },
        { name: "hostName" },
        { name: "pagePath" },
        { name: "hour" },
      ],
      metrics: [{ name: "screenPageViews" }],
      limit: 100000,
    },
  });

  const rows = res.data.rows || [];
  const out = [];

  for (const r of rows) {
    const dateRaw = r.dimensionValues?.[0]?.value || "";
    const host = r.dimensionValues?.[1]?.value || "";
    const pagePath = r.dimensionValues?.[2]?.value || "";
    const hour = r.dimensionValues?.[3]?.value || "";
    const views = Number(r.metricValues?.[0]?.value || "0");

    if (!host || !isCardupdatrPage(pagePath)) continue;

    out.push({
      date: normalizeDate(dateRaw, startDate),
      host,
      pagePath,
      hour,
      views,
    });
  }

  return out;
}

export function aggregateGAFunnelByFI(gaRows, fiRegistry = {}) {
  const lookupDefault = new Map();
  const lookupByInstance = new Map();
  for (const [, fiObj] of Object.entries(fiRegistry)) {
    if (!fiObj || typeof fiObj !== "object") continue;
    const keySource = fiObj.fi_lookup_key || fiObj.fi_name || "";
    if (!keySource) continue;
    const normalizedKey = keySource.toString().toLowerCase();
    const instanceName = (fiObj.instance || UNKNOWN_INSTANCE)
      .toString()
      .toLowerCase();
    const integration =
      (fiObj.integration_type || "UNKNOWN").toString().toUpperCase();
    lookupByInstance.set(`${normalizedKey}__${instanceName}`, integration);
    if (!lookupDefault.has(normalizedKey)) {
      lookupDefault.set(normalizedKey, integration);
    }
  }

  function parseHost(host) {
    const resolved = resolveFiFromHost(host);
    if (!resolved) {
      return {
        fi_lookup_key: host,
        instance: "unknown",
      };
    }
    return {
      fi_lookup_key: resolved.fi_key,
      instance: resolved.instance,
    };
  }

  const byFI = {};

  for (const row of gaRows) {
    const { date, host, pagePath, views } = row;
    const parsed = parseHost(host);
    if (!parsed || !parsed.fi_lookup_key) continue;
    const fi_lookup_key = parsed.fi_lookup_key.toString().toLowerCase();
    const instance = (parsed.instance || "").toString().toLowerCase();

    if (!byFI[fi_lookup_key]) {
      const integrationKey = lookupByInstance.get(
        `${fi_lookup_key}__${(instance || UNKNOWN_INSTANCE)}`
      );
      const fallback = lookupDefault.get(fi_lookup_key);
      byFI[fi_lookup_key] = {
        fi_lookup_key,
        instance,
        select: 0,
        user: 0,
        cred: 0,
        daily: {},
        integration_type: integrationKey || fallback || "UNKNOWN",
      };
    }

    const bucket = byFI[fi_lookup_key];
    if (!bucket.daily[date]) {
      bucket.daily[date] = { select: 0, user: 0, cred: 0 };
    }

    if (pagePath.startsWith("/select-merchants")) {
      bucket.select += views;
      bucket.daily[date].select += views;
    } else if (pagePath.startsWith("/user-data-collection")) {
      bucket.user += views;
      bucket.daily[date].user += views;
    } else if (pagePath.startsWith("/credential-entry")) {
      bucket.cred += views;
      bucket.daily[date].cred += views;
    }
  }

  return byFI;
}

export function printGAFunnelReport(grouped) {
  function printSection(title, rows) {
    console.log("");
    console.log(title);
    console.log("FI                         | sel  usr  cred | sess  place | sel→cred");
    console.log("---------------------------+----------------+-------------+---------");
    for (const r of rows) {
      const name = (r.fi_lookup_key || "").padEnd(27, " ");
      const sel = String(r.ga_select || 0).padStart(4, " ");
      const usr = String(r.ga_user || 0).padStart(4, " ");
      const crd = String(r.ga_cred || 0).padStart(4, " ");
      const sess = String(r.sis_sessions || 0).padStart(5, " ");
      const plc = String(r.sis_placements || 0).padStart(6, " ");

      const selToCred =
        r.ga_select > 0
          ? ((r.ga_cred || 0) / r.ga_select * 100).toFixed(1) + "%"
          : "   —  ";

      console.log(
        `${name} | ${sel} ${usr} ${crd} | ${sess} ${plc} | ${selToCred.padStart(
          7,
          " "
        )}`
      );
    }
  }

  if (grouped.SSO && grouped.SSO.length) {
    printSection("GA + SIS CardUpdatr funnel (SSO)", grouped.SSO);
  }
  if (grouped["NON-SSO"] && grouped["NON-SSO"].length) {
    printSection("GA + SIS CardUpdatr funnel (NON-SSO)", grouped["NON-SSO"]);
  }
  if (grouped.CardSavr && grouped.CardSavr.length) {
    printSection("GA + SIS CardUpdatr funnel (CardSavr)", grouped.CardSavr);
  }
}
