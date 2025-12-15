// src/utils/config.mjs
import fs from "fs";
import path from "path";

export function loadSsoFis(baseDir) {
  const ssoPath = path.join(baseDir, "sso_fis.json");
  const manualSso = new Set(["advancial-prod"]);
  try {
    const raw = fs.readFileSync(ssoPath, "utf8");
    const arr = JSON.parse(raw);
    if (Array.isArray(arr)) {
      console.log(`Loaded ${arr.length} SSO FIs from sso_fis.json`);
      const set = new Set(arr.map((x) => x.toLowerCase()));
      manualSso.forEach((fi) => set.add(fi));
      return set;
    }
  } catch (e) {
    console.log("No sso_fis.json found or bad JSON — treating all as NON-SSO.");
  }
  return new Set(manualSso);
}

export function loadInstances(baseDir) {
  const instPath = path.join(baseDir, "secrets", "instances.json");
  try {
    const raw = fs.readFileSync(instPath, "utf8");
    const arr = JSON.parse(raw);
    if (Array.isArray(arr) && arr.length > 0) {
      console.log(`Loaded ${arr.length} instance(s) from secrets/instances.json`);
      return arr;
    }
  } catch (e) {
    // ignore, we'll fall back
  }

  console.log("No secrets/instances.json — using .env values as single instance.");
  return [
    {
      name: "default",
      CARDSAVR_INSTANCE: process.env.CARDSAVR_INSTANCE,
      USERNAME: process.env.USERNAME,
      PASSWORD: process.env.PASSWORD,
      API_KEY: process.env.API_KEY,
      APP_NAME: process.env.APP_NAME,
    },
  ];
}
