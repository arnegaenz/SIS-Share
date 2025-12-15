(function () {
  const ALL = "All";

  function debounce(fn, wait) {
    let t;
    return function (...args) {
      clearTimeout(t);
      t = setTimeout(() => fn.apply(this, args), wait);
    };
  }

  function getFileName(path) {
    const parts = (path || "").split("/");
    const last = parts.pop() || "index.html";
    return last || "index.html";
  }

  function activateNav() {
    const here = getFileName(location.pathname);
    document.querySelectorAll("nav a[href]").forEach((a) => {
      const file = getFileName(a.getAttribute("href") || "");
      if (file === here) a.classList.add("active");
    });
  }

  function normalizeRegistryEntry(entry) {
    if (!entry) return null;
    const fi = (entry.fi_lookup_key || entry.fi_name || "").toString().trim();
    if (!fi) return null;
    const normalizePartner = (val) => {
      const s = (val || "").toString().trim();
      if (!s) return "Unknown";
      if (s.toLowerCase() === "direct" || s.toLowerCase() === "direct ss01") return "Direct ss01";
      return s;
    };
    const normalizeIntegration = (val, instanceRaw) => {
      const upperInst = (instanceRaw || "").toString().trim().toLowerCase();
      if (upperInst.includes("pscu")) return "SSO";
      if (upperInst.includes("ondot")) return "CardSavr";
      if (upperInst.includes("digitalonboarding")) return "NON-SSO";
      if (upperInst.includes("msu")) return "SSO";
      const upper = (val || "").toString().trim().toUpperCase();
      if (upper === "SSO") return "SSO";
      if (upper === "NON-SSO" || upper === "NON_SSO" || upper === "NONSSO") return "NON-SSO";
      return upper || "UNKNOWN";
    };
    return {
      fi_lookup_key: fi,
      fi_name: entry.fi_name || fi,
      partner: normalizePartner(entry.partner),
      integration: normalizeIntegration(entry.integration || entry.integration_type, entry.instance),
      instance: entry.instance || "unknown",
    };
  }

  async function loadRegistry() {
    if (Array.isArray(window.FI_Registry)) {
      console.log("[filters] using window.FI_Registry");
      return window.FI_Registry.map(normalizeRegistryEntry).filter(Boolean);
    }
    const tryFetch = async (url) => {
      try {
        console.log("[filters] fetching registry from", url);
        const res = await fetch(url);
        if (!res.ok) throw new Error(res.statusText);
        const json = await res.json();
        if (Array.isArray(json)) return json.map(normalizeRegistryEntry).filter(Boolean);
        if (json && typeof json === "object") {
          return Object.values(json).map(normalizeRegistryEntry).filter(Boolean);
        }
      } catch (err) {
        console.warn("[filters] registry load failed from", url, err);
      }
      return [];
    };

    const sources = [
      "assets/data/fi_registry.json", // relative to page
      "/assets/data/fi_registry.json", // site root
      "/public/assets/data/fi_registry.json", // fallback for some dev servers
      "fi_registry.json",
      "/fi_registry.json",
      "fi-registry",
      "/fi-registry",
    ];
    for (const url of sources) {
      const found = await tryFetch(url);
      if (found.length) return found;
    }
    console.error(
      "FI_Registry missing; add window.FI_Registry or ensure /assets/data/fi_registry.json, /fi_registry.json, or /fi-registry is reachable."
    );
    return [
      {
        fi_lookup_key: "mock_fi",
        fi_name: "Mock FI",
        partner: "MockPartner",
        integration: "SSO",
        instance: "mock",
      },
    ]; // fallback so UI renders for troubleshooting
  }

  function unique(list) {
    return Array.from(new Set(list.filter(Boolean))).sort((a, b) => a.localeCompare(b));
  }

  let allowedInstances = null;

  async function loadInstanceAllowList() {
    try {
      const res = await fetch("/instances");
      if (!res.ok) return;
      const json = await res.json();
      const names = (json.instances || [])
        .map((inst) => inst.name || inst.instance || inst.id)
        .filter(Boolean)
        .map((n) => normalizeInstanceKey(n));
      if (names.length) {
        allowedInstances = new Set(names);
        console.log("[filters] instance allow-list loaded", names.length);
      }
    } catch (err) {
      console.warn("[filters] instance allow-list load failed", err);
    }
  }

  const normalizeFiKey = (val) =>
    val ? val.toString().trim().toLowerCase() : "";
  const normalizeInstanceKey = (val) => {
    if (!val || val === ALL) return "any";
    const s = val.toString().trim().toLowerCase();
    return s || "any";
  };

  function parseQuery() {
    const p = new URLSearchParams(location.search);
    return {
      partner: p.get("partner") || ALL,
      integration: p.get("integration") || ALL,
      instance: p.get("instance") || ALL,
      fis: (p.get("fi") || "")
        .split(",")
        .map((v) => v.trim())
        .filter(Boolean),
    };
  }

  function writeQuery(state) {
    const p = new URLSearchParams(location.search);
    if (state.partner && state.partner !== ALL) p.set("partner", state.partner);
    else p.delete("partner");
    if (state.integration && state.integration !== ALL) p.set("integration", state.integration);
    else p.delete("integration");
    if (state.instance && state.instance !== ALL) p.set("instance", state.instance);
    else p.delete("instance");
    if (state.fis && state.fis.size) p.set("fi", Array.from(state.fis).join(","));
    else p.delete("fi");
    history.replaceState(null, "", `${location.pathname}?${p.toString()}`);
  }

  const readStorage = () => null;
  const writeStorage = () => {};

  function matches(meta, state) {
    if (!meta) return false;
    if (state.partner !== ALL && meta.partner !== state.partner) return false;
    if (state.integration !== ALL && meta.integration !== state.integration) return false;
    if (state.instance !== ALL && meta.instance !== state.instance) return false;
    if (state.fis.size && !state.fis.has(meta.fi)) return false;
    return true;
  }

  function deriveOptions(registry, state) {
    const byPartner = state.partner === ALL ? registry : registry.filter((r) => r.partner === state.partner);
    const byIntegration =
      state.integration === ALL ? byPartner : byPartner.filter((r) => r.integration === state.integration);
    const matchesInstance = (entry) => {
      if (state.instance === ALL) return true;
      const target = normalizeInstanceKey(state.instance);
      const list = [].concat(entry.instance || []);
      if (!list.length) return false;
      return list.map(normalizeInstanceKey).includes(target);
    };
    const byInstance = byIntegration.filter(matchesInstance);
    const currentSlice = byInstance;
    let instancesOut = unique(byInstance.map((r) => r.instance));
    if (allowedInstances && allowedInstances.size) {
      instancesOut = instancesOut.filter((inst) => allowedInstances.has(normalizeInstanceKey(inst)));
    }
    if (!instancesOut.includes("customer-dev")) instancesOut.push("customer-dev");
    const partners = unique(currentSlice.map((r) => r.partner)).filter((p) => p !== "Unknown");

    // Create FI options with instance labels: "fi_name (instance)"
    const fiOptions = byInstance.map((r) => ({
      value: r.fi_lookup_key,  // Store just the FI name
      label: `${r.fi_lookup_key} (${r.instance})`,  // Display "FI (instance)"
    }));
    // Remove duplicates based on label and sort
    const uniqueFiOptions = Array.from(
      new Map(fiOptions.map((opt) => [opt.label, opt])).values()
    ).sort((a, b) => a.label.localeCompare(b.label));

    return {
      partners,
      integrations: unique(byInstance.map((r) => r.integration)),
      fis: uniqueFiOptions,
      instances: instancesOut,
      currentSlice,
    };
  }

  function renderMultiSelect(container, values, state) {
    const btn = container.querySelector("button");
    const panel = container.querySelector(".panel");

    // Setup event handlers (only if not already set up)
    if (!btn.dataset.handlersAttached) {
      const openPanel = () => {
        panel.removeAttribute("hidden");
        container.dataset.open = "true";
      };
      const closePanel = () => {
        panel.setAttribute("hidden", "hidden");
        container.dataset.open = "false";
      };

      btn.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const open = panel.hasAttribute("hidden") ? false : true;
        if (open) closePanel();
        else openPanel();
      });

      btn.dataset.handlersAttached = "true";
    }

    panel.innerHTML = "";

    if (!values.length) {
      btn.textContent = "No FIs";
      state.fis.clear();
      return;
    }

    // Extract actual values (FI names) from option objects
    const fiValues = values.map((opt) => (typeof opt === "object" ? opt.value : opt));

    const shouldSelectAll = !state.__fiTouched;
    const nextSelected = shouldSelectAll ? new Set(fiValues) : new Set(state.fis);
    // Keep internal state in sync with the UI default of "all selected" on first load
    if (shouldSelectAll && state.fis.size === 0) {
      state.fis = new Set(fiValues);
    }

    // Toggle all row
    const toggleLabel = document.createElement("label");
    const toggleCb = document.createElement("input");
    toggleCb.type = "checkbox";
    toggleCb.value = "__toggle_all__";
    toggleCb.checked = nextSelected.size === values.length;
    toggleLabel.appendChild(toggleCb);
    toggleLabel.appendChild(document.createTextNode(" (select/deselect all)"));
    panel.appendChild(toggleLabel);

    values.forEach((opt) => {
      // Handle both old format (string) and new format (object with value/label)
      const val = typeof opt === "object" ? opt.value : opt;
      const displayText = typeof opt === "object" ? opt.label : opt;
      const id = `fi-${val}`;
      const label = document.createElement("label");
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.value = val;
      cb.id = id;
      cb.checked = nextSelected.has(val);
      label.appendChild(cb);
      label.appendChild(document.createTextNode(" " + displayText));
      panel.appendChild(label);
    });
    if (shouldSelectAll) state.fis = nextSelected;

    const updateLabel = () => {
      const count = state.fis.size;
      const total = values.length;
      const allSelected = count && count === total;
      btn.textContent = allSelected ? `All FIs (${total})` : count ? `${count} selected` : "No FIs";
    };
    updateLabel();

    panel.addEventListener("change", (ev) => {
      if (!ev.target || !ev.target.value) return;
      state.__fiTouched = true;
      if (ev.target.value === "__toggle_all__") {
        const checkAll = ev.target.checked;
        state.fis = checkAll ? new Set(fiValues) : new Set();
        panel.querySelectorAll('input[type="checkbox"]').forEach((cb) => {
          if (cb.value !== "__toggle_all__") cb.checked = checkAll;
        });
        updateLabel();
        return;
      }
      if (ev.target.checked) state.fis.add(ev.target.value);
      else state.fis.delete(ev.target.value);
      // sync toggle-all checkbox
      const allChecked = state.fis.size === values.length;
      const toggle = panel.querySelector('input[value="__toggle_all__"]');
      if (toggle) toggle.checked = allChecked;
      updateLabel();
    });
  }

  function renderFilterBar(container, state, options, applyCb) {
    container.innerHTML = `
      <div class="filter-group">
        <label for="filter-instance">Instance</label>
        <select id="filter-instance">
          <option value="${ALL}">All</option>
          ${options.instances.map((p) => `<option value="${p}">${p}</option>`).join("")}
        </select>
      </div>
      <div class="filter-group">
        <label for="filter-partner">Partner</label>
        <select id="filter-partner">
          <option value="${ALL}">All</option>
          ${options.partners.map((p) => `<option value="${p}">${p}</option>`).join("")}
        </select>
      </div>
      <div class="filter-group">
        <label for="filter-integration">Integration</label>
        <select id="filter-integration">
          <option value="${ALL}">All</option>
          ${options.integrations.map((p) => `<option value="${p}">${p}</option>`).join("")}
        </select>
      </div>
      <div class="filter-group">
        <label for="filter-fi-button">FI</label>
        <div class="multi-select" id="filter-fi">
          <button type="button" id="filter-fi-button">All FIs</button>
          <div class="panel" hidden></div>
        </div>
      </div>
      <div class="filter-group">
        <button type="button" id="filter-clear">Clear filters</button>
      </div>
    `;
    const selPartner = container.querySelector("#filter-partner");
    const selIntegration = container.querySelector("#filter-integration");
    const selInstance = container.querySelector("#filter-instance");
    const ms = container.querySelector("#filter-fi");

    selPartner.value = state.partner;
    selIntegration.value = state.integration;
    selInstance.value = state.instance;
    renderMultiSelect(ms, options.fis, state);
    if (state.disableFi) {
      selPartner.disabled = true;
      selIntegration.disabled = true;
      ms.querySelector("button").disabled = true;
      ms.title = "Filtering unavailable on this view";
      selPartner.title = selIntegration.title = ms.title;
    } else {
      selPartner.disabled = false;
      selIntegration.disabled = false;
      ms.querySelector("button").disabled = false;
      selPartner.removeAttribute("title");
      selIntegration.removeAttribute("title");
      ms.removeAttribute("title");
    }

    const refreshOptions = () => {
      const next = deriveOptions(options.registry, state);
      selPartner.innerHTML = `<option value="${ALL}">All</option>${next.partners
        .map((p) => `<option value="${p}">${p}</option>`)
        .join("")}`;
      selIntegration.innerHTML = `<option value="${ALL}">All</option>${next.integrations
        .map((p) => `<option value="${p}">${p}</option>`)
        .join("")}`;
      selInstance.innerHTML = `<option value="${ALL}">All</option>${next.instances
        .map((p) => `<option value="${p}">${p}</option>`)
        .join("")}`;
      selPartner.value = state.partner;
      selIntegration.value = state.integration;
      selInstance.value = state.instance;
      renderMultiSelect(ms, next.fis, state);
    };

    const onScopeChange = debounce(() => {
      state.partner = selPartner.value || ALL;
      state.integration = selIntegration.value || ALL;
      state.instance = selInstance.value || ALL;
      state.fis.clear();
      state.__fiTouched = false;
      writeQuery(state);
      writeStorage(state);
      refreshOptions();
      applyCb();
    }, 50);

    selPartner.addEventListener("change", () => {
      state.fis.clear();
      state.integration = ALL;
      state.instance = ALL;
      state.__fiTouched = false;
      onScopeChange();
    });
    selIntegration.addEventListener("change", () => {
      state.fis.clear();
      state.instance = ALL;
      state.__fiTouched = false;
      onScopeChange();
    });
    selInstance.addEventListener("change", onScopeChange);
    ms.querySelector(".panel").addEventListener("change", () => {
      writeQuery(state);
      writeStorage(state);
      applyCb();
    });
    container.querySelector("#filter-clear").addEventListener("click", () => {
      state.partner = ALL;
      state.integration = ALL;
      state.instance = ALL;
      state.fis.clear();
      state.__fiTouched = false;  // Reset touched flag so checkboxes re-check
      refreshOptions();
      applyCb();
      writeQuery(state);
      writeStorage(state);
    });
  }

  function filterDom(state, selector) {
    document.querySelectorAll(selector).forEach((el) => {
      const meta = {
        partner: el.dataset.partner,
        integration: el.dataset.integration,
        instance: el.dataset.instance,
        fi: el.dataset.fi,
      };
      el.style.display = matches(meta, state) ? "" : "none";
    });
  }

  async function initFilters(pageId) {
    activateNav();
    const container = document.getElementById("filter-bar");
    if (!container) return;
    container.innerHTML = `<div class="filter-group"><label>Filters</label><div class="muted">Loading filters…</div></div>`;
    console.log("[filters] initFilters start", pageId);
    await loadInstanceAllowList();
    const registry = await loadRegistry();
    console.log("[filters] registry loaded", registry.length, "page", pageId);
    const state = {
      partner: ALL,
      integration: ALL,
      instance: ALL,
      fis: new Set(),
      page: pageId,
      disableFi: false,
      __fiTouched: false,
    };
    // Always start fresh on load (same as pressing "Clear filters")
    state.partner = ALL;
    state.integration = ALL;
    state.instance = ALL;
    state.fis.clear();
    writeQuery(state);
    writeStorage(state);

    if (!registry.length) {
      container.innerHTML = `
        <div class="filter-group">
          <label>Filters</label>
          <div class="muted" style="max-width:320px">
            FI registry unavailable; using mock entries so filters still render.
          </div>
        </div>
      `;
      // fall through with mock entry
    }

    const options = deriveOptions(registry, state);
    options.registry = registry;

    const apply = () => {
      const canonicalFiInstances =
        state.fis.size > 0
          ? new Set(
              Array.from(state.fis)
                .map((fi) => normalizeFiKey(fi))
                .filter(Boolean)
                .map((fiKey) => `${fiKey}__${normalizeInstanceKey(state.instance)}`)
            )
          : null;
      state.canonicalFiInstances = canonicalFiInstances;

      window.__FILTER_STATE = { ...state, canonicalFiInstances };
      window.__FILTER_REGISTRY = registry;
      window.__FILTER_LAST_APPLIED = Date.now();
      if (pageId === "funnel" && typeof window.applyFilters === "function") {
        window.applyFilters();
        return;
      }
      if (pageId === "troubleshoot" && typeof window.applyFilters === "function") {
        window.applyFilters();
        return;
      }
      if (pageId === "heatmap" && typeof window.applyFilters === "function") {
        window.applyFilters();
        return;
      }
      if (pageId === "troubleshoot") {
        filterDom(state, ".session-card[data-fi]");
      } else {
        filterDom(state, "[data-fi]");
      }
    };

    renderFilterBar(container, state, options, apply);
    console.log("[filters] render complete", {
      partners: options.partners.length,
      integrations: options.integrations.length,
      fis: options.fis.length,
      instances: options.instances.length,
      pageId,
    });
    apply();
  }

  window.initFilters = initFilters;
  activateNav();
})();
