import { getAttemptsDistribution } from "./data-cache.js?v=3";

// -- SIS Phase 3: Funnel facade scaffold --
import { getFunnelData } from "./funnel.data.js";
import { renderFunnelView } from "./funnel.view.js";
// ----------------------------

(function () {
  const sisBindOnce =
    window && typeof window.sisBindOnce === "function"
      ? window.sisBindOnce
      : function (el, type, handler, opts) {
          if (!el) return;
          const key = "sisBound_" + type;
          const ds = el.dataset || (el.dataset = {});
          if (!ds[key]) {
            el.addEventListener(type, handler, opts);
            ds[key] = "1";
          }
        };

  function parseIntLoose(s) {
    const v = String(s || "")
      .replace(/[,\s]/g, "")
      .replace(/[^\d-]/g, "");
    const n = Number.parseInt(v, 10);
    return Number.isFinite(n) ? n : 0;
  }

  function formatJobs(v) {
    if (typeof v !== "number" || !Number.isFinite(v)) return "—";
    return v.toFixed(2);
  }

  function quantileSorted(arr, start, n, p) {
    if (!arr || !n || n <= 0) return null;
    const idx = (n - 1) * p;
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    const a = +arr[start + lo];
    const b = +arr[start + hi];
    if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
    if (hi === lo) return a;
    return a + (b - a) * (idx - lo);
  }

  function ensureAttemptsCard() {
    const card =
      document.querySelector('[data-sis-card="attempts"]') ||
      document.querySelector(".funnel-dist-card");
    if (!card) return null;

    card.setAttribute("data-sis-card", "attempts");
    card.classList.add("sis-attempts-card");
    card.style.position = "relative";

    return card;
  }

  function updateAttemptsStatsAndDistribution() {
    const card = ensureAttemptsCard();
    if (!card) return;

    const values = card.querySelectorAll(".funnel-dist-value[data-key]");
    const panel = card.querySelector("#distribution-wrap");
    const tbody = panel ? panel.querySelector("tbody") : null;
    const empty = panel ? panel.querySelector(".funnel-dist-empty") : null;

    const jobCounts =
      window.SIS &&
      window.SIS.funnel &&
      Array.isArray(window.SIS.funnel.sessionJobCounts)
        ? window.SIS.funnel.sessionJobCounts
        : null;

    if (!jobCounts || !jobCounts.length) {
      for (let i = 0; i < values.length; i++) values[i].textContent = "—";
      if (tbody) tbody.innerHTML = "";
      if (empty) empty.hidden = false;
      window.sisAttemptsDistribution = [];
      return;
    }

    let start = 0;
    const noJobsEl = document.getElementById("convMetricNoJobs");
    if (noJobsEl) start = parseIntLoose(noJobsEl.textContent);
    if (start < 0) start = 0;
    if (start > jobCounts.length) start = jobCounts.length;
    while (start < jobCounts.length && (+jobCounts[start] || 0) <= 0) start++;

    const n = jobCounts.length - start;
    if (n <= 0) {
      for (let i = 0; i < values.length; i++) values[i].textContent = "—";
      if (tbody) tbody.innerHTML = "";
      if (empty) empty.hidden = false;
      window.sisAttemptsDistribution = [];
      return;
    }

    let sum = 0;
    for (let i = start; i < jobCounts.length; i++) sum += +jobCounts[i] || 0;
    const avg = sum / n;
    const median = quantileSorted(jobCounts, start, n, 0.5);
    const p75 = quantileSorted(jobCounts, start, n, 0.75);

    for (let i = 0; i < values.length; i++) {
      const key = values[i].getAttribute("data-key") || "";
      if (key === "median") values[i].textContent = formatJobs(median);
      else if (key === "avg") values[i].textContent = formatJobs(avg);
      else if (key === "p75") values[i].textContent = formatJobs(p75);
    }

    const distRows = [];
    let cur = +jobCounts[start] || 0;
    let curCount = 0;
    for (let i = start; i < jobCounts.length; i++) {
      const v = +jobCounts[i] || 0;
      if (v === cur) curCount++;
      else {
        if (cur > 0 && curCount > 0) distRows.push({ jobsPerSession: cur, sessions: curCount });
        cur = v;
        curCount = 1;
      }
    }
    if (cur > 0 && curCount > 0) distRows.push({ jobsPerSession: cur, sessions: curCount });

    window.sisAttemptsDistribution = distRows;

    if (tbody) {
      tbody.innerHTML = distRows
        .map(function (r) {
          const jobs = Number(r.jobsPerSession || 0);
          const sessions = Number(r.sessions || 0);
          const label = String(jobs) + " job" + (jobs === 1 ? "" : "s");
          return (
            "<tr><td>" +
            label +
            "</td><td class=\"t-right\">" +
            sessions.toLocaleString() +
            "</td></tr>"
          );
        })
        .join("");
    }
    if (empty) empty.hidden = distRows.length > 0;
  }

  function buildPopoverContent(popEl) {
    if (!popEl) return;

    let distRows = [];
    try {
      distRows = (window.getAttemptsDistribution ? window.getAttemptsDistribution() : (typeof getAttemptsDistribution==='function' ? getAttemptsDistribution() : [])) || [];
    } catch (e) {
      if (window.sisWarn) window.sisWarn('Render failed: funnel', e);
      distRows = [];
    }

    popEl.innerHTML =
      '<div class="sis-popover-card">' +
      '<div class="sis-popover-header">' +
      '<div class="sis-popover-title">Jobs per session</div>' +
      '<button class="sis-popover-close" aria-label="Close" type="button">\\u2715</button>' +
      "</div>" +
      '<div class="sis-popover-body">' +
      '<table class="sis-table compact">' +
      "<thead><tr><th>Jobs per session</th><th class=\"t-right\">Sessions</th></tr></thead>" +
      "<tbody>" +
      distRows
        .map(function (r) {
          const jobs = Number(r.jobsPerSession || 0);
          const sessions = Number(r.sessions || 0);
          const label = String(jobs) + " job" + (jobs === 1 ? "" : "s");
          return (
            "<tr><td>" +
            label +
            "</td><td class=\"t-right\">" +
            sessions.toLocaleString() +
            "</td></tr>"
          );
        })
        .join("") +
      "</tbody></table></div></div>";
  }

  function legacyRenderFunnel(container, filters) {
    initAttemptsDistributionUI();
  }

  function splitRenderFunnel(container, filters) {
    try {
      const model = getFunnelData(filters, {
        // later steps will inject real summary + daily rows here
        summary: {},
      });
      renderFunnelView(container, model);
    } catch(e){
      if (window.sisWarn) window.sisWarn('Split render failed', e);
    }
  }

  function sisWarn(msg, ctx){
    try { if (window.sisWarn) window.sisWarn(msg, ctx); } catch(_){}
  }

  function getFunnelFacadeContainer() {
    return (
      document.querySelector(".sis-content") ||
      document.querySelector("main") ||
      document.body
    );
  }

  function initAttemptsDistributionUI() {
    try{
    const card = ensureAttemptsCard();
    if (!card) return;

    let btn = card.querySelector('[data-sis-action="distribution"]');
    if (!btn) {
      btn = card.querySelector("#toggle-distribution") || card.querySelector("#attempts-popover-toggle");
      if (btn) btn.setAttribute("data-sis-action", "distribution");
      btn = card.querySelector('[data-sis-action="distribution"]');
    }

    const panel = card.querySelector("#distribution-wrap");
    let pop = panel ? panel : card.querySelector('[data-sis-popover="distribution"]');
    if (!pop) pop = card.querySelector(".sis-popover");
    if (!pop && !panel) {
      pop = document.createElement("div");
      pop.className = "sis-popover";
      pop.setAttribute("role", "dialog");
      pop.setAttribute("aria-modal", "false");
      pop.setAttribute("aria-hidden", "true");
      card.appendChild(pop);
    }
    if (pop && !panel) pop.setAttribute("data-sis-popover", "distribution");
    if (panel) panel.setAttribute("data-sis-popover", "distribution");

    if (!btn || !pop) return;
    if (!pop.getAttribute("data-open")) pop.setAttribute("data-open", "0");
    updateAttemptsStatsAndDistribution();

    function setCaret(open) {
      const caret = btn.querySelector(".sis-chip-caret");
      if (caret) caret.textContent = open ? "\u25B4" : "\u25BE";
    }

    function setOpen(isOpen) {
      pop.setAttribute("data-open", isOpen ? "1" : "0");
      if (panel) {
        if (isOpen) {
          try {
            const top = (btn.offsetTop + btn.offsetHeight + 6) || 44;
            card.style.setProperty("--sis-dist-top", top + "px");
          } catch (_) {}
        }
        panel.hidden = !isOpen;
      }
      if (!panel) pop.setAttribute("aria-hidden", isOpen ? "false" : "true");
      card.classList.toggle("popover-open", Boolean(isOpen) && !panel);
      btn.setAttribute("aria-expanded", isOpen ? "true" : "false");
      setCaret(Boolean(isOpen));
      if (isOpen) updateAttemptsStatsAndDistribution();
      if (!panel && isOpen && (!pop.innerHTML || !pop.innerHTML.trim())) buildPopoverContent(pop);
      if (!isOpen) {
        const closeEl = pop.querySelector(".sis-popover-close");
        if (closeEl) closeEl.blur && closeEl.blur();
      }
    }

    // Toggle open/close on click
    sisBindOnce(btn, 'click', (e)=>{
      e.preventDefault();
      e.stopPropagation();
      const open = pop.getAttribute('data-open') === '1';
      setOpen(!open);
      if (!open) {
        try { pop.focus(); } catch(_){}
        const closeBtn2 = pop.querySelector(".sis-popover-close");
        if (closeBtn2) {
          sisBindOnce(closeBtn2, "click", (e2) => {
            e2.preventDefault();
            e2.stopPropagation();
            setOpen(false);
          });
        }
      }
    });

    const closeBtn = pop.querySelector(".sis-popover-close");
    if (closeBtn) {
      sisBindOnce(closeBtn, "click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        setOpen(false);
      });
    }

    // Close when clicking outside
    sisBindOnce(document, 'click', (e)=>{
      const c = document.querySelector('[data-sis-card="attempts"]');
      const p = c ? c.querySelector('[data-sis-popover="distribution"]') : null;
      if (!c || !p) return;
      if (p.getAttribute('data-open') !== '1') return;
      if (!c.contains(e.target)) {
        p.setAttribute('data-open','0');
        const panel2 = c.querySelector("#distribution-wrap");
        if (panel2) panel2.hidden = true;
        else p.setAttribute("aria-hidden", "true");
        c.classList.remove("popover-open");
        const b = c.querySelector('[data-sis-action="distribution"]');
        if (b) {
          b.setAttribute("aria-expanded", "false");
          const caret = b.querySelector(".sis-chip-caret");
          if (caret) caret.textContent = "\u25BE";
        }
      }
    });

    // Close on Escape
    sisBindOnce(document, 'keydown', (e)=>{
      const c = document.querySelector('[data-sis-card="attempts"]');
      const p = c ? c.querySelector('[data-sis-popover="distribution"]') : null;
      if (!c || !p) return;
      if (e.key === 'Escape' && p.getAttribute('data-open') === '1'){
        p.setAttribute('data-open','0');
        const panel2 = c.querySelector("#distribution-wrap");
        if (panel2) panel2.hidden = true;
        else p.setAttribute("aria-hidden", "true");
        c.classList.remove("popover-open");
        const b = c.querySelector('[data-sis-action="distribution"]');
        if (b) {
          b.setAttribute("aria-expanded", "false");
          const caret = b.querySelector(".sis-chip-caret");
          if (caret) caret.textContent = "\u25BE";
        }
      }
    });
    } catch(e){ (window.sisWarn||console.warn)('initAttemptsDistributionUI failed', e); }
  }

  function hookRenders() {
    const original = window.renderConversionAnalysis;
    if (typeof original === "function" && !original.__sisFunnelRenderedWrapped) {
      const wrapped = function () {
        const res = original.apply(this, arguments);
        try {
          const container = getFunnelFacadeContainer();
          const filters = {};
          splitRenderFunnel(container, filters);
          if (window.sisRenderBus && typeof window.sisRenderBus.emit === "function") {
            window.sisRenderBus.emit("funnel:rendered");
          }
        } catch (e) { if (window.sisWarn) window.sisWarn('Render failed: funnel', e); }
        return res;
      };
      wrapped.__sisFunnelRenderedWrapped = true;
      window.renderConversionAnalysis = wrapped;
    }
  }

  try {
    hookRenders();
  } catch (e) { if (window.sisWarn) window.sisWarn('Render failed: funnel', e); }

  if (!window.__sisAttemptsDistributionSubscribed) {
    window.__sisAttemptsDistributionSubscribed = true;
    if (window.sisRenderBus && typeof window.sisRenderBus.on === 'function'){
      window.sisRenderBus.on('funnel:rendered', initAttemptsDistributionUI);
    }
    document.addEventListener('DOMContentLoaded', initAttemptsDistributionUI);
  }

  if (document.readyState === "loading") {
    if (!window.__sisAttemptsDistributionDOMContentLoadedBound) {
      window.__sisAttemptsDistributionDOMContentLoadedBound = true;
      document.addEventListener("DOMContentLoaded", function () {
        try {
          const container = getFunnelFacadeContainer();
          const filters = {};
          splitRenderFunnel(container, filters);
          if (window.sisRenderBus && typeof window.sisRenderBus.emit === "function") {
            window.sisRenderBus.emit("funnel:rendered");
          }
        } catch (e) { if (window.sisWarn) window.sisWarn('Render failed: funnel', e); }
      });
    }
  } else {
    try {
      const container = getFunnelFacadeContainer();
      const filters = {};
      splitRenderFunnel(container, filters);
      if (window.sisRenderBus && typeof window.sisRenderBus.emit === "function") {
        window.sisRenderBus.emit("funnel:rendered");
      }
    } catch (e) { if (window.sisWarn) window.sisWarn('Render failed: funnel', e); }
  }
})();
