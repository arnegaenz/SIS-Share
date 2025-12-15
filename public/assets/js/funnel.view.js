//
// SIS Funnel — View Module (scaffold)
// DOM rendering only. No data fetching or heavy logic here.
//
/* global document */

function el(tag, cls){
const n = document.createElement(tag);
if (cls) n.className = cls;
return n;
}

/**
	•	Render the Funnel into the given container using a normalized model.
	•	Keep markup/classnames conservative so we don’t break CSS.
	•	This is a scaffold: we’ll swap in the real DOM build in Step 3–4.
*/
export function renderFunnelView(container, model){
if (!container) return;
// Minimal, non-destructive placeholder to verify wiring during split.
// We do NOT remove or replace the existing page in Step 2.
// Later (Step 3–4) we will render into a hidden container for snapshot compare.
const probe = el('div', 'sis-funnel-split-probe');
probe.setAttribute('data-sis-probe', 'funnel-split');
probe.style.display = 'none';
probe.textContent = JSON.stringify({
summary: model && model.summary ? {
sessions: model.summary.sessions,
sessionsWithJobs: model.summary.sessionsWithJobs,
sessionsWithSuccess: model.summary.sessionsWithSuccess
} : {},
distributionSize: Array.isArray(model && model.distribution) ? model.distribution.length : 0
});
container.appendChild(probe);
}

