//
// SIS Funnel — Data Module (scaffold)
// Pure transforms only. No DOM reads/writes here.
//
/* global sisWarn, getAttemptsDistribution */

// Shape contract (JSDoc only)
/**
	•	@typedef {import('./types.js').FunnelSummary} FunnelSummary
	•	@typedef {import('./types.js').DistributionRow} DistributionRow
	•	@typedef {import('./types.js').FiltersState} FiltersState
*/

// Safe number coercion
function num(v){ return Number.isFinite(+v) ? +v : 0; }

// Normalize summary object without changing math
function coerceFunnelSummary(obj){
const o = obj || {};
return {
gaSelect: num(o.gaSelect),
gaUser: num(o.gaUser),
gaCred: num(o.gaCred),
sessions: num(o.sessions),
sessionsWithJobs: num(o.sessionsWithJobs ?? o.sess_jobs),
sessionsWithSuccess: num(o.sessionsWithSuccess ?? o.sess_success),
placements: num(o.placements)
};
}

// Distribution rows via shared helper (added in Phase 2)
function safeDistributionRows(){
try {
if (typeof getAttemptsDistribution === 'function') {
const rows = getAttemptsDistribution();
return Array.isArray(rows) ? rows : [];
}
} catch(e){ if (typeof sisWarn==='function') sisWarn('Funnel.data: distribution read failed', e); }
return [];
}

/**
	•	Build a normalized model the view can render.
	•	This function must mirror current funnel inputs; do not change formulas.
	•	@param {FiltersState} filters
	•	@param {object} opts optional hooks for supplying current summary/rows if needed later
*/
export function getFunnelData(filters, opts={}){
// Placeholders — in Step 3+ we’ll adapt this to call the exact existing data sources.
// For now, return a minimal model so the view module can be wired without risk.
const summary = coerceFunnelSummary(opts.summary || {});
const distribution = safeDistributionRows(); // DistributionRow[]

return {
summary,          // FunnelSummary
distribution,     // DistributionRow[]
filters: Object.assign({}, filters || {}),
meta: { version: 'scaffold-1' }
};
}

