(function(){
  // Apply saved theme across pages (set by theme-toggle on maintenance)
  try{
    var savedTheme = localStorage.getItem("sis-theme");
    if (savedTheme === "dark" || savedTheme === "light") {
      document.documentElement.dataset.theme = savedTheme;
    }
  }catch(e){}

  // Find the filename part of the current path (index.html default)
  function currentFile(){
    var p = window.location.pathname || "";
    var last = p.split('/').filter(Boolean).pop() || "index.html";
    // Allow folder roots to resolve to index.html
    if (!/\./.test(last)) last = "index.html";
    return last.toLowerCase();
  }
  var file = currentFile();

  // Map multi-route aliases (just in case)
  var aliases = {
    "index.html":"index.html",
    "overview.html":"index.html",
    "funnel.html":"funnel.html",
    "fi-funnel.html":"funnel.html",
    "heatmap.html":"heatmap.html",
    "troubleshoot.html":"troubleshoot.html",
    "troubleshooting.html":"troubleshoot.html",
    "maintenance.html":"maintenance.html"
  };

  var target = aliases[file] || file;

  // Auto-activate matching nav link by filename
  var nav = document.querySelector('.sis-nav');
  if (nav){
    var links = nav.querySelectorAll('a[href]');
    links.forEach(function(a){
      try{
        var href = a.getAttribute('href').split('#')[0].split('?')[0];
        var last = href.split('/').filter(Boolean).pop() || "index.html";
        last = last.toLowerCase();
        if (aliases[last]) last = aliases[last];
        if (last === target){ a.classList.add('active'); }
      }catch(e){}
    });
  }
})();

window.sisRenderBus = window.sisRenderBus || (function(){
  const m = new Map();
  return {
    on: (evt, fn) => { if (!m.has(evt)) m.set(evt, new Set()); m.get(evt).add(fn); },
    off: (evt, fn) => { const s = m.get(evt); if (s) s.delete(fn); },
    emit: (evt, data) => { const s = m.get(evt); if (s) for (const fn of s) try { fn(data); } catch(_){} }
  };
})();

window.sisWarn = window.sisWarn || function(msg, ctx){
try { console.warn('[SIS]', msg, ctx || ''); } catch(_){}
};
(function(){
if (window.sisToast) return;
let tId = null, last = '';
window.sisToast = function(msg){
if (!msg || msg === last) return;
last = msg;
let el = document.getElementById('sis-toast');
if (!el) {
el = document.createElement('div');
el.id = 'sis-toast';
el.style.position = 'fixed';
el.style.top = '12px';
el.style.right = '12px';
el.style.zIndex = '99999';
el.style.padding = '10px 14px';
el.style.borderRadius = '10px';
el.style.background = 'rgba(0,0,0,.75)';
el.style.color = '#fff';
el.style.font = '14px/1.3 system-ui, -apple-system, Segoe UI, Roboto, Arial';
el.style.boxShadow = '0 6px 20px rgba(0,0,0,.35)';
el.style.pointerEvents = 'none';
el.style.maxWidth = '420px';
el.style.whiteSpace = 'pre-wrap';
document.body.appendChild(el);
}
el.textContent = msg;
el.style.opacity = '1';
clearTimeout(tId);
tId = setTimeout(()=>{ el.style.opacity = '0'; last=''; }, 3000);
};
})();

function sisBindOnce(el, type, handler, opts){
if (!el) return;
const key = 'sisBound_' + type;
const ds = el.dataset || (el.dataset = {});
if (!ds[key]) { el.addEventListener(type, handler, opts); ds[key] = '1'; }
}
