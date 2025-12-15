/**
 * Persistent Data Cache using localStorage + IndexedDB
 *
 * This module provides a caching layer for daily data that persists across
 * page navigations and browser sessions. It automatically invalidates the
 * cache when the server data is updated.
 *
 * Uses localStorage for small items and IndexedDB for large bulk caches.
 */

(function() {
  'use strict';

  const CACHE_PREFIX = 'sis_daily_cache_';
  const VERSION_KEY = 'sis_data_version';
  const LOCALSTORAGE_THRESHOLD = 1 * 1024 * 1024; // 1MB - use IndexedDB for larger items
  const DB_NAME = 'SISDataCache';
  const DB_VERSION = 1;
  const STORE_NAME = 'cache';

  /**
   * IndexedDB wrapper for large cache entries
   */
  class IndexedDBCache {
    constructor() {
      this.db = null;
      this.ready = false;
      this.initPromise = this._init();
    }

    async _init() {
      return new Promise((resolve, reject) => {
        const request = indexedDB.open(DB_NAME, DB_VERSION);

        request.onerror = () => {
          console.warn('IndexedDB initialization failed:', request.error);
          resolve(false); // Continue without IndexedDB
        };

        request.onsuccess = () => {
          this.db = request.result;
          this.ready = true;
          console.log('IndexedDB initialized successfully');
          resolve(true);
        };

        request.onupgradeneeded = (event) => {
          const db = event.target.result;
          if (!db.objectStoreNames.contains(STORE_NAME)) {
            db.createObjectStore(STORE_NAME);
          }
        };
      });
    }

    async ensureReady() {
      if (this.ready) return true;
      return await this.initPromise;
    }

    async get(key) {
      await this.ensureReady();
      if (!this.db) return null;

      return new Promise((resolve) => {
        try {
          const transaction = this.db.transaction([STORE_NAME], 'readonly');
          const store = transaction.objectStore(STORE_NAME);
          const request = store.get(key);

          request.onsuccess = () => resolve(request.result || null);
          request.onerror = () => {
            console.warn('IndexedDB read error:', request.error);
            resolve(null);
          };
        } catch (err) {
          console.warn('IndexedDB get failed:', err);
          resolve(null);
        }
      });
    }

    async set(key, data) {
      await this.ensureReady();
      if (!this.db) return false;

      return new Promise((resolve) => {
        try {
          const transaction = this.db.transaction([STORE_NAME], 'readwrite');
          const store = transaction.objectStore(STORE_NAME);
          const request = store.put(data, key);

          request.onsuccess = () => resolve(true);
          request.onerror = () => {
            console.warn('IndexedDB write error:', request.error);
            resolve(false);
          };
        } catch (err) {
          console.warn('IndexedDB set failed:', err);
          resolve(false);
        }
      });
    }

    async has(key) {
      await this.ensureReady();
      if (!this.db) return false;

      return new Promise((resolve) => {
        try {
          const transaction = this.db.transaction([STORE_NAME], 'readonly');
          const store = transaction.objectStore(STORE_NAME);
          const request = store.get(key);

          request.onsuccess = () => resolve(request.result !== undefined);
          request.onerror = () => resolve(false);
        } catch (err) {
          resolve(false);
        }
      });
    }

    async remove(key) {
      await this.ensureReady();
      if (!this.db) return;

      try {
        const transaction = this.db.transaction([STORE_NAME], 'readwrite');
        const store = transaction.objectStore(STORE_NAME);
        store.delete(key);
      } catch (err) {
        console.warn('IndexedDB remove error:', err);
      }
    }

    async clearAll() {
      await this.ensureReady();
      if (!this.db) return;

      try {
        const transaction = this.db.transaction([STORE_NAME], 'readwrite');
        const store = transaction.objectStore(STORE_NAME);
        store.clear();
        console.log('IndexedDB cache cleared');
      } catch (err) {
        console.warn('IndexedDB clear error:', err);
      }
    }

    async getStats() {
      await this.ensureReady();
      if (!this.db) return { entries: 0 };

      return new Promise((resolve) => {
        try {
          const transaction = this.db.transaction([STORE_NAME], 'readonly');
          const store = transaction.objectStore(STORE_NAME);
          const request = store.count();

          request.onsuccess = () => resolve({ entries: request.result });
          request.onerror = () => resolve({ entries: 0 });
        } catch (err) {
          resolve({ entries: 0 });
        }
      });
    }
  }

  /**
   * DataCache class - manages persistent caching of daily data
   */
  class DataCache {
    constructor() {
      this.currentVersion = this._getStoredVersion(); // Load from localStorage immediately
      this.memoryCache = new Map(); // Fast in-memory cache for current session
      this.indexedDB = new IndexedDBCache(); // IndexedDB for large items
      this.initialized = false;

      // Start background validation
      this.init();
    }

    _shouldForceIndexedDB(key) {
      // Avoid blocking JSON.stringify for known huge objects (e.g. bulk daily cache).
      return typeof key === 'string' && key.indexOf('funnel_all_daily_data_v') === 0;
    }

    /**
     * Initialize the cache by checking server data version (runs in background)
     */
    async init() {
      try {
        const response = await fetch('/api/data-version');
        if (!response.ok) {
          console.warn('Failed to fetch data version, using cached version');
          this.initialized = true;
          return;
        }

        const versionData = await response.json();
        const serverVersion = versionData.version;

        // Check if cached version matches server version
        const cachedVersion = this._getStoredVersion();

        // Convert both to strings for comparison to handle type mismatches
        const serverVersionStr = String(serverVersion);
        const cachedVersionStr = String(cachedVersion);

        if (cachedVersionStr !== serverVersionStr) {
          console.log('Data version mismatch, clearing cache', {
            cached: cachedVersionStr,
            server: serverVersionStr
          });
          this.clearAll();
          this.currentVersion = serverVersionStr;
          this._setStoredVersion(serverVersionStr);
        } else {
          console.log('Cache version valid:', serverVersionStr);
          this.currentVersion = serverVersionStr;
        }

        this.initialized = true;
      } catch (err) {
        console.warn('Cache initialization failed:', err);
        this.initialized = true; // Continue anyway with cached version
      }
    }

    /**
     * Get data from cache (checks memory first, then localStorage, then IndexedDB)
     */
    get(key) {
      // Check memory cache first
      if (this.memoryCache.has(key)) {
        return this.memoryCache.get(key);
      }

      // Check localStorage for small items
      try {
        const cacheKey = CACHE_PREFIX + key;
        const cached = localStorage.getItem(cacheKey);

        if (cached) {
          const data = JSON.parse(cached);
          // Store in memory cache for faster subsequent access
          this.memoryCache.set(key, data);
          return data;
        }
      } catch (err) {
        // Ignore localStorage errors, will check IndexedDB
      }

      // For large items, check IndexedDB asynchronously
      // Return null immediately and let async getter handle it
      return null;
    }

    /**
     * Async get for IndexedDB support (for large cache entries)
     */
    async getAsync(key) {
      // Check memory cache first
      if (this.memoryCache.has(key)) {
        return this.memoryCache.get(key);
      }

      // Check localStorage for small items
      try {
        const cacheKey = CACHE_PREFIX + key;
        const cached = localStorage.getItem(cacheKey);

        if (cached) {
          const data = JSON.parse(cached);
          this.memoryCache.set(key, data);
          return data;
        }
      } catch (err) {
        // Continue to IndexedDB check
      }

      // Check IndexedDB for large items
      try {
        const data = await this.indexedDB.get(key);
        if (data) {
          // Store in memory cache for faster subsequent access
          this.memoryCache.set(key, data);
          return data;
        }
      } catch (err) {
        console.warn('IndexedDB read error:', err);
      }

      return null;
    }

    /**
     * Store data in cache (memory + localStorage for small items, IndexedDB for large)
     */
    async set(key, data) {
      // Always store in memory cache
      this.memoryCache.set(key, data);

      try {
        if (this._shouldForceIndexedDB(key)) {
          // Ensure any small/stale localStorage copy doesn't win reads.
          try {
            localStorage.removeItem(CACHE_PREFIX + key);
          } catch (err) {
            // Ignore localStorage errors
          }

          console.log(`Caching "${key}" in IndexedDB (forced)`);
          const success = await this.indexedDB.set(key, data);
          if (success) {
            console.log(`Successfully cached "${key}" in IndexedDB`);
          }
          return success;
        }

        const serialized = JSON.stringify(data);
        const size = serialized.length;

        // Small items go to localStorage
        if (size <= LOCALSTORAGE_THRESHOLD) {
          try {
            const cacheKey = CACHE_PREFIX + key;
            localStorage.setItem(cacheKey, serialized);
            console.log(`Cached "${key}" in localStorage (${(size / 1024).toFixed(1)}KB)`);
            return true;
          } catch (err) {
            if (err.name === 'QuotaExceededError') {
              console.warn('localStorage quota exceeded, falling back to IndexedDB');
              // Fall through to IndexedDB
            } else {
              console.warn('localStorage write error:', err);
              return false;
            }
          }
        }

        // Large items go to IndexedDB
        console.log(`Caching "${key}" in IndexedDB (${(size / 1024 / 1024).toFixed(2)}MB)`);
        const success = await this.indexedDB.set(key, data);
        if (success) {
          console.log(`Successfully cached "${key}" in IndexedDB`);
        }
        return success;

      } catch (err) {
        console.warn('Cache write error:', err);
        return false;
      }
    }

    /**
     * Check if key exists in cache
     */
    has(key) {
      if (this.memoryCache.has(key)) {
        return true;
      }

      try {
        const cacheKey = CACHE_PREFIX + key;
        if (localStorage.getItem(cacheKey) !== null) {
          return true;
        }
      } catch (err) {
        // Continue to IndexedDB check
      }

      // Note: Can't check IndexedDB synchronously
      // Use hasAsync() for complete check
      return false;
    }

    /**
     * Async check if key exists (includes IndexedDB)
     */
    async hasAsync(key) {
      if (this.memoryCache.has(key)) {
        return true;
      }

      try {
        const cacheKey = CACHE_PREFIX + key;
        if (localStorage.getItem(cacheKey) !== null) {
          return true;
        }
      } catch (err) {
        // Continue to IndexedDB check
      }

      return await this.indexedDB.has(key);
    }

    /**
     * Remove specific key from cache
     */
    async remove(key) {
      this.memoryCache.delete(key);

      try {
        const cacheKey = CACHE_PREFIX + key;
        localStorage.removeItem(cacheKey);
      } catch (err) {
        // Ignore localStorage errors
      }

      await this.indexedDB.remove(key);
    }

    /**
     * Clear all cached data
     */
    clearAll() {
      this.memoryCache.clear();

      try {
        const keys = Object.keys(localStorage);
        keys.forEach(key => {
          if (key.startsWith(CACHE_PREFIX)) {
            localStorage.removeItem(key);
          }
        });
        console.log('localStorage cache cleared');
      } catch (err) {
        console.warn('localStorage clear error:', err);
      }

      // Clear IndexedDB asynchronously
      this.indexedDB.clearAll();
    }

    /**
     * Get cache statistics
     */
    async getStats() {
      const stats = {
        memoryEntries: this.memoryCache.size,
        localStorageEntries: 0,
        localStorageSize: 0,
        indexedDBEntries: 0,
        version: this.currentVersion
      };

      try {
        const keys = Object.keys(localStorage);
        const cacheKeys = keys.filter(key => key.startsWith(CACHE_PREFIX));
        stats.localStorageEntries = cacheKeys.length;

        let totalSize = 0;
        cacheKeys.forEach(key => {
          totalSize += localStorage.getItem(key).length;
        });
        stats.localStorageSize = totalSize;
      } catch (err) {
        stats.localStorageError = err.message;
      }

      try {
        const idbStats = await this.indexedDB.getStats();
        stats.indexedDBEntries = idbStats.entries;
      } catch (err) {
        stats.indexedDBError = err.message;
      }

      return stats;
    }

    /**
     * Get stored version from localStorage
     */
    _getStoredVersion() {
      try {
        return localStorage.getItem(VERSION_KEY);
      } catch (err) {
        return null;
      }
    }

    /**
     * Set stored version in localStorage
     */
    _setStoredVersion(version) {
      try {
        localStorage.setItem(VERSION_KEY, version);
      } catch (err) {
        console.warn('Failed to store version:', err);
      }
    }
  }

  // Create global instance immediately (init runs in constructor)
  window.DataCache = new DataCache();

  // Expose stats for debugging (now async)
  window.getCacheStats = async () => await window.DataCache.getStats();

  console.log('DataCache module loaded and ready (localStorage + IndexedDB)');
})();

export const SIS_CACHE_VERSION = 2;

export function getAttemptsDistribution(){
const candidates = [window.job_distribution, window.jobs_distribution, window.attemptsDistribution, window.sisAttemptsDistribution];
const arr = candidates.find(a => Array.isArray(a)) || [];
/** @type {import('./types.js').DistributionRow[]} */
const rows = arr.map(r => ({
jobsPerSession: +((r.jobsPerSession ?? r.jobs_per_session ?? r.jobs ?? r.bucket) || 0),
sessions: +((r.sessions ?? r.count ?? r.value) || 0)
})).filter(r => Number.isFinite(r.jobsPerSession) && Number.isFinite(r.sessions) && r.sessions > 0)
.sort((a,b)=>a.jobsPerSession - b.jobsPerSession);
return rows;
}

export function coerceDailyRows(rows){
const out = [];
if (!Array.isArray(rows)) return out;
for (const r of rows){
const d = (r.date || '').toString();
const okDate = /^\d{4}-\d{2}-\d{2}$/.test(d);
const o = {
date: okDate ? d : '',
fi: (r.fi ?? r.FI ?? '').toString(),
instance: (r.instance ?? r.env ?? r.slice ?? '').toString(),
sessions: +r.sessions || 0,
sessionsWithJobs: +(r.sessionsWithJobs ?? r.sessions_w_jobs ?? r.sess_jobs ?? 0),
sessionsWithSuccess: +(r.sessionsWithSuccess ?? r.sessions_w_success ?? r.sess_success ?? 0),
placements: +r.placements || 0
};
if (!okDate) continue;
out.push(o);
}
if (rows.length && !out.length) { try { window.sisToast && window.sisToast('Some data was ignored due to format issues'); if (window.sisWarn) window.sisWarn('Some data was ignored due to format issues'); } catch(_){}}
return out;
}

export function coerceFunnelSummary(obj){
const o = obj || {};
return {
gaSelect: +o.gaSelect || 0,
gaUser: +o.gaUser || 0,
gaCred: +o.gaCred || 0,
sessions: +o.sessions || 0,
sessionsWithJobs: +(o.sessionsWithJobs ?? o.sess_jobs ?? 0),
sessionsWithSuccess: +(o.sessionsWithSuccess ?? o.sess_success ?? 0),
placements: +o.placements || 0
};
}

(function cacheVersioning(){
try {
const k = 'SIS_CACHE_VERSION';
const prev = +localStorage.getItem(k) || 0;
if (prev !== SIS_CACHE_VERSION) {
// If you have IndexedDB stores, bump their keys or clear here.
localStorage.setItem(k, String(SIS_CACHE_VERSION));
if (window.sisWarn) window.sisWarn('Cache reset due to schema version bump', {from: prev, to: SIS_CACHE_VERSION});
}
} catch(e){ /* no-op */ }
})();
