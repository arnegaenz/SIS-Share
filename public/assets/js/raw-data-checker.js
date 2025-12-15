/**
 * Client-side raw data metadata checker
 * Determines which dates in a range need refetching based on server-side metadata
 */

/**
 * Checks raw data status for a date range
 * @param {string} startDate - YYYY-MM-DD
 * @param {string} endDate - YYYY-MM-DD
 * @returns {Promise<Array>} List of dates that need refetching
 */
export async function checkDateRangeForRefetch(startDate, endDate) {
  try {
    const response = await fetch(`/api/check-raw-data?start=${startDate}&end=${endDate}`);
    if (!response.ok) {
      console.warn('[Raw Checker] API check failed, assuming no refetch needed');
      return [];
    }

    const result = await response.json();
    // result = { datesToRefetch: ['2025-12-10', '2025-12-11'], reasons: {...} }

    console.log(`[Raw Checker] Checked ${startDate} to ${endDate}:`, result);
    return result.datesToRefetch || [];
  } catch (err) {
    console.error('[Raw Checker] Error checking raw data:', err);
    return [];
  }
}

/**
 * Triggers auto-fetch for incomplete dates
 * @param {Array<string>} dates - List of YYYY-MM-DD dates to refetch
 * @returns {Promise<void>}
 */
export async function autoFetchIncompleteDates(dates) {
  if (!dates || dates.length === 0) {
    console.log('[Raw Checker] No dates need refetching');
    return;
  }

  const startDate = dates[0];
  const endDate = dates[dates.length - 1];

  console.log(`[Raw Checker] Auto-fetching incomplete dates: ${dates.join(', ')}`);

  // Trigger the existing update endpoint (SSE stream)
  const eventSource = new EventSource(
    `/run-update/stream?start=${startDate}&end=${endDate}&autoRefetch=1`
  );

  return new Promise((resolve, reject) => {
    // Server emits "done" on success; keep "complete" for backward compatibility.
    const onDone = () => {
      console.log('[Raw Checker] Auto-fetch complete');
      eventSource.close();
      resolve();
    };

    eventSource.addEventListener('complete', () => {
      onDone();
    });

    eventSource.addEventListener('done', () => {
      onDone();
    });

    eventSource.addEventListener('job_error', (event) => {
      try {
        const data = event && event.data ? JSON.parse(event.data) : null;
        const msg = data && data.message ? data.message : 'Auto-fetch failed';
        console.error('[Raw Checker] Auto-fetch job_error:', data || event);
        eventSource.close();
        reject(new Error(msg));
      } catch (e) {
        console.error('[Raw Checker] Auto-fetch job_error (parse failed):', e);
        eventSource.close();
        reject(e);
      }
    });

    eventSource.addEventListener('error', (err) => {
      console.error('[Raw Checker] Auto-fetch error:', err);
      eventSource.close();
      reject(err);
    });

    eventSource.addEventListener('progress', (event) => {
      try {
        const data = JSON.parse(event.data);
        console.log(`[Raw Checker] Progress: ${data.message || 'Processing...'}`);
      } catch (e) {
        console.log('[Raw Checker] Progress event received');
      }
    });
  });
}
