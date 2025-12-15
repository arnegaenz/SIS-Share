// src/fetch/fetchSessions.mjs
import { getSessionsPage } from "../api.mjs";

export async function fetchSessionsForInstance(
  session,
  instanceName,
  startDate,
  endDate,
  seenSessionIds,
  allSessionsCombined
) {
  console.log(`Fetching sessions from ${startDate} to ${endDate}...`);

  const instanceSessions = [];
  let sessionPagingHeaderJson = null;

  const collectRows = (rows) => {
    for (const s of rows) {
      const baseId = s.id ?? s.session_id ?? `sess-${allSessionsCombined.length}`;
      const globalId = `${instanceName}-${baseId}`;
      if (!seenSessionIds.has(globalId)) {
        seenSessionIds.add(globalId);
        const enriched = { ...s, _instance: instanceName };
        allSessionsCombined.push(enriched);
        instanceSessions.push(enriched);
      }
    }
  };

  const firstResp = await getSessionsPage(
    session,
    startDate,
    endDate,
    sessionPagingHeaderJson
  );

  const normalize = (resp) =>
    Array.isArray(resp?.body)
      ? resp.body
      : Array.isArray(resp?.cardholder_sessions)
      ? resp.cardholder_sessions
      : Array.isArray(resp)
      ? resp
      : [];

  const firstRows = normalize(firstResp);

  if (firstRows.length === 0) {
    console.log("Sessions TRY 1 returned no rows; will try path-with-query form...");
    try {
      const secondResp = await session.get(
        `/cardholder_sessions?created_on_min=${encodeURIComponent(
          `${startDate}T00:00:00Z`
        )}&created_on_max=${encodeURIComponent(`${endDate}T23:59:59Z`)}`
      );
      const secondRows = normalize(secondResp);
      if (secondRows.length === 0) {
        console.log("Sessions TRY 2 also returned no rows — treating as empty for this instance.");
        return instanceSessions;
      }
      collectRows(secondRows);
      console.log(
        `✅ Finished fetching sessions for ${instanceName}: ${instanceSessions.length} from this instance | ${allSessionsCombined.length} total across instances`
      );
      return instanceSessions;
    } catch (err) {
      console.log("Sessions TRY 2 (path-with-query) failed, likely unsupported on this instance.");
      return instanceSessions;
    }
  }

  collectRows(firstRows);

  let rawHeader = firstResp?.headers?.get
    ? firstResp.headers.get("x-cardsavr-paging")
    : firstResp?.headers?.["x-cardsavr-paging"];

  let totalPages = null;

  while (rawHeader) {
    let paging;
    try {
      paging = JSON.parse(rawHeader);
    } catch {
      break;
    }

    const page = Number(paging.page) || 1;
    const pageLength = Number(paging.page_length) || 0;
    const totalResults = Number(paging.total_results) || 0;

    if (!totalPages && pageLength > 0) {
      totalPages = Math.ceil(totalResults / pageLength);
    }

    if (pageLength === 0 || page * pageLength >= totalResults) break;

    const nextPage = page + 1;
    const nextPaging = { ...paging, page: nextPage };
    const resp = await getSessionsPage(
      session,
      startDate,
      endDate,
      JSON.stringify(nextPaging)
    );
    const rows = normalize(resp);
    collectRows(rows);
    if (totalPages && nextPage % 10 === 0) {
      console.log(
        `  ...fetched sessions page ${nextPage} of ${totalPages} for ${instanceName}`
      );
    }
    rawHeader = resp?.headers?.get
      ? resp.headers.get("x-cardsavr-paging")
      : resp?.headers?.["x-cardsavr-paging"];
  }

  console.log(
    `✅ Finished fetching sessions for ${instanceName}: ${instanceSessions.length} from this instance | ${allSessionsCombined.length} total across instances`
  );

  return instanceSessions;
}
