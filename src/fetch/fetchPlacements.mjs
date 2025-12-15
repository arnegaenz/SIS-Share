// src/fetch/fetchPlacements.mjs
import { getCardPlacementPage } from "../api.mjs";

function extractPlacementRows(resp) {
  if (!resp) return [];

  if (Array.isArray(resp.body)) return resp.body;
  if (Array.isArray(resp.card_placement_results)) return resp.card_placement_results;
  if (Array.isArray(resp.results)) return resp.results;

  const body = resp.body;
  if (body && typeof body === "object") {
    if (Array.isArray(body.card_placement_results)) return body.card_placement_results;
    if (Array.isArray(body.results)) return body.results;
    if (Array.isArray(body.items)) return body.items;
  }

  if (Array.isArray(resp)) return resp;
  return [];
}

export async function fetchPlacementsForInstance(
  session,
  instanceName,
  startDate,
  endDate,
  seenPlacementIds,
  allPlacementsCombined
) {
  console.log(
    `Fetching card placement results from ${startDate} to ${endDate}...`
  );

  const MAX_PLACEMENT_PAGES = 500;
  const instancePlacements = [];
  let instancePlacementCounter = 0;

  const firstPlacementResp = await getCardPlacementPage(
    session,
    startDate,
    endDate,
    { page: 1 }
  );

  const firstRows = extractPlacementRows(firstPlacementResp);
  for (const r of firstRows) {
    const baseId =
      r.id ||
      r.result_id ||
      r.place_card_on_single_site_job_id ||
      `row-${instanceName}-${instancePlacementCounter++}`;

    const dedupeKey = `${instanceName}:${baseId}`;

    if (!seenPlacementIds.has(dedupeKey)) {
      seenPlacementIds.add(dedupeKey);
      const enriched = { ...r, _instance: instanceName };
      allPlacementsCombined.push(enriched);
      instancePlacements.push(enriched);
    }
  }

  const rawPlacementHeader = firstPlacementResp.headers?.get
    ? firstPlacementResp.headers.get("x-cardsavr-paging")
    : firstPlacementResp.headers?.["x-cardsavr-paging"];

  if (rawPlacementHeader) {
    let pagingMeta = JSON.parse(rawPlacementHeader);
    const pageLength =
      Number(pagingMeta.page_length) || firstRows.length || 25;
    const totalResults = Number(pagingMeta.total_results) || firstRows.length;
    const totalPages =
      pageLength > 0 ? Math.ceil(totalResults / pageLength) : 1;

    let currentPage = Number(pagingMeta.page) || 1;

    while (
      currentPage < totalPages &&
      currentPage < MAX_PLACEMENT_PAGES
    ) {
      const nextPage = currentPage + 1;
      const requestPaging = {
        ...pagingMeta,
        page: nextPage,
      };

      const resp = await getCardPlacementPage(
        session,
        startDate,
        endDate,
        requestPaging
      );

      const rows = extractPlacementRows(resp);

      for (const r of rows) {
        const baseId =
          r.id ||
          r.result_id ||
          r.place_card_on_single_site_job_id ||
          `row-${instanceName}-${instancePlacementCounter++}`;

        const dedupeKey = `${instanceName}:${baseId}`;

        if (!seenPlacementIds.has(dedupeKey)) {
          seenPlacementIds.add(dedupeKey);
          const enriched = { ...r, _instance: instanceName };
          allPlacementsCombined.push(enriched);
          instancePlacements.push(enriched);
        }
      }

      if (nextPage % 10 === 0) {
        console.log(
          `  ...fetched card placements page ${nextPage} of ${totalPages} for ${instanceName}`
        );
      }

      const nextHeader = resp.headers?.get
        ? resp.headers.get("x-cardsavr-paging")
        : resp.headers?.["x-cardsavr-paging"];

      if (!nextHeader) {
        break;
      }

      try {
        pagingMeta = JSON.parse(nextHeader);
      } catch {
        pagingMeta.page = nextPage;
      }

      const reportedPage = Number(pagingMeta.page);
      if (!Number.isFinite(reportedPage) || reportedPage <= currentPage) {
        break;
      }
      currentPage = reportedPage;
    }

    console.log(
      `✅ Finished fetching card placements for ${instanceName}: ${totalResults} reported by server | ${allPlacementsCombined.length} total across instances`
    );
  } else {
    console.log(
      `✅ Finished fetching card placements for ${instanceName}: ${firstRows.length} from this instance | ${allPlacementsCombined.length} total across instances`
    );
    return instancePlacements;
  }

  return instancePlacements;
}
