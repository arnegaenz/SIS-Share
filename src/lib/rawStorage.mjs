import fs from "node:fs";
import path from "node:path";

const RAW_ROOT = path.resolve("raw");

function writeFileAtomicSync(targetPath, contents) {
  const dir = path.dirname(targetPath);
  const base = path.basename(targetPath);
  const tmpPath = path.join(
    dir,
    `.${base}.tmp-${process.pid}-${Date.now()}-${Math.random()
      .toString(16)
      .slice(2)}`
  );
  fs.writeFileSync(tmpPath, contents);
  fs.renameSync(tmpPath, targetPath);
}

export function ensureRawDirs() {
  for (const d of ["ga", "sessions", "placements"]) {
    fs.mkdirSync(path.join(RAW_ROOT, d), { recursive: true });
  }
}

export function rawPath(type, date) {
  return path.join(RAW_ROOT, type, `${date}.json`);
}

export function rawExists(type, date) {
  return fs.existsSync(rawPath(type, date));
}

export function writeRaw(type, date, obj, { atomic = true } = {}) {
  ensureRawDirs();
  const outPath = rawPath(type, date);
  const contents = JSON.stringify(obj, null, 2);
  if (atomic) {
    writeFileAtomicSync(outPath, contents);
  } else {
    fs.writeFileSync(outPath, contents);
  }
}

export function writeRawAtomic(type, date, obj) {
  return writeRaw(type, date, obj, { atomic: true });
}

export function readRaw(type, date) {
  const p = rawPath(type, date);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function deleteRaw(type, date) {
  const p = rawPath(type, date);
  try {
    fs.unlinkSync(p);
    return true;
  } catch {
    return false;
  }
}

/**
 * Checks if a day is complete (day has ended in UTC)
 * @param {string} dateStr - YYYY-MM-DD format
 * @returns {boolean}
 */
function isDayComplete(dateStr) {
  const now = new Date();
  const dayEndUTC = new Date(dateStr + 'T23:59:59.999Z');
  return now > dayEndUTC;
}

/**
 * Writes raw data with metadata tracking fetch time and completion status
 * @param {string} type - 'sessions', 'placements', or 'ga'
 * @param {string} dateStr - YYYY-MM-DD format
 * @param {object} data - Raw data object (sessions array, placements array, or ga rows)
 * @param {object} options - Options like {atomic: true}
 */
export function writeRawWithMetadata(type, dateStr, data, options = {}) {
  const now = new Date();
  const isComplete = isDayComplete(dateStr);

  // Put _metadata FIRST so it appears at the top of the JSON file
  const wrappedData = {
    _metadata: {
      fetchedAt: now.toISOString(),
      isComplete: isComplete
    },
    ...data  // Spread existing data (sessions, placements, or rows)
  };

  console.log(`[Raw Storage] Writing ${type}/${dateStr}.json - Complete: ${isComplete}`);

  // Use existing writeRaw function with metadata-wrapped data
  writeRaw(type, dateStr, wrappedData, options);
}

/**
 * Reads raw data and returns both metadata and data
 * @param {string} type - 'sessions', 'placements', or 'ga'
 * @param {string} dateStr - YYYY-MM-DD format
 * @returns {object} { metadata: {...}, data: {...} } or { metadata: null, data: null } if file doesn't exist
 */
export function readRawWithMetadata(type, dateStr) {
  const p = rawPath(type, dateStr);

  if (!fs.existsSync(p)) {
    return { metadata: null, data: null };
  }

  try {
    const content = fs.readFileSync(p, 'utf8');
    const parsed = JSON.parse(content);

    // Check if metadata exists - if not, backfill it
    if (!parsed._metadata) {
      console.log(`[Raw Storage] Backfilling metadata for ${type}/${dateStr}.json`);

      // Assume old data was fetched before day ended (mark incomplete if day not ended yet)
      const isComplete = isDayComplete(dateStr);

      // Reconstruct object with _metadata FIRST (at top of file)
      const reordered = {
        _metadata: {
          fetchedAt: new Date(0).toISOString(),  // Unknown fetch time (epoch)
          isComplete: isComplete
        },
        ...parsed  // All existing data after metadata
      };

      // Write back with metadata at top
      writeFileAtomicSync(p, JSON.stringify(reordered, null, 2));

      // Update parsed to use reordered version
      parsed._metadata = reordered._metadata;
    }

    const { _metadata, ...data } = parsed;
    return {
      metadata: _metadata,
      data: data
    };
  } catch (err) {
    console.error(`[Raw Storage] Error reading ${type}/${dateStr}.json:`, err.message);
    return { metadata: null, data: null };
  }
}

/**
 * Checks if raw data exists and needs refetching
 * @param {string} type - 'sessions', 'placements', or 'ga'
 * @param {string} dateStr - YYYY-MM-DD format
 * @returns {object} { exists: boolean, needsRefetch: boolean, reason: string }
 */
export function checkRawDataStatus(type, dateStr) {
  const { metadata, data } = readRawWithMetadata(type, dateStr);

  if (!metadata || !data) {
    return { exists: false, needsRefetch: true, reason: 'File does not exist' };
  }

  if (metadata.isComplete === true) {
    return { exists: true, needsRefetch: false, reason: 'Day is complete, no refetch needed' };
  }

  // Metadata exists but isComplete is false
  if (isDayComplete(dateStr)) {
    return { exists: true, needsRefetch: true, reason: 'Day is now complete, refetch to get final data' };
  }

  return { exists: true, needsRefetch: false, reason: 'Day is still in progress' };
}
