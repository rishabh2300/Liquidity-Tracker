/**
 * Liquidity Tracker — data processing pipeline
 *
 * Mirrors the Python prototype exactly:
 *   ws_balance + z10_holding + icici_balance  →  consolidated tracker
 *
 * All work happens in-memory. Nothing touches disk.
 */

import Papa from 'papaparse';
import * as XLSX from 'xlsx';
import JSZip from 'jszip';

// -----------------------------------------------------------------------------
// File parsing — handles both CSV and Excel (.xlsx / .xlsm)
// -----------------------------------------------------------------------------

/**
 * Parses a File object into an array of row objects.
 * For CSVs, `stringColumns` are read as strings (protects account numbers
 * from being coerced to Number and losing leading zeros or gaining `.0`).
 */
export function parseFile(file, stringColumns = []) {
  return new Promise((resolve, reject) => {
    const name = (file.name || '').toLowerCase();
    const isExcel = name.endsWith('.xlsx') || name.endsWith('.xlsm') || name.endsWith('.xls');

    if (isExcel) {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const data = new Uint8Array(e.target.result);
          const workbook = XLSX.read(data, { type: 'array' });
          const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
          // raw: false so Excel formats numbers/dates as displayed strings,
          // which keeps account numbers intact.
          const rows = XLSX.utils.sheet_to_json(firstSheet, { defval: '', raw: false });
          resolve(rows);
        } catch (err) {
          reject(new Error(`Failed to parse Excel file "${file.name}": ${err.message}`));
        }
      };
      reader.onerror = () => reject(new Error(`Failed to read file "${file.name}"`));
      reader.readAsArrayBuffer(file);
      return;
    }

    // CSV branch
    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: (field) => !stringColumns.includes(field),
      complete: (results) => {
        if (results.errors && results.errors.length > 0) {
          // Papa emits non-fatal warnings; surface only if no rows came through.
          if (!results.data || results.data.length === 0) {
            reject(new Error(`Failed to parse "${file.name}": ${results.errors[0].message}`));
            return;
          }
        }
        resolve(results.data);
      },
      error: (err) => reject(new Error(`Failed to parse "${file.name}": ${err.message}`)),
    });
  });
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

/**
 * Normalizes a bank account number to a 12-digit zero-padded string.
 * Mirrors:   str.replace(r'\.0$', '').zfill(12)
 */
export function normalizeAccount(value) {
  if (value === null || value === undefined || value === '') return '';
  const asStr = String(value).trim().replace(/\.0+$/, '');
  return asStr.padStart(12, '0');
}

/** Safely coerces a CSV value to a Number; returns 0 on junk/blank. */
function toNumber(value) {
  if (value === null || value === undefined || value === '') return 0;
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  // Strip commas, currency symbols, and whitespace
  const cleaned = String(value).replace(/[,\s₹$]/g, '');
  const n = parseFloat(cleaned);
  return Number.isFinite(n) ? n : 0;
}

/** Trims and normalizes a string key (for dedup / mapping). */
function str(v) {
  return v === null || v === undefined ? '' : String(v).trim();
}

// -----------------------------------------------------------------------------
// Pipeline stages — each one maps directly to a block of the Python reference
// -----------------------------------------------------------------------------

/**
 * Builds the RM mapping: { [WS client id]: RMNAME }
 * First-seen-wins for duplicates, matching pandas' drop_duplicates default.
 */
function buildRmMapping(z10Rows) {
  const mapping = {};
  for (const row of z10Rows) {
    const clientId = str(row['WS client id']);
    const rm = str(row['RMNAME']);
    if (!clientId || !rm) continue;
    if (!(clientId in mapping)) {
      mapping[clientId] = rm;
    }
  }
  return mapping;
}

/**
 * Sums MKTVALUE per client where Security Type Description === 'Mutual Funds'
 * AND ASTCLSNAME === 'Cash'.  Returns { [CLIENT_CODE]: Liquid_MF_Balance }.
 */
function buildLiquidMfBalances(z10Rows) {
  const balances = {};
  for (const row of z10Rows) {
    const secType = str(row['Security Type Description']);
    const astCls = str(row['ASTCLSNAME']);
    if (secType !== 'Mutual Funds' || astCls !== 'Cash') continue;

    const clientId = str(row['WS client id']);
    if (!clientId) continue;
    balances[clientId] = (balances[clientId] || 0) + toNumber(row['MKTVALUE']);
  }
  return balances;
}

/**
 * Builds the ICICI mapping: { [normalized account]: Available Balance }.
 * First-seen-wins on duplicate accounts, matching the Python drop_duplicates.
 */
function buildIciciMapping(iciciRows) {
  const mapping = {};
  for (const row of iciciRows) {
    const acct = normalizeAccount(row['Account Number']);
    if (!acct) continue;
    if (!(acct in mapping)) {
      mapping[acct] = toNumber(row['Available Balance']);
    }
  }
  return mapping;
}

// -----------------------------------------------------------------------------
// Main entry point
// -----------------------------------------------------------------------------

/**
 * Runs the full liquidity calculation.
 *
 * @param {Array} wsRows     rows from WS Balance file
 * @param {Array} z10Rows    rows from Z10 Holding / AssetClass file
 * @param {Array} iciciRows  rows from ICICI Balance file
 * @returns {Object} { rows, summary, warnings }
 */
export function buildLiquidityTracker(wsRows, z10Rows, iciciRows) {
  const warnings = [];

  // Schema sanity checks — fail loud with a clear message instead of silently
  // producing a zero-filled tracker.
  const requiredWs = ['CLIENT_CODE', 'CLIENTNAME', 'BANK_ACCOUNT'];
  const requiredZ10 = ['WS client id', 'RMNAME', 'Security Type Description', 'ASTCLSNAME', 'MKTVALUE'];
  const requiredIcici = ['Account Number', 'Available Balance'];

  assertColumns(wsRows, requiredWs, 'WS Balance');
  assertColumns(z10Rows, requiredZ10, 'Z10 Holding');
  assertColumns(iciciRows, requiredIcici, 'ICICI Balance');

  // Stage 1 — RM mapping from Z10
  const rmMapping = buildRmMapping(z10Rows);

  // Stage 2 — Liquid MF balances (Mutual Funds + Cash asset class)
  const liquidMfBalances = buildLiquidMfBalances(z10Rows);

  // Stage 3 — ICICI account-level cash
  const iciciMapping = buildIciciMapping(iciciRows);

  // Stage 4 — build the tracker from WS Balance as the base
  const rows = [];
  let clientsWithoutRm = 0;
  let clientsWithoutIcici = 0;

  for (const wsRow of wsRows) {
    const clientCode = str(wsRow['CLIENT_CODE']);
    if (!clientCode) continue;

    const normAcct = normalizeAccount(wsRow['BANK_ACCOUNT']);
    const rmName = rmMapping[clientCode] || '';
    const iciciBalance = iciciMapping[normAcct] || 0;
    const liquidMf = liquidMfBalances[clientCode] || 0;

    if (!rmName) clientsWithoutRm += 1;
    if (!(normAcct in iciciMapping)) clientsWithoutIcici += 1;

    rows.push({
      CLIENT_CODE: clientCode,
      CLIENTNAME: str(wsRow['CLIENTNAME']),
      BANK_ACCOUNT: str(wsRow['BANK_ACCOUNT']),
      'RM Name': rmName,
      'ICICI Bank Balance': iciciBalance,
      'Liquid_MF_Balance': liquidMf,
      'Total Liquidity': iciciBalance + liquidMf,
    });
  }

  // Stage 5 — sort: RM Name asc, Total Liquidity desc.
  // Clients without an RM sort last (matches pandas NaN-last behavior).
  rows.sort((a, b) => {
    const rmA = a['RM Name'];
    const rmB = b['RM Name'];
    if (rmA === '' && rmB !== '') return 1;
    if (rmB === '' && rmA !== '') return -1;
    if (rmA !== rmB) return rmA.localeCompare(rmB);
    return b['Total Liquidity'] - a['Total Liquidity'];
  });

  // Summary stats — useful header for the ops team
  const summary = {
    totalClients: rows.length,
    totalRMs: new Set(rows.map((r) => r['RM Name']).filter(Boolean)).size,
    totalIcici: rows.reduce((s, r) => s + r['ICICI Bank Balance'], 0),
    totalLiquidMf: rows.reduce((s, r) => s + r['Liquid_MF_Balance'], 0),
    totalLiquidity: rows.reduce((s, r) => s + r['Total Liquidity'], 0),
  };

  if (clientsWithoutRm > 0) {
    warnings.push(`${clientsWithoutRm} client(s) had no RM mapping in Z10.`);
  }
  if (clientsWithoutIcici > 0) {
    warnings.push(`${clientsWithoutIcici} client(s) had no matching ICICI account.`);
  }

  return { rows, summary, warnings };
}

function assertColumns(rows, required, label) {
  if (!rows || rows.length === 0) {
    throw new Error(`${label} file appears to be empty.`);
  }
  const present = new Set(Object.keys(rows[0]));
  const missing = required.filter((c) => !present.has(c));
  if (missing.length > 0) {
    throw new Error(
      `${label} is missing required column(s): ${missing.join(', ')}. ` +
        `Found: ${Array.from(present).join(', ')}`
    );
  }
}

// -----------------------------------------------------------------------------
// Export helpers
// -----------------------------------------------------------------------------

/**
 * Builds an XLSX ArrayBuffer from a set of tracker rows + summary metrics.
 * Used for both the master file and per-RM files so they share formatting.
 */
function buildWorkbookBuffer(rows, summaryRows) {
  const wb = XLSX.utils.book_new();

  const trackerSheet = XLSX.utils.json_to_sheet(rows, {
    header: [
      'RM Name',
      'CLIENT_CODE',
      'CLIENTNAME',
      'BANK_ACCOUNT',
      'ICICI Bank Balance',
      'Liquid_MF_Balance',
      'Total Liquidity',
    ],
  });
  trackerSheet['!cols'] = [
    { wch: 24 },
    { wch: 14 },
    { wch: 36 },
    { wch: 18 },
    { wch: 18 },
    { wch: 18 },
    { wch: 18 },
  ];
  XLSX.utils.book_append_sheet(wb, trackerSheet, 'Liquidity Tracker');

  const summarySheet = XLSX.utils.json_to_sheet(summaryRows);
  summarySheet['!cols'] = [{ wch: 28 }, { wch: 22 }];
  XLSX.utils.book_append_sheet(wb, summarySheet, 'Summary');

  return XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
}

function masterSummaryRows(summary) {
  return [
    { Metric: 'Total Clients', Value: summary.totalClients },
    { Metric: 'Total RMs', Value: summary.totalRMs },
    { Metric: 'Total ICICI Balance', Value: summary.totalIcici },
    { Metric: 'Total Liquid MF Balance', Value: summary.totalLiquidMf },
    { Metric: 'Total Liquidity', Value: summary.totalLiquidity },
    { Metric: 'Generated At', Value: new Date().toLocaleString('en-IN') },
  ];
}

function rmSummaryRows(rmName, rmRows) {
  const totalIcici = rmRows.reduce((s, r) => s + r['ICICI Bank Balance'], 0);
  const totalLiquidMf = rmRows.reduce((s, r) => s + r['Liquid_MF_Balance'], 0);
  return [
    { Metric: 'RM Name', Value: rmName },
    { Metric: 'Clients', Value: rmRows.length },
    { Metric: 'Total ICICI Balance', Value: totalIcici },
    { Metric: 'Total Liquid MF Balance', Value: totalLiquidMf },
    { Metric: 'Total Liquidity', Value: totalIcici + totalLiquidMf },
    { Metric: 'Generated At', Value: new Date().toLocaleString('en-IN') },
  ];
}

/** Formats the tracker as an Excel file (ArrayBuffer) the UI can download. */
export function exportToXlsx(rows, summary) {
  return buildWorkbookBuffer(rows, masterSummaryRows(summary));
}

/**
 * Groups tracker rows by 'RM Name'. Rows with no RM land under 'Unassigned'.
 * Preserves the input order within each group (which is already sorted by
 * Total Liquidity desc for a given RM, thanks to buildLiquidityTracker).
 */
export function groupRowsByRm(rows) {
  const groups = new Map();
  for (const row of rows) {
    const key = row['RM Name'] || 'Unassigned';
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }
  return groups;
}

/** Turns an RM name into a filesystem-safe filename fragment. */
function safeFileName(name) {
  return String(name).replace(/[\\/:*?"<>|]+/g, '_').trim() || 'Unassigned';
}

/**
 * Builds a ZIP Blob containing:
 *   - Onza_Liquidity_Tracker_<date>.xlsx     (master, all clients)
 *   - RM_Files/<RM name>.xlsx                (one per RM, own clients only)
 *
 * Returns a Promise<Blob> — callers should use file-saver (or equivalent)
 * to trigger the download.
 */
export async function exportToRmZip(rows, summary) {
  const zip = new JSZip();
  const today = new Date().toISOString().slice(0, 10);

  const masterBuffer = buildWorkbookBuffer(rows, masterSummaryRows(summary));
  zip.file(`Onza_Liquidity_Tracker_${today}.xlsx`, masterBuffer);

  const rmFolder = zip.folder('RM_Files');
  const groups = groupRowsByRm(rows);
  for (const [rmName, rmRows] of groups) {
    const buf = buildWorkbookBuffer(rmRows, rmSummaryRows(rmName, rmRows));
    rmFolder.file(`${safeFileName(rmName)}.xlsx`, buf);
  }

  return zip.generateAsync({ type: 'blob' });
}
