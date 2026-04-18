/**
 * Liquidity Tracker — data processing pipelines.
 *
 * Two independent flows:
 *   1. Daily Tracker: Client Master + Z10 + Current Account Data -> per-RM tracker.
 *   2. Update Master: Bank Recon diffed against the existing Client Master.
 *
 * All work happens in-memory.
 */

import Papa from 'papaparse';
import * as XLSX from 'xlsx';
import JSZip from 'jszip';

const extractArray = (input) => {
  if (Array.isArray(input)) return input;
  if (input && Array.isArray(input.data)) return input.data;
  return [];
};

// -----------------------------------------------------------------------------
// File parsing — handles both CSV and Excel (.xlsx / .xlsm)
// -----------------------------------------------------------------------------

export function parseFile(file) {
  return new Promise((resolve, reject) => {
    const name = (file.name || '').toLowerCase();
    const isExcel = name.endsWith('.xlsx') || name.endsWith('.xlsm') || name.endsWith('.xls');

    const finish = (rows, source) => {
      if (!Array.isArray(rows)) {
        reject(new Error(`Parser for "${file.name}" returned a non-array (${source}).`));
        return;
      }
      resolve(rows);
    };

    if (isExcel) {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const data = new Uint8Array(e.target.result);
          const workbook = XLSX.read(data, { type: 'array' });
          const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
          const rows = XLSX.utils.sheet_to_json(firstSheet, { defval: '', raw: false });
          finish(rows, 'xlsx');
        } catch (err) {
          reject(new Error(`Failed to parse Excel file "${file.name}": ${err.message}`));
        }
      };
      reader.onerror = () => reject(new Error(`Failed to read file "${file.name}"`));
      reader.readAsArrayBuffer(file);
      return;
    }

    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      complete: (results) => {
        if (results.errors && results.errors.length > 0) {
          if (!results.data || results.data.length === 0) {
            reject(new Error(`Failed to parse "${file.name}": ${results.errors[0].message}`));
            return;
          }
        }
        finish(results.data, 'csv');
      },
      error: (err) => reject(new Error(`Failed to parse "${file.name}": ${err.message}`)),
    });
  });
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

/** Normalizes a bank account number to a 12-digit zero-padded string. */
export function normalizeAccount(value) {
  if (value === null || value === undefined || value === '') return '';
  const asStr = String(value).trim().replace(/\.0+$/, '');
  return asStr.padStart(12, '0');
}

function toNumber(value) {
  if (value === null || value === undefined || value === '') return 0;
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  const cleaned = String(value).replace(/[,\s₹$]/g, '');
  const n = parseFloat(cleaned);
  return Number.isFinite(n) ? n : 0;
}

function str(v) {
  return v === null || v === undefined ? '' : String(v).trim();
}

function assertColumns(rows, required, label) {
  if (!Array.isArray(rows)) {
    throw new Error(`${label} did not parse to an array (got ${typeof rows}).`);
  }
  if (rows.length === 0) {
    throw new Error(`${label} file appears to be empty.`);
  }
  const presentKeys = Object.keys(rows[0]);
  const presentLower = new Set(presentKeys.map((k) => k.toLowerCase()));
  const missing = required.filter((c) => !presentLower.has(c.toLowerCase()));
  if (missing.length > 0) {
    throw new Error(
      `${label} is missing required column(s): ${missing.join(', ')}. ` +
        `Found: ${presentKeys.join(', ')}`
    );
  }
}

/** Returns the first matching value from row, trying each key in order (case-insensitive). */
function getField(row, ...keys) {
  for (const k of keys) {
    if (k in row) return row[k];
  }
  const lowerRow = {};
  for (const k of Object.keys(row)) lowerRow[k.toLowerCase()] = row[k];
  for (const k of keys) {
    const v = lowerRow[k.toLowerCase()];
    if (v !== undefined) return v;
  }
  return undefined;
}

// -----------------------------------------------------------------------------
// Pipeline stages — Z10 and Current Account Data are shared primitives
// -----------------------------------------------------------------------------

function buildLiquidMfBalances(z10Rows) {
  const balances = {};
  for (const row of z10Rows) {
    const secType = str(getField(row, 'SECURITY TYPE DESCRIPTION', 'Security Type Description'));
    const astCls = str(getField(row, 'ASTCLSNAME'));
    if (secType !== 'Mutual Funds' || astCls !== 'Cash') continue;

    const clientId = str(getField(row, 'WS CLIENT ID', 'WS client id'));
    if (!clientId) continue;
    balances[clientId] = (balances[clientId] || 0) + toNumber(getField(row, 'MKTVALUE'));
  }
  return balances;
}

function buildCurrentAccountMapping(caRows) {
  const mapping = {};
  for (const row of caRows) {
    const acct = normalizeAccount(row['Account Number']);
    if (!acct) continue;
    if (!(acct in mapping)) {
      mapping[acct] = toNumber(row['Available Balance']);
    }
  }
  return mapping;
}

/**
 * Bank exports often include title rows before the real header
 * (e.g. "OPERATIVE ACCOUNTS"), causing parsers to emit __EMPTY keys.
 * This scans every row looking for one whose VALUES contain all of
 * requiredHeaders (case-insensitive).  Once found it builds a key-map
 * and returns only the data rows that follow, re-keyed to the canonical
 * header names.  Returns null if the header row cannot be found.
 */
function reshapeByHeaderRow(rows, requiredHeaders) {
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const valueToKey = {};
    for (const [k, v] of Object.entries(row)) {
      if (v !== undefined && v !== null && String(v).trim() !== '') {
        valueToKey[String(v).trim().toLowerCase()] = k;
      }
    }

    const keyMap = {};
    for (const h of requiredHeaders) {
      const match = valueToKey[h.toLowerCase()];
      if (match !== undefined) keyMap[h] = match;
    }

    if (Object.keys(keyMap).length === requiredHeaders.length) {
      return rows
        .slice(i + 1)
        .map((row) => {
          const out = {};
          for (const [header, srcKey] of Object.entries(keyMap)) {
            out[header] = row[srcKey];
          }
          return out;
        })
        .filter((row) =>
          Object.values(row).some((v) => v !== '' && v !== undefined && v !== null)
        );
    }
  }
  return null;
}

// -----------------------------------------------------------------------------
// Tab 1 — Daily Tracker
// -----------------------------------------------------------------------------

const MASTER_COLS = ['WS Client Code', 'Client Name', 'RM Name', 'ICICI Acc. No.'];
const Z10_COLS = ['WS CLIENT ID', 'SECURITY TYPE DESCRIPTION', 'ASTCLSNAME', 'MKTVALUE'];
const CA_COLS = ['Account Number', 'Available Balance'];

/**
 * Builds the daily tracker using the Client Master as the source of truth.
 * Z10 contributes Liquid MF balances (Mutual Funds + Cash only).
 * Current Account Data contributes ICICI cash balances.
 */
export function buildDailyTracker(masterRows, z10Rows, caRows) {
  const safeMaster = extractArray(masterRows);
  const safeZ10 = extractArray(z10Rows);
  const safeCa = extractArray(caRows);

  assertColumns(safeMaster, MASTER_COLS, 'Client Master');
  assertColumns(safeZ10, Z10_COLS, 'Z10');

  // CA files from bank exports often have title rows before the real header.
  // Try to locate the "Account Number" header row dynamically; fall back to
  // the raw rows if they already carry the correct column names.
  const reshapedCa = reshapeByHeaderRow(safeCa, CA_COLS) ?? safeCa;
  assertColumns(reshapedCa, CA_COLS, 'Current Account Data');

  const warnings = [];
  const liquidMfBalances = buildLiquidMfBalances(safeZ10);
  const caMapping = buildCurrentAccountMapping(reshapedCa);

  const rows = [];
  let clientsWithoutRm = 0;
  let clientsWithoutIcici = 0;

  for (const mRow of safeMaster) {
    const clientCode = str(mRow['WS Client Code']);
    if (!clientCode) continue;

    const rmName = str(mRow['RM Name']);
    const iciciAccount = normalizeAccount(mRow['ICICI Acc. No.']);
    const iciciBalance = caMapping[iciciAccount] || 0;
    const liquidMf = liquidMfBalances[clientCode] || 0;

    if (!rmName) clientsWithoutRm += 1;
    if (!iciciAccount || !(iciciAccount in caMapping)) clientsWithoutIcici += 1;

    rows.push({
      'RM Name': rmName,
      'WS Client Code': clientCode,
      'Client Name': str(mRow['Client Name']),
      'ICICI Acc. No.': iciciAccount,
      'ICICI Bank Balance': iciciBalance,
      'Liquid MF Balance': liquidMf,
      'Total Liquidity': iciciBalance + liquidMf,
    });
  }

  rows.sort((a, b) => {
    const rmA = a['RM Name'];
    const rmB = b['RM Name'];
    if (rmA === '' && rmB !== '') return 1;
    if (rmB === '' && rmA !== '') return -1;
    if (rmA !== rmB) return rmA.localeCompare(rmB);
    return b['Total Liquidity'] - a['Total Liquidity'];
  });

  const summary = {
    totalClients: rows.length,
    totalRMs: new Set(rows.map((r) => r['RM Name']).filter(Boolean)).size,
    totalIcici: rows.reduce((s, r) => s + r['ICICI Bank Balance'], 0),
    totalLiquidMf: rows.reduce((s, r) => s + r['Liquid MF Balance'], 0),
    totalLiquidity: rows.reduce((s, r) => s + r['Total Liquidity'], 0),
  };

  if (clientsWithoutRm > 0) {
    warnings.push(`${clientsWithoutRm} client(s) in the Master have no RM Name.`);
  }
  if (clientsWithoutIcici > 0) {
    warnings.push(`${clientsWithoutIcici} client(s) had no matching Current Account balance.`);
  }

  return { rows, summary, warnings };
}

// -----------------------------------------------------------------------------
// Tab 2 — Update Master (diff against Bank Recon)
// -----------------------------------------------------------------------------

const RECON_COLS = ['Bank Code', 'Client Name', 'Bank Account'];

/**
 * Applies Bank Recon updates to the Client Master.
 *   - New Client Codes in the Recon are appended with a blank RM Name.
 *   - Existing clients whose Bank Account differs (after normalization)
 *     have their ICICI Acc. No. updated to the Recon value.
 *   - Existing clients with matching accounts are left unchanged.
 *
 * Returns the full updated master plus per-row status flags and a summary.
 */
export function buildUpdatedMaster(masterRows, reconRows) {
  const safeMaster = extractArray(masterRows);
  const safeRecon = extractArray(reconRows);

  assertColumns(safeMaster, MASTER_COLS, 'Client Master');
  assertColumns(safeRecon, RECON_COLS, 'Bank Recon');

  const byCode = new Map();
  for (const m of safeMaster) {
    const code = str(m['WS Client Code']);
    if (!code) continue;
    byCode.set(code, {
      'WS Client Code': code,
      'Client Name': str(m['Client Name']),
      'RM Name': str(m['RM Name']),
      'ICICI Acc. No.': normalizeAccount(m['ICICI Acc. No.']),
      Status: 'Unchanged',
    });
  }

  let added = 0;
  let updated = 0;

  for (const r of safeRecon) {
    const code = str(getField(r, 'Bank Code', 'CLIENT_CODE'));
    if (!code) continue;

    const reconAccount = normalizeAccount(getField(r, 'Bank Account', 'BANK_ACCOUNT'));
    const reconName = str(getField(r, 'Client Name', 'CLIENTNAME'));

    if (!byCode.has(code)) {
      byCode.set(code, {
        'WS Client Code': code,
        'Client Name': reconName,
        'RM Name': '',
        'ICICI Acc. No.': reconAccount,
        Status: 'New',
      });
      added += 1;
      continue;
    }

    const existing = byCode.get(code);
    if (reconAccount && existing['ICICI Acc. No.'] !== reconAccount) {
      existing['ICICI Acc. No.'] = reconAccount;
      existing.Status = 'Updated';
      updated += 1;
    }
  }

  const rows = Array.from(byCode.values()).sort((a, b) =>
    a['WS Client Code'].localeCompare(b['WS Client Code'])
  );

  return {
    rows,
    summary: {
      totalRecords: rows.length,
      added,
      updated,
      unchanged: rows.length - added - updated,
    },
  };
}

// -----------------------------------------------------------------------------
// Export helpers
// -----------------------------------------------------------------------------

const DAILY_HEADER = [
  'RM Name',
  'WS Client Code',
  'Client Name',
  'ICICI Acc. No.',
  'ICICI Bank Balance',
  'Liquid MF Balance',
  'Total Liquidity',
];

const DAILY_COL_WIDTHS = [
  { wch: 24 }, { wch: 16 }, { wch: 36 }, { wch: 18 },
  { wch: 18 }, { wch: 18 }, { wch: 18 },
];

const MASTER_HEADER = ['WS Client Code', 'Client Name', 'RM Name', 'ICICI Acc. No.'];
const MASTER_COL_WIDTHS = [{ wch: 16 }, { wch: 36 }, { wch: 24 }, { wch: 20 }];

function buildDailyWorkbookBuffer(rows, summaryRows) {
  const wb = XLSX.utils.book_new();

  const sheet = XLSX.utils.json_to_sheet(rows, { header: DAILY_HEADER });
  sheet['!cols'] = DAILY_COL_WIDTHS;
  XLSX.utils.book_append_sheet(wb, sheet, 'Liquidity Tracker');

  const summarySheet = XLSX.utils.json_to_sheet(summaryRows);
  summarySheet['!cols'] = [{ wch: 28 }, { wch: 22 }];
  XLSX.utils.book_append_sheet(wb, summarySheet, 'Summary');

  return XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
}

function dailySummaryRows(summary) {
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
  const totalLiquidMf = rmRows.reduce((s, r) => s + r['Liquid MF Balance'], 0);
  return [
    { Metric: 'RM Name', Value: rmName },
    { Metric: 'Clients', Value: rmRows.length },
    { Metric: 'Total ICICI Balance', Value: totalIcici },
    { Metric: 'Total Liquid MF Balance', Value: totalLiquidMf },
    { Metric: 'Total Liquidity', Value: totalIcici + totalLiquidMf },
    { Metric: 'Generated At', Value: new Date().toLocaleString('en-IN') },
  ];
}

export function groupRowsByRm(rows) {
  const groups = new Map();
  for (const row of rows) {
    const key = row['RM Name'] || 'Unassigned';
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }
  return groups;
}

function safeFileName(name) {
  return String(name).replace(/[\\/:*?"<>|]+/g, '_').trim() || 'Unassigned';
}

/**
 * ZIP Blob with the master daily tracker + one workbook per RM.
 */
export async function exportDailyTrackerZip(rows, summary) {
  const zip = new JSZip();
  const today = new Date().toISOString().slice(0, 10);

  const masterBuffer = buildDailyWorkbookBuffer(rows, dailySummaryRows(summary));
  zip.file(`Onza_Liquidity_Tracker_${today}.xlsx`, masterBuffer);

  const rmFolder = zip.folder('RM_Files');
  for (const [rmName, rmRows] of groupRowsByRm(rows)) {
    const buf = buildDailyWorkbookBuffer(rmRows, rmSummaryRows(rmName, rmRows));
    rmFolder.file(`${safeFileName(rmName)}.xlsx`, buf);
  }

  return zip.generateAsync({ type: 'blob' });
}

/**
 * Single-workbook export of the updated Client Master.
 */
export function exportMasterToXlsx(rows) {
  const wb = XLSX.utils.book_new();

  const sheet = XLSX.utils.json_to_sheet(rows, { header: MASTER_HEADER });
  sheet['!cols'] = MASTER_COL_WIDTHS;
  XLSX.utils.book_append_sheet(wb, sheet, 'Client Master');

  return XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
}
