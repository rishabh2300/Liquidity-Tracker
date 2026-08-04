/**
 * Liquidity Tracker — data processing pipeline.
 *
 * Daily Tracker: Z0 Client Master + Z10 + Current Account Data + Axis Client Wise
 *                -> per-RM tracker. The Client Master mapping is built dynamically
 *                from the Z0 file (no Bank Recon diff).
 *
 * All work happens in-memory.
 */

import Papa from 'papaparse';
import * as XLSX from 'xlsx';
import JSZip from 'jszip';

/**
 * Resolves input to a flat row array.
 *   - Already an array                → returned as-is.
 *   - Multi-sheet Excel object        → tries each name in preferredSheets,
 *                                       then falls back to the first sheet.
 *   - PapaParse result ({ data: [] }) → extracts .data.
 *   - Anything else                   → empty array.
 */
const extractArray = (input, preferredSheets = []) => {
  if (Array.isArray(input)) return input;
  if (input && Array.isArray(input.data)) return input.data;
  if (input && typeof input === 'object') {
    for (const name of preferredSheets) {
      if (Array.isArray(input[name])) return input[name];
    }
    const firstKey = Object.keys(input)[0];
    return firstKey && Array.isArray(input[firstKey]) ? input[firstKey] : [];
  }
  return [];
};

// -----------------------------------------------------------------------------
// File parsing — handles both CSV and Excel (.xlsx / .xlsm)
// -----------------------------------------------------------------------------

/** Returns the name of the first sheet in an Excel file, or null for CSV. */
export function getFirstSheetName(file) {
  return new Promise((resolve) => {
    const name = (file.name || '').toLowerCase();
    const isExcel = name.endsWith('.xlsx') || name.endsWith('.xlsm') || name.endsWith('.xls');
    if (!isExcel) { resolve(null); return; }
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const wb = XLSX.read(new Uint8Array(e.target.result), { type: 'array' });
        resolve(wb.SheetNames[0] ?? null);
      } catch { resolve(null); }
    };
    reader.onerror = () => resolve(null);
    reader.readAsArrayBuffer(file);
  });
}

/**
 * Parses a CSV or Excel file.
 *   - CSV        → resolves to a flat array of row objects.
 *   - Excel      → resolves to an object keyed by sheet name, each value being
 *                  a flat array of row objects. Callers that expect a single
 *                  sheet should pick the first key or a known sheet name.
 */
export function parseFile(file) {
  return new Promise((resolve, reject) => {
    const name = (file.name || '').toLowerCase();
    const isExcel = name.endsWith('.xlsx') || name.endsWith('.xlsm') || name.endsWith('.xls');

    if (isExcel) {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const data = new Uint8Array(e.target.result);
          const wb = XLSX.read(data, { type: 'array' });
          const sheets = {};
          wb.SheetNames.forEach((sheetName) => {
            sheets[sheetName] = XLSX.utils.sheet_to_json(wb.Sheets[sheetName], {
              defval: '',
              raw: false,
            });
          });
          resolve(sheets);
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
      transformHeader: (h) => String(h ?? '').trim(),
      skipEmptyLines: true,
      complete: (results) => {
        if (results.errors && results.errors.length > 0) {
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
// Content-based workbook detection
//
// Real exports are messy: pivot sheets sitting ahead of the data sheet
// (Z10 ships as [Sheet2-pivot, Sheet3-pivot, Sheet1-data]), title rows above
// the real header, renamed tabs. Instead of trusting sheet order or sheet
// names, every input is located by scanning all sheets for the row that
// carries ALL of its required column headers.
// -----------------------------------------------------------------------------

/**
 * Parses a workbook into raw per-sheet grids (arrays of arrays of formatted
 * strings). CSVs resolve to the same shape as a single-sheet workbook so
 * findTable's header scan handles them identically (trimming, title rows,
 * short rows).
 */
export function parseWorkbookGrids(file) {
  return new Promise((resolve, reject) => {
    const name = (file.name || '').toLowerCase();
    const isExcel = name.endsWith('.xlsx') || name.endsWith('.xlsm') || name.endsWith('.xls');

    if (!isExcel) {
      Papa.parse(file, {
        header: false,
        skipEmptyLines: true,
        complete: (results) =>
          resolve({ kind: 'grid', sheetNames: ['CSV'], grids: { CSV: results.data || [] } }),
        error: (err) => reject(new Error(`Failed to parse "${file.name}": ${err.message}`)),
      });
      return;
    }

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const wb = XLSX.read(new Uint8Array(e.target.result), { type: 'array' });
        const grids = {};
        wb.SheetNames.forEach((sheetName) => {
          grids[sheetName] = XLSX.utils.sheet_to_json(wb.Sheets[sheetName], {
            header: 1,
            defval: '',
            raw: false,
          });
        });
        resolve({ kind: 'excel', sheetNames: wb.SheetNames, grids });
      } catch (err) {
        reject(new Error(`Failed to parse Excel file "${file.name}": ${err.message}`));
      }
    };
    reader.onerror = () => reject(new Error(`Failed to read file "${file.name}"`));
    reader.readAsArrayBuffer(file);
  });
}

const normHeaderCell = (v) => String(v ?? '').trim().toUpperCase();

/**
 * Finds the table matching requiredCols anywhere in a parsed workbook: scans
 * every sheet's first rows for one whose cell values contain ALL required
 * headers. Matching is trimmed, case-insensitive, and exact-equality only —
 * substring matching would false-positive on pivot titles like
 * "Sum of MKTVALUE". Returns { sheetName, headerRowIndex, rows } with rows
 * keyed by the real (trimmed) headers, or null when no sheet qualifies.
 */
export function findTable(parsed, requiredCols, { maxScanRows = 40 } = {}) {
  const wanted = requiredCols.map(normHeaderCell);

  const grids = parsed?.grids || {};
  for (const sheetName of parsed?.sheetNames || Object.keys(grids)) {
    const grid = grids[sheetName] || [];
    const scanLimit = Math.min(grid.length, maxScanRows);
    for (let i = 0; i < scanLimit; i++) {
      const cellSet = new Set((grid[i] || []).map(normHeaderCell));
      if (!wanted.every((c) => cellSet.has(c))) continue;

      const headers = (grid[i] || []).map((c) => String(c ?? '').trim());
      const rows = [];
      for (let r = i + 1; r < grid.length; r++) {
        const cells = grid[r];
        if (!cells || !cells.some((v) => String(v ?? '').trim() !== '')) continue;
        const row = {};
        headers.forEach((h, col) => {
          if (!h) return;
          let key = h;
          let n = 1;
          while (key in row) key = `${h}_${n++}`;
          row[key] = cells[col] ?? '';
        });
        rows.push(row);
      }
      return { sheetName, headerRowIndex: i, rows };
    }
  }
  return null;
}

// Column fingerprints that uniquely identify each source report. Verified
// against real exports: no fingerprint matches any sheet of another report
// (the Z10 pivots lack WS CLIENT ID/ASTCLSNAME; the recon-summary sheet's
// lookalike headers are space-padded variants that still exact-match nothing).
const WS_MASTER_COLS = ['CLIENT_CODE', 'CLIENTNAME', 'BANK_ACCOUNT'];
const CA_EFFECTIVE_COLS = ['Account Number', 'Effective Balance'];
const CW_COLS = ['Client Name', 'Ledger Bal BOD'];
const Z2_COLS = ['CLIENTID', 'CLIENTCODE', 'CLIENTNAME', 'RMNAME', 'TRADING BANK ACCOUNT'];
const CLHR_COLS = ['Ent Id', 'Ent Name', 'Free Valuation'];

/**
 * Identifies which daily report a parsed workbook is, by content.
 * Roles: 'recon' (WS Balances + ICICI Balances), 'z2', 'z10', 'cw',
 * 'clhr' (recognized but unused), 'unknown' (with a hint when possible).
 */
export function classifyWorkbook(parsed) {
  const ws = findTable(parsed, WS_MASTER_COLS);
  // CA header variants in preference order — the canonical ICICI-portal pair
  // first, then the aliases normalizeCAColumns has always tolerated.
  const ca =
    findTable(parsed, CA_COLS) ||
    findTable(parsed, CA_EFFECTIVE_COLS) ||
    findTable(parsed, ['Account No', 'Available Balance']) ||
    findTable(parsed, ['ACCOUNT_NUMBER', 'Available Balance']) ||
    findTable(parsed, ['Bank Account', 'Sum of Bank Balance (Custody)']) ||
    findTable(parsed, ['Bank Account', 'Sum of Bank Balance (System)']);
  if (ws && ca) {
    return {
      role: 'recon',
      wsRows: ws.rows,
      caRows: ca.rows,
      wsSheet: ws.sheetName,
      caSheet: ca.sheetName,
    };
  }
  const z2 = findTable(parsed, Z2_COLS);
  if (z2) return { role: 'z2', z2Rows: z2.rows, sheet: z2.sheetName };
  const z10 = findTable(parsed, Z10_COLS);
  if (z10) return { role: 'z10', z10Rows: z10.rows, sheet: z10.sheetName };
  const cw = findTable(parsed, CW_COLS);
  if (cw) return { role: 'cw', cwRows: cw.rows, sheet: cw.sheetName };
  if (findTable(parsed, CLHR_COLS)) return { role: 'clhr' };
  if (ws) {
    return {
      role: 'unknown',
      hint: 'Found WS Balances data but no ICICI account sheet — the Bank Recon workbook needs both.',
    };
  }
  if (ca) {
    return {
      role: 'unknown',
      hint: 'Found ICICI account balances but no WS Balances sheet — the Bank Recon workbook needs both.',
    };
  }
  return { role: 'unknown' };
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

/** Normalizes a bank account number to a 12-digit zero-padded string. */
export function normalizeAccount(value) {
  if (value === null || value === undefined || value === '') return '';
  const asStr = String(value).trim().replace(/^'+/, '').replace(/\.0+$/, '');
  if (isNoAccountValue(asStr)) return 'N/A';
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

function isNoAccountValue(value) {
  const normalized = str(value).replace(/\./g, '').toUpperCase();
  return normalized === 'N/A' || normalized === 'NA' || normalized === 'NONE' || normalized === 'NO';
}

function normalizeWsCode(value) {
  const code = str(value).replace(/\.0+$/, '');
  return /^1\d{5}$/.test(code) ? code : '';
}

function assertColumns(rows, required, label) {
  if (!Array.isArray(rows)) {
    throw new Error(`${label} did not parse to an array (got ${typeof rows}).`);
  }
  if (rows.length === 0) {
    throw new Error(`${label} file appears to be empty.`);
  }
  const presentKeys = Object.keys(rows[0]);
  const presentLower = new Set(presentKeys.map((k) => k.trim().toLowerCase()));
  const missing = required.filter((c) => !presentLower.has(c.trim().toLowerCase()));
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

function getFirstNonEmptyField(row, ...keys) {
  let fallback;
  for (const key of keys) {
    const value = getField(row, key);
    if (fallback === undefined && value !== undefined) fallback = value;
    if (str(value)) return value;
  }
  return fallback;
}

function getFirstAccountField(row, ...keys) {
  let placeholder;
  for (const key of keys) {
    const value = getField(row, key);
    if (value === undefined) continue;
    if (isNoAccountValue(value)) {
      if (placeholder === undefined) placeholder = value;
      continue;
    }
    if (str(value)) return value;
  }
  return placeholder ?? '';
}

function getFirstWsCodeField(row, ...keys) {
  for (const key of keys) {
    const code = normalizeWsCode(getField(row, key));
    if (code) return code;
  }
  return '';
}

// -----------------------------------------------------------------------------
// Pipeline stages — Z10 and Current Account Data are shared primitives
// -----------------------------------------------------------------------------

function buildLiquidMfBalances(z10Rows) {
  const balances = {};
  const names = {};
  for (const row of z10Rows) {
    // Test-client holdings (RM "Test RM") never count toward liquidity.
    if (isTestRm(getField(row, 'RMNAME', 'RM NAME', 'RM Name'))) continue;
    const secType = str(getField(row, 'SECURITY TYPE DESCRIPTION', 'Security Type Description')).toLowerCase();
    const astCls  = str(getField(row, 'ASTCLSNAME')).toLowerCase();
    const subCls  = str(getField(row, 'SUBCLSNAME', 'Sub Class Name')).toLowerCase();
    const scheme  = str(getField(row, 'SCHEMENAME', 'Scheme Name', 'SECURITY NAME', 'Security Name')).toLowerCase();

    // Exclude Alternates, AIFs, and PMS regardless of other classifications.
    const isExcluded =
      astCls.includes('alternate') ||
      astCls.includes('aif') ||
      astCls.includes('pms') ||
      subCls.includes('alternate') ||
      subCls.includes('aif') ||
      subCls.includes('pms') ||
      scheme.includes(' aif') ||
      scheme.includes('alternate investment');

    if (isExcluded) continue;

    // Only count Mutual Fund rows explicitly classified as Cash / Liquid.
    const isMutualFund = secType === 'mutual funds' || secType === 'mutual fund';
    const isCashLiquid = astCls === 'cash' || astCls.includes('liquid');

    if (!isMutualFund || !isCashLiquid) continue;

    const clientId = normalizeWsCode(getField(row, 'WS CLIENT ID', 'WS client id'));
    if (!clientId) continue;
    balances[clientId] = (balances[clientId] || 0) + toNumber(getField(row, 'MKTVALUE'));
    if (!names[clientId]) {
      names[clientId] = str(getField(row, 'CLIENT NAME', 'Client Name'));
    }
  }
  return { balances, names };
}

/**
 * Builds a map of Axis Demat 'Ledger Bal BOD' keyed by uppercased Client Name.
 * Axis and Wealth Spectrum use different client codes, so name is the only
 * reliable join key. Multiple Axis rows for one client are summed.
 */
function buildAxisBalances(cwRows) {
  const balances = {};
  // Preserve the source-cased name for orphan-row display when the Axis
  // client has no matching WS Balances record.
  const names = {};
  const codes = {};
  for (const row of cwRows) {
    const clientName = str(getField(row, 'Client Name', 'CLIENT NAME'));
    if (!clientName) continue;
    const key = normalizeNameKey(clientName);
    const bal = toNumber(getField(row, 'Ledger Bal BOD', 'LEDGER BAL BOD'));
    balances[key] = (balances[key] || 0) + bal;
    if (!names[key]) names[key] = clientName;
    if (!codes[key]) codes[key] = str(getField(row, 'Client Code', 'CLIENT CODE'));
  }
  return { balances, names, codes };
}

function buildCurrentAccountMapping(caRows) {
  const mapping = {};
  // Per-account metadata so orphan rows can show the bank's account name.
  const accountInfo = {};

  for (const row of caRows) {
    const raw = str(row['Account Number'] ?? row['ACCOUNT_NUMBER'] ?? row['Account No'] ?? '');
    if (!raw || isNoAccountValue(raw)) continue;

    // Use ?? so a genuine 0 balance is stored, not skipped.
    const bal = toNumber(
      row['Effective Balance'] !== undefined && row['Effective Balance'] !== ''
        ? row['Effective Balance']
        : row['Available Balance'] !== undefined && row['Available Balance'] !== ''
          ? row['Available Balance']
          : row['BALANCE']
    );

    const accountName = str(row['Account Name'] ?? row['ACCOUNT_NAME'] ?? row['Name'] ?? '');

    // Store under both the zero-padded form and the stripped form so lookups
    // succeed regardless of whether Excel dropped leading zeros.
    const padded   = normalizeAccount(raw);
    const stripped = raw.replace(/^'+/, '').replace(/^0+/, '') || '0';

    if (padded   && !(padded   in mapping)) mapping[padded]   = bal;
    if (stripped && !(stripped in mapping)) mapping[stripped] = bal;

    if (padded && !(padded in accountInfo)) {
      accountInfo[padded] = { balance: bal, accountName, raw };
    }
  }
  return { mapping, accountInfo };
}

// -----------------------------------------------------------------------------
// Business rules & Z2 enrichment
// -----------------------------------------------------------------------------

/**
 * Join key for client names: collapses whitespace runs and uppercases.
 * Axis exports carry double internal spaces ("VELLACHI  MURUGAPPAN") that
 * a plain toUpperCase() join misses. Exported because the UI uses the same
 * key for saved manual merge mappings.
 */
export function normalizeNameKey(v) {
  return str(v).replace(/\s+/g, ' ').toUpperCase();
}

// Standing rules confirmed by ops (July 2026):
// - Test clients (RM "Test RM" in WS/Z10/Z2, or the dummy account below) are
//   excluded from the tracker and its totals.
// - Onza's own ICICI current account stays visible as a tracker row, always
//   assigned to Nikhil S Shah.
const TEST_RM = 'test rm';
const TEST_DUMMY_ACCOUNTS = new Set(['123456789123']);
export const ONZA_CORPORATE_ACCOUNT = '000205040390';
export const ONZA_CORPORATE_RM = 'Nikhil S Shah';

function isTestRm(rmName) {
  return str(rmName).toLowerCase() === TEST_RM;
}

function isTestAccount(account) {
  const normalized = normalizeAccount(account);
  return normalized !== '' && normalized !== 'N/A' && TEST_DUMMY_ACCOUNTS.has(normalized);
}

/**
 * Builds join indexes over the Z2 Client Master (optional enrichment file).
 * CLIENTCODE is preferred but holds a PAN on Ex-Broker rows, so CLIENTID is
 * the fallback; both pass through the /^1\d{5}$/ WS-code gate. Accepts either
 * raw Z2 rows or an already-built enrichment (returned unchanged).
 */
export function buildZ2Enrichment(z2Rows) {
  if (z2Rows && z2Rows.byCode) return z2Rows;
  const rows = extractArray(z2Rows);
  if (!rows.length) return null;

  const byCode = new Map();
  const byAccount = new Map();
  const byName = new Map();
  let count = 0;
  for (const row of rows) {
    const rmName = str(getField(row, 'RMNAME', 'RM Name'));
    if (isTestRm(rmName)) continue; // never re-import test clients via Z2
    const code = getFirstWsCodeField(row, 'CLIENTCODE', 'CLIENTID');
    const name = str(getField(row, 'CLIENTNAME', 'Client Name'));
    if (!code && !name) continue;
    const rawAccount = getFirstAccountField(row, 'TRADING BANK ACCOUNT', 'MANDATEBANKACCOUNTNO');
    const account = isNoAccountValue(rawAccount) ? '' : str(rawAccount);
    const entry = { code, name, rmName, account };
    count += 1;
    if (code && !byCode.has(code)) byCode.set(code, entry);
    if (account) {
      const padded = normalizeAccount(account);
      if (padded && padded !== 'N/A' && !byAccount.has(padded)) byAccount.set(padded, entry);
    }
    const nameKey = normalizeNameKey(name);
    if (nameKey && !byName.has(nameKey)) byName.set(nameKey, entry);
  }
  return { byCode, byAccount, byName, count };
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

const MASTER_FIELDS = {
  clientCode: ['WS Client Code', 'CLIENTCODE', 'CLIENTID', 'ACCOUNTCODE', 'Client Code'],
  clientName: ['Client Name', 'CLIENTNAME', 'CLIENT NAME'],
  rmName: ['RM Name', 'RMNAME', 'RELATIONSHIP MANAGER'],
  iciciAccount: [
    'ICICI Acc. No.',
    'TRADING BANK ACCOUNT',
    'MANDATEBANKACCOUNTNO',
    'CLIENT BANK ACCOUNT',
    'BANK_ACCOUNT',
    'Bank Account',
  ],
};

const MASTER_COLS = ['WS Client Code', 'Client Name', 'RM Name', 'ICICI Acc. No.'];

function assertMasterColumns(rows, label) {
  if (!Array.isArray(rows)) {
    throw new Error(`${label} did not parse to an array (got ${typeof rows}).`);
  }
  if (rows.length === 0) {
    throw new Error(`${label} file appears to be empty.`);
  }

  const presentKeys = Object.keys(rows[0]);
  const presentLower = new Set(presentKeys.map((k) => k.toLowerCase()));
  const required = [
    ['Client code', MASTER_FIELDS.clientCode],
    ['Client name', MASTER_FIELDS.clientName],
    ['RM name', MASTER_FIELDS.rmName],
    ['ICICI account number', MASTER_FIELDS.iciciAccount],
  ];
  const missing = required
    .filter(([, candidates]) => !candidates.some((c) => presentLower.has(c.toLowerCase())))
    .map(([name]) => name);

  if (missing.length > 0) {
    throw new Error(
      `${label} is missing required column(s): ${missing.join(', ')}. ` +
        `Found: ${presentKeys.join(', ')}`
    );
  }
}

function normalizeMasterRows(masterRows, label = 'Z0 Client Master') {
  const safeMaster = extractArray(masterRows);
  assertMasterColumns(safeMaster, label);

  return safeMaster.map((row) => {
    const account = getFirstAccountField(row, ...MASTER_FIELDS.iciciAccount);
    return {
      'WS Client Code': getFirstWsCodeField(row, ...MASTER_FIELDS.clientCode),
      'Client Name': str(getFirstNonEmptyField(row, ...MASTER_FIELDS.clientName)),
      'RM Name': str(getFirstNonEmptyField(row, ...MASTER_FIELDS.rmName)),
      'ICICI Acc. No.': isNoAccountValue(account) ? 'N/A' : str(account),
      ...(row.Status ? { Status: row.Status } : {}),
    };
  });
}

/**
 * Bank exports use different column names for account/balance and often
 * have stray leading/trailing whitespace in the headers (e.g.
 * " Effective Balance "). This remaps every recognised variant to canonical
 * keys so the rest of the pipeline can use exact-match lookups.
 *   Account Number    ← "Account Number" / "Bank Account" / "Account No" / any "account"
 *   Effective Balance ← "Effective Balance" (preferred for ICICI sheet)
 *   Available Balance ← "Available Balance" / "Sum of Bank Balance (Custody)" /
 *                       "(System)" / any "balance"
 */
function normalizeCAColumns(rows) {
  if (!rows.length) return rows;
  const keys = Object.keys(rows[0]);
  const norm = (k) => k.trim().toLowerCase();

  const findExact = (...candidates) => {
    for (const c of candidates) {
      const target = norm(c);
      const found = keys.find((k) => norm(k) === target);
      if (found) return found;
    }
    return null;
  };

  const findContains = (substr) => {
    const s = substr.toLowerCase();
    return keys.find((k) => norm(k).includes(s)) ?? null;
  };

  const accountKey =
    findExact('Account Number', 'Account No', 'ACCOUNT_NUMBER', 'Bank Account') ||
    findContains('account');

  const effectiveKey = findExact('Effective Balance');
  const availableKey = findExact('Available Balance');
  const fallbackBalKey =
    findExact('Sum of Bank Balance (Custody)', 'Sum of Bank Balance (System)') ||
    findContains('balance');

  if (!accountKey || (!effectiveKey && !availableKey && !fallbackBalKey)) return rows;

  return rows.map((row) => ({
    ...row,
    'Account Number': row[accountKey],
    'Effective Balance': effectiveKey ? row[effectiveKey] : '',
    'Available Balance': availableKey
      ? row[availableKey]
      : fallbackBalKey
        ? row[fallbackBalKey]
        : '',
  }));
}
const Z10_COLS = ['WS CLIENT ID', 'SECURITY TYPE DESCRIPTION', 'ASTCLSNAME', 'MKTVALUE'];
const CA_COLS = ['Account Number', 'Available Balance'];

/**
 * Builds the daily tracker using the Z0 Client Master as the source of truth.
 * Z10 contributes Liquid MF balances (Mutual Funds + Cash only).
 * Current Account Data contributes ICICI cash balances.
 * z2Input (optional) is the Z2 Client Master — raw rows or a prebuilt
 * enrichment — used to auto-fill missing RM names and resolve orphans.
 * manualMerges (optional) holds user-saved mappings of orphan balances onto
 * existing clients: { axisNames: { nameKey: wsCode }, accounts: { paddedAccount: wsCode } }.
 */
export function buildDailyTracker(masterRows, z10Rows, caRows, cwRows, masterMeta = {}, z2Input = null, manualMerges = null) {
  const safeMaster = normalizeMasterRows(masterRows);
  const safeZ10 = extractArray(z10Rows);
  const safeCa = extractArray(caRows, ['ICICI Balances', 'ICICI Balance']);
  const cw = extractArray(cwRows);
  const z2 = buildZ2Enrichment(z2Input);

  assertColumns(safeZ10, Z10_COLS, 'Z10');

  // CA files from bank exports often have title rows before the real header.
  // Try to locate the "Account Number" header row dynamically; fall back to
  // the raw rows if they already carry the correct column names.
  const reshapedCa = normalizeCAColumns(reshapeByHeaderRow(safeCa, CA_COLS) ?? safeCa);
  assertColumns(reshapedCa, CA_COLS, 'Current Account Data');

  const warnings = [];
  const exceptions = [];
  const autoResolved = [];
  const { balances: liquidMfBalances, names: z10Names } = buildLiquidMfBalances(safeZ10);
  const { mapping: caMapping, accountInfo: caAccountInfo } = buildCurrentAccountMapping(reshapedCa);
  const { balances: axisBalances, names: axisNames, codes: axisCodes } = buildAxisBalances(cw);

  // Carry the master forward so any auto-added clients (Z10/Z2 orphans) flow
  // into the exported "Updated Z0 Client Master" too. Built inside the master
  // loop so excluded test clients and Z2-filled RM names carry through.
  const finalMasterRows = [];
  const masterCodes = new Set();
  // Tracker row per client code, so saved manual mappings can credit orphan
  // balances onto an existing client's row.
  const rowByCode = new Map();
  const mergeMap = manualMerges || {};
  // Saved manual merges are resolved in ONE pass after every row exists, so a
  // mapping onto a client whose row is created in a later loop still lands.
  const pendingMerges = [];
  // Balances belonging to excluded test clients must not re-enter the totals
  // through the orphan auto-add loops below.
  const excludedAccountKeys = new Set();
  const excludedNameKeys = new Set();
  const excludedCodes = new Set();

  const rows = [];
  let clientsWithoutRm = 0;
  let clientsWithoutIcici = 0;
  let testClientsExcluded = 0;
  let rmFilledFromZ2 = 0;
  const matchedZ10Codes = new Set();
  // Track which ICICI account numbers have already been credited so a single
  // bank account shared across sub-accounts (Main / PMS / AIF) isn't summed
  // multiple times in the dashboard total.
  const creditedAccounts = new Set();
  let duplicateAccountCount = 0;
  // Same idea for Axis: WS Balances often has multiple rows per client
  // (PMS / AIF sub-accounts) but Axis reports one balance per client name.
  const creditedAxisNames = new Set();

  safeMaster.forEach((mRow, index) => {
    const clientCode = str(mRow['WS Client Code']);
    if (!clientCode) {
      // Codeless rows can't be tracked but must survive into the exported master.
      finalMasterRows.push({ ...mRow });
      return;
    }

    const excelRow = index + 2;
    let rmName = str(mRow['RM Name']);
    const rawIciciField = str(mRow['ICICI Acc. No.']);

    // Standing rule: test clients never reach the tracker or its totals —
    // including via the orphan auto-add loops, hence the suppression sets.
    if (isTestRm(rmName) || isTestAccount(rawIciciField)) {
      testClientsExcluded += 1;
      excludedCodes.add(clientCode);
      excludedNameKeys.add(normalizeNameKey(mRow['Client Name']));
      if (!isNoAccountValue(rawIciciField)) {
        const acct = normalizeAccount(mRow['ICICI Acc. No.']);
        if (acct) excludedAccountKeys.add(acct + '|' + (acct.replace(/^0+/, '') || '0'));
      }
      return;
    }

    // Fill missing/Unassigned RM names from Z2 — the two sources never
    // disagree where both exist, so this is safe to auto-apply.
    if ((!rmName || rmName.toLowerCase() === 'unassigned') && z2) {
      const hit = z2.byCode.get(clientCode);
      if (hit && hit.rmName) {
        rmName = hit.rmName;
        rmFilledFromZ2 += 1;
      }
    }

    masterCodes.add(clientCode);
    finalMasterRows.push({ ...mRow, 'RM Name': rmName });

    // 'N/A' means the client intentionally has no ICICI account — skip balance
    // lookup and suppress the "Missing Current Account Balance" exception.
    const isNaAccount = isNoAccountValue(rawIciciField);
    const iciciAccount = isNaAccount ? '' : normalizeAccount(mRow['ICICI Acc. No.']);
    const iciciStripped = iciciAccount.replace(/^0+/, '') || '0';
    const rawBalance = iciciAccount ? (caMapping[iciciAccount] ?? caMapping[iciciStripped] ?? 0) : 0;

    // Only credit the balance to the FIRST master row that uses this account
    // (non-zero either way — a shared negative must not double-count).
    const accountKey = iciciAccount + '|' + iciciStripped;
    const isDuplicateAccount =
      iciciAccount && rawBalance !== 0 && creditedAccounts.has(accountKey);
    const iciciBalance = isDuplicateAccount ? 0 : rawBalance;
    if (iciciAccount && rawBalance !== 0) creditedAccounts.add(accountKey);

    const liquidMf = liquidMfBalances[clientCode] || 0;
    if (clientCode in liquidMfBalances) matchedZ10Codes.add(clientCode);

    const masterNameKey = normalizeNameKey(mRow['Client Name']);
    const rawAxisBalance = masterNameKey ? (axisBalances[masterNameKey] || 0) : 0;
    const axisBalance = creditedAxisNames.has(masterNameKey) ? 0 : rawAxisBalance;
    if (masterNameKey && rawAxisBalance) creditedAxisNames.add(masterNameKey);

    if (isDuplicateAccount) {
      duplicateAccountCount += 1;
      exceptions.push({
        'Issue': 'Duplicate ICICI Account (balance shown on first master row only)',
        'Master Row': excelRow,
        'Source File': masterMeta.fileName || '',
        'Sheet Name': masterMeta.sheetName || '',
        'Client Code': clientCode,
        'Client Name': str(mRow['Client Name']),
        'RM Name': rmName,
        'ICICI Acc. No.': iciciAccount,
        'Liquid MF Balance': '',
      });
    }

    const baseMeta = {
      'Source File': masterMeta.fileName || '',
      'Sheet Name': masterMeta.sheetName || '',
    };

    // 'Unassigned' is a placeholder, not a real RM — surface it as missing
    // so it lands in the exception report instead of hiding in plain sight.
    if (!rmName || rmName.toLowerCase() === 'unassigned') {
      clientsWithoutRm += 1;
      exceptions.push({
        'Issue': 'Missing RM Name',
        'Master Row': excelRow,
        ...baseMeta,
        'Client Code': clientCode,
        'Client Name': str(mRow['Client Name']),
        'RM Name': '',
        'ICICI Acc. No.': iciciAccount,
      });
    }
    if (!isNaAccount && (!iciciAccount || (!(iciciAccount in caMapping) && !(iciciStripped in caMapping)))) {
      const issue = iciciAccount
        ? 'Current Account Not Found In ICICI Balances'
        : 'Missing Current Account Number';
      clientsWithoutIcici += 1;
      exceptions.push({
        'Issue': issue,
        'Master Row': excelRow,
        ...baseMeta,
        'Client Code': clientCode,
        'Client Name': str(mRow['Client Name']),
        'RM Name': rmName,
        'ICICI Acc. No.': iciciAccount,
      });
    }

    const totalLiquidity = iciciBalance + liquidMf + axisBalance;

    const trackerRow = {
      'RM Name': rmName,
      'WS Client Code': clientCode,
      'Client Name': str(mRow['Client Name']),
      'ICICI Acc. No.': isNaAccount ? 'N/A' : iciciAccount,
      'ICICI Bank Balance': iciciBalance,
      'Liquid MF Balance': liquidMf,
      'Axis Demat Balance': axisBalance,
      'Total Liquidity': totalLiquidity,
    };
    rows.push(trackerRow);
    if (!rowByCode.has(clientCode)) rowByCode.set(clientCode, trackerRow);
  });

  // Auto-add Z10 clients whose code has no Master record — they would
  // otherwise vanish from the dashboard total. When Z2 knows the client the
  // row is completed from it silently; otherwise it appears as an orphan row
  // (blank RM, blank ICICI account) flagged in the exception report.
  let orphanLiquidMf = 0;
  for (const [code, bal] of Object.entries(liquidMfBalances)) {
    if (matchedZ10Codes.has(code)) continue;
    if (excludedCodes.has(code)) continue;
    masterCodes.add(code);
    const z2Hit = z2 ? z2.byCode.get(code) : null;
    if (!(z2Hit && z2Hit.rmName)) orphanLiquidMf += bal;
    const clientName = z10Names[code] || z2Hit?.name || '';
    const rmName = z2Hit?.rmName || '';
    const orphanRow = {
      'RM Name': rmName,
      'WS Client Code': code,
      'Client Name': clientName,
      'ICICI Acc. No.': '',
      'ICICI Bank Balance': 0,
      'Liquid MF Balance': bal,
      'Axis Demat Balance': 0,
      'Total Liquidity': bal,
    };
    rows.push(orphanRow);
    if (!rowByCode.has(code)) rowByCode.set(code, orphanRow);
    finalMasterRows.push({
      'WS Client Code': code,
      'Client Name': clientName,
      'RM Name': rmName,
      // NONE (not '') so an Apply-Fixes rebuild doesn't resurrect this row
      // as a "Missing Current Account Number" exception.
      'ICICI Acc. No.': 'NONE',
      Status: z2Hit ? 'New (from Z10, RM via Z2)' : 'New (from Z10)',
    });
    if (z2Hit && rmName) {
      autoResolved.push({
        'Type': 'Liquid MF holding without master record',
        'Client Code': code,
        'Client Name': clientName,
        'RM Name': rmName,
        'ICICI Acc. No.': '',
        'Amount': bal,
        'Resolved From': 'Z2 Client Master',
      });
    } else {
      exceptions.push({
        'Issue': 'Liquid MF Holding Without Master Record (auto-added)',
        'Master Row': '',
        'Source File': masterMeta.fileName || '',
        'Sheet Name': masterMeta.sheetName || '',
        'Client Code': code,
        'Client Name': clientName,
        'RM Name': '',
        'ICICI Acc. No.': '',
        'Liquid MF Balance': bal,
      });
    }
  }

  // Auto-add ICICI accounts with a balance that aren't on any Master row.
  // Without this they'd be silently dropped, making the dashboard total
  // diverge from the sum of the bank's Effective Balance column.
  // Onza's own corporate account is kept as a normal row (standing rule);
  // accounts Z2 can map to a client are completed silently.
  let orphanIciciCash = 0;
  for (const [paddedAcct, info] of Object.entries(caAccountInfo)) {
    const stripped = paddedAcct.replace(/^0+/, '') || '0';
    if (creditedAccounts.has(paddedAcct + '|' + stripped)) continue;
    if (excludedAccountKeys.has(paddedAcct + '|' + stripped)) continue;
    if (isTestAccount(paddedAcct)) continue;
    if (!info.balance) continue;

    if (paddedAcct === ONZA_CORPORATE_ACCOUNT) {
      rows.push({
        'RM Name': ONZA_CORPORATE_RM,
        'WS Client Code': '',
        'Client Name': info.accountName || 'Onza Financial Services',
        'ICICI Acc. No.': paddedAcct,
        'ICICI Bank Balance': info.balance,
        'Liquid MF Balance': 0,
        'Axis Demat Balance': 0,
        'Total Liquidity': info.balance,
      });
      autoResolved.push({
        'Type': "Onza's own current account (standing rule)",
        'Client Code': '',
        'Client Name': info.accountName,
        'RM Name': ONZA_CORPORATE_RM,
        'ICICI Acc. No.': paddedAcct,
        'Amount': info.balance,
        'Resolved From': 'Standing rule',
      });
      continue;
    }

    // Saved manual mapping: defer to the post-loop pass so the target row is
    // guaranteed to exist regardless of which loop created it.
    if (mergeMap.accounts?.[paddedAcct]) {
      pendingMerges.push({
        kind: 'icici',
        target: mergeMap.accounts[paddedAcct],
        balance: info.balance,
        account: paddedAcct,
        accountName: info.accountName,
      });
      continue;
    }

    const z2Hit = z2 ? z2.byAccount.get(paddedAcct) : null;
    if (z2Hit && z2Hit.code && !masterCodes.has(z2Hit.code)) {
      masterCodes.add(z2Hit.code);
      // Credit the client's Axis balance here too, so the Axis-orphan loop
      // doesn't raise a separate exception for a client Z2 fully identifies.
      const resolvedNameKey = normalizeNameKey(z2Hit.name || info.accountName);
      let resolvedAxis = 0;
      if (resolvedNameKey && !creditedAxisNames.has(resolvedNameKey) && axisBalances[resolvedNameKey]) {
        resolvedAxis = axisBalances[resolvedNameKey];
        creditedAxisNames.add(resolvedNameKey);
      }
      const z2Row = {
        'RM Name': z2Hit.rmName,
        'WS Client Code': z2Hit.code,
        'Client Name': z2Hit.name || info.accountName,
        'ICICI Acc. No.': paddedAcct,
        'ICICI Bank Balance': info.balance,
        'Liquid MF Balance': 0,
        'Axis Demat Balance': resolvedAxis,
        'Total Liquidity': info.balance + resolvedAxis,
      };
      rows.push(z2Row);
      if (!rowByCode.has(z2Hit.code)) rowByCode.set(z2Hit.code, z2Row);
      finalMasterRows.push({
        'WS Client Code': z2Hit.code,
        'Client Name': z2Hit.name || info.accountName,
        'RM Name': z2Hit.rmName,
        'ICICI Acc. No.': stripped,
        Status: 'New (from Z2)',
      });
      autoResolved.push({
        'Type': 'ICICI balance without master record',
        'Client Code': z2Hit.code,
        'Client Name': z2Hit.name || info.accountName,
        'RM Name': z2Hit.rmName,
        'ICICI Acc. No.': paddedAcct,
        'Amount': info.balance,
        'Resolved From': 'Z2 Client Master',
      });
      continue;
    }

    orphanIciciCash += info.balance;
    rows.push({
      'RM Name': '',
      'WS Client Code': '',
      'Client Name': info.accountName,
      'ICICI Acc. No.': paddedAcct,
      'ICICI Bank Balance': info.balance,
      'Liquid MF Balance': 0,
      'Axis Demat Balance': 0,
      'Total Liquidity': info.balance,
    });
    exceptions.push({
      'Issue': 'ICICI Balance Without Master Record (auto-added)',
      'Master Row': '',
      'Source File': masterMeta.fileName || '',
      'Sheet Name': masterMeta.sheetName || '',
      'Client Code': '',
      'Client Name': info.accountName,
      'RM Name': '',
      'ICICI Acc. No.': paddedAcct,
      'Liquid MF Balance': '',
    });
  }

  // Auto-add Axis clients whose normalized name didn't match any Master row.
  // Without this they'd be missing from the per-client table even though they
  // contribute to the dashboard's Total Axis Balance. Names Z2 recognizes as
  // clients not yet in the master are completed silently.
  let orphanAxisCash = 0;
  for (const [nameKey, bal] of Object.entries(axisBalances)) {
    if (creditedAxisNames.has(nameKey)) continue;
    if (excludedNameKeys.has(nameKey)) continue;
    if (!bal) continue;
    const displayName = axisNames[nameKey] || nameKey;
    const axisCode = axisCodes[nameKey] || '';

    // Saved manual mapping: defer to the post-loop pass (see ICICI loop).
    if (mergeMap.axisNames?.[nameKey]) {
      pendingMerges.push({
        kind: 'axis',
        target: mergeMap.axisNames[nameKey],
        balance: bal,
        displayName,
        axisCode,
      });
      continue;
    }

    const z2Hit = z2 ? z2.byName.get(nameKey) : null;
    if (z2Hit && z2Hit.code && !masterCodes.has(z2Hit.code)) {
      masterCodes.add(z2Hit.code);
      const z2AxisRow = {
        'RM Name': z2Hit.rmName,
        'WS Client Code': z2Hit.code,
        'Client Name': z2Hit.name || displayName,
        'ICICI Acc. No.': '',
        'ICICI Bank Balance': 0,
        'Liquid MF Balance': 0,
        'Axis Demat Balance': bal,
        'Total Liquidity': bal,
      };
      rows.push(z2AxisRow);
      if (!rowByCode.has(z2Hit.code)) rowByCode.set(z2Hit.code, z2AxisRow);
      finalMasterRows.push({
        'WS Client Code': z2Hit.code,
        'Client Name': z2Hit.name || displayName,
        'RM Name': z2Hit.rmName,
        // NONE (not '') so an Apply-Fixes rebuild doesn't resurrect this row
        // as a "Missing Current Account Number" exception.
        'ICICI Acc. No.': 'NONE',
        Status: 'New (from Z2)',
      });
      autoResolved.push({
        'Type': 'Axis Demat balance without master record',
        'Client Code': z2Hit.code,
        'Client Name': z2Hit.name || displayName,
        'RM Name': z2Hit.rmName,
        'ICICI Acc. No.': '',
        'Amount': bal,
        'Resolved From': 'Z2 Client Master',
      });
      continue;
    }

    orphanAxisCash += bal;
    rows.push({
      'RM Name': '',
      'WS Client Code': '',
      'Client Name': displayName,
      'ICICI Acc. No.': '',
      'ICICI Bank Balance': 0,
      'Liquid MF Balance': 0,
      'Axis Demat Balance': bal,
      'Total Liquidity': bal,
    });
    exceptions.push({
      'Issue': 'Axis Demat Balance Without Master Record (auto-added)',
      'Master Row': '',
      'Source File': masterMeta.fileName || '',
      'Sheet Name': masterMeta.sheetName || '',
      'Client Code': axisCode,
      'Client Name': displayName,
      'RM Name': '',
      'ICICI Acc. No.': '',
      'Liquid MF Balance': '',
    });
  }

  // Resolve saved manual merges now that every tracker row exists. When the
  // target still exists, fold the orphan balance in; otherwise the mapping is
  // stale (target removed) — fall back to the normal orphan row + exception so
  // the money still appears and totals stay conserved.
  for (const p of pendingMerges) {
    const targetRow = rowByCode.get(p.target);
    if (targetRow && p.kind === 'icici') {
      targetRow['ICICI Bank Balance'] += p.balance;
      targetRow['Total Liquidity'] += p.balance;
      autoResolved.push({
        'Type': 'ICICI balance merged into existing client',
        'Client Code': p.target, 'Client Name': targetRow['Client Name'], 'RM Name': targetRow['RM Name'],
        'ICICI Acc. No.': p.account, 'Amount': p.balance, 'Resolved From': 'Saved manual mapping',
      });
    } else if (targetRow && p.kind === 'axis') {
      targetRow['Axis Demat Balance'] += p.balance;
      targetRow['Total Liquidity'] += p.balance;
      autoResolved.push({
        'Type': 'Axis Demat balance merged into existing client',
        'Client Code': p.target, 'Client Name': targetRow['Client Name'], 'RM Name': targetRow['RM Name'],
        'ICICI Acc. No.': '', 'Amount': p.balance, 'Resolved From': 'Saved manual mapping',
      });
    } else if (p.kind === 'icici') {
      // Stale mapping — target gone. Emit as a normal ICICI orphan.
      orphanIciciCash += p.balance;
      rows.push({
        'RM Name': '', 'WS Client Code': '', 'Client Name': p.accountName, 'ICICI Acc. No.': p.account,
        'ICICI Bank Balance': p.balance, 'Liquid MF Balance': 0, 'Axis Demat Balance': 0, 'Total Liquidity': p.balance,
      });
      exceptions.push({
        'Issue': 'ICICI Balance Without Master Record (auto-added)', 'Master Row': '',
        'Source File': masterMeta.fileName || '', 'Sheet Name': masterMeta.sheetName || '',
        'Client Code': '', 'Client Name': p.accountName, 'RM Name': '', 'ICICI Acc. No.': p.account, 'Liquid MF Balance': '',
      });
    } else {
      // Stale Axis mapping — emit as a normal Axis orphan.
      orphanAxisCash += p.balance;
      rows.push({
        'RM Name': '', 'WS Client Code': '', 'Client Name': p.displayName, 'ICICI Acc. No.': '',
        'ICICI Bank Balance': 0, 'Liquid MF Balance': 0, 'Axis Demat Balance': p.balance, 'Total Liquidity': p.balance,
      });
      exceptions.push({
        'Issue': 'Axis Demat Balance Without Master Record (auto-added)', 'Master Row': '',
        'Source File': masterMeta.fileName || '', 'Sheet Name': masterMeta.sheetName || '',
        'Client Code': p.axisCode, 'Client Name': p.displayName, 'RM Name': '', 'ICICI Acc. No.': '', 'Liquid MF Balance': '',
      });
    }
  }

  rows.sort((a, b) => {
    const rmA = a['RM Name'];
    const rmB = b['RM Name'];
    if (rmA === '' && rmB !== '') return 1;
    if (rmB === '' && rmA !== '') return -1;
    if (rmA !== rmB) return rmA.localeCompare(rmB);
    return b['Total Liquidity'] - a['Total Liquidity'];
  });

  // Now that unmatched Axis clients are auto-added as orphan rows, summing
  // the per-row column equals SUM(E2:E) of the Client Wise file.
  const axisTotal = rows.reduce((s, r) => s + (r['Axis Demat Balance'] || 0), 0);

  const summary = {
    totalClients: rows.length,
    totalRMs: new Set(rows.map((r) => r['RM Name']).filter(Boolean)).size,
    totalIcici: rows.reduce((s, r) => s + r['ICICI Bank Balance'], 0),
    totalLiquidMf: rows.reduce((s, r) => s + r['Liquid MF Balance'], 0),
    axisTotal,
    totalLiquidity: rows.reduce((s, r) => s + r['Total Liquidity'], 0),
    orphanLiquidMf,
    orphanIciciCash,
    orphanAxisCash,
  };

  if (testClientsExcluded > 0) {
    warnings.push(
      `Excluded ${testClientsExcluded} test client row(s) (RM "Test RM" / dummy account) from the tracker and totals.`
    );
  }
  const z2ResolvedCount = autoResolved.filter((a) => a['Resolved From'] === 'Z2 Client Master').length;
  const manualMergeCount = autoResolved.filter((a) => a['Resolved From'] === 'Saved manual mapping').length;
  if (z2 && (rmFilledFromZ2 > 0 || z2ResolvedCount > 0)) {
    warnings.push(
      `Z2 Client Master: auto-filled ${rmFilledFromZ2} RM name(s) and resolved ` +
      `${z2ResolvedCount} unmatched record(s) — no manual fixing needed for these.`
    );
  }
  if (manualMergeCount > 0) {
    warnings.push(
      `${manualMergeCount} orphan balance(s) merged into existing clients via saved mappings.`
    );
  }
  if (!z2 && (clientsWithoutRm > 0 || orphanIciciCash > 0 || orphanLiquidMf > 0 || Math.abs(orphanAxisCash) > 0.5)) {
    warnings.push(
      'Tip: also upload the Z2 Client Master export — it auto-fills missing RM names and unmatched accounts.'
    );
  }
  if (clientsWithoutRm > 0) {
    warnings.push(`${clientsWithoutRm} client(s) in the Master have no RM Name.`);
  }
  if (clientsWithoutIcici > 0) {
    warnings.push(`${clientsWithoutIcici} client(s) had no matching Current Account balance.`);
  }
  if (orphanLiquidMf > 0) {
    const formatted = orphanLiquidMf.toLocaleString('en-IN', {
      style: 'currency', currency: 'INR', maximumFractionDigits: 0,
    });
    warnings.push(
      `${formatted} of Liquid MF holdings belong to client codes not in the Master ` +
      `(auto-added as orphan rows). See exception report.`
    );
  }
  if (duplicateAccountCount > 0) {
    warnings.push(
      `${duplicateAccountCount} master row(s) share an ICICI account with another ` +
      `record; balance is credited to the first row only to avoid double-counting.`
    );
  }
  if (orphanIciciCash > 0) {
    const formatted = orphanIciciCash.toLocaleString('en-IN', {
      style: 'currency', currency: 'INR', maximumFractionDigits: 0,
    });
    warnings.push(
      `${formatted} of ICICI cash sits in accounts not mapped to any Master row ` +
      `(auto-added as orphan rows). See exception report.`
    );
  }
  if (Math.abs(orphanAxisCash) > 0.5) {
    const formatted = orphanAxisCash.toLocaleString('en-IN', {
      style: 'currency', currency: 'INR', maximumFractionDigits: 0,
    });
    warnings.push(
      `${formatted} of Axis Demat balance is from client names that don't match any ` +
      `Master record (auto-added as orphan rows). See exception report.`
    );
  }

  return {
    rows,
    summary,
    warnings,
    exceptions,
    autoResolved,
    finalMasterRows,
    stats: { testClientsExcluded, rmFilledFromZ2 },
    generatedAt: new Date(),
  };
}

// -----------------------------------------------------------------------------
// Dynamic Master — built from WS Balances rows
// -----------------------------------------------------------------------------

/**
 * Builds the Client Master from WS Balances rows (Bank Recon export).
 * Bank account leading zeros are stripped so the account matches the
 * Current Account Data file exactly.
 */
export function buildDynamicMaster(wsBalancesRows, z10Rows) {
  const ws = extractArray(wsBalancesRows);
  const z10 = extractArray(z10Rows);

  // 1. Build a lookup dictionary for RM Names from Z10 (header casing varies
  //    between Wealth Spectrum exports, so reads are case-insensitive).
  const rmLookup = {};
  z10.forEach((row) => {
    // Clean the code in case Z10 reads it as a float (e.g., "100001.0")
    const code = str(getField(row, 'WS ACCOUNT CODE', 'WS CLIENT ID')).replace(/\.0+$/, '');
    const rmName = str(getField(row, 'RMNAME', 'RM NAME'));

    if (code && rmName && !rmLookup[code]) {
      rmLookup[code] = rmName;
    }
  });

  // 2. Build the master, pulling the RM name from the lookup
  const dynamicMaster = [];
  ws.forEach((r) => {
    const code = str(getField(r, 'CLIENT_CODE'));
    if (!code) return;

    const rawBank = str(getField(r, 'BANK_ACCOUNT'));
    const bankCode = str(getField(r, 'BANK_CODE', 'BANKNAME')).toUpperCase();
    const cleanBank = rawBank.replace(/^0+/, '');

    // ICICI current accounts are always 12-digit. A row whose bank code names
    // another bank (SBI, …), or whose bank code is a NONE placeholder with a
    // non-12-digit internal ledger code (501009xx), can never match the ICICI
    // portal export — classify it N/A instead of raising a false
    // "not found in ICICI" exception. Blank bank codes keep legacy behavior.
    const digits = rawBank.replace(/\D/g, '');
    const bankCodeSaysOther =
      bankCode !== '' && !isNoAccountValue(bankCode) && !bankCode.includes('ICICI');
    const nonePlaceholderLedger =
      bankCode !== '' && isNoAccountValue(bankCode) && digits.length !== 12;
    const nonIcici = bankCodeSaysOther || nonePlaceholderLedger;

    dynamicMaster.push({
      'WS Client Code': code,
      'Client Name': getField(r, 'CLIENTNAME') || 'Unknown',
      'RM Name': rmLookup[code] || 'Unassigned',
      'ICICI Acc. No.':
        !rawBank || isNoAccountValue(rawBank) || !cleanBank
          ? 'NONE'
          : nonIcici
            ? 'N/A'
            : cleanBank,
    });
  });

  return { rows: dynamicMaster };
}

// -----------------------------------------------------------------------------
// Manual client merge
// -----------------------------------------------------------------------------

/**
 * Merges the tracker rows of manually selected clients into one row —
 * used when the same person appears under mismatched names across bank and
 * demat files (e.g. "R JAGADISH" vs "Ramamoorthi Jagadish").
 *
 * selectedGroups is an array, in selection order, of row-object arrays taken
 * from result.rows BY REFERENCE. The first group's first row is the primary:
 * it keeps its RM Name, WS Client Code, Client Name, and ICICI Acc. No.;
 * the financial columns become the sum across every selected row. All other
 * selected rows are removed. Returns a new result with rows and the
 * client/RM summary counts updated (money totals are unchanged by a merge).
 */
export function mergeTrackerRows(result, selectedGroups) {
  const flat = (selectedGroups || []).flat();
  if (!result || flat.length < 2) return result;

  const primaryRow = selectedGroups[0][0];
  const sum = (col) => flat.reduce((s, r) => s + (r[col] || 0), 0);
  const mergedRow = {
    ...primaryRow,
    'ICICI Bank Balance': sum('ICICI Bank Balance'),
    'Liquid MF Balance': sum('Liquid MF Balance'),
    'Axis Demat Balance': sum('Axis Demat Balance'),
    'Total Liquidity': sum('Total Liquidity'),
  };

  const dropped = new Set(flat);
  const rows = result.rows
    .map((r) => (r === primaryRow ? mergedRow : r))
    .filter((r) => r === mergedRow || !dropped.has(r));

  return {
    ...result,
    rows,
    summary: {
      ...result.summary,
      totalClients: rows.length,
      totalRMs: new Set(rows.map((r) => r['RM Name']).filter(Boolean)).size,
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
  'Axis Demat Balance',
  'Total Liquidity',
];

const DAILY_COL_WIDTHS = [
  { wch: 24 }, { wch: 16 }, { wch: 36 }, { wch: 18 },
  { wch: 18 }, { wch: 18 }, { wch: 20 }, { wch: 18 },
];

const MASTER_HEADER = MASTER_COLS;
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
    { Metric: 'Total Axis Balance', Value: summary.axisTotal },
    { Metric: 'Total Liquidity', Value: summary.totalLiquidity },
    { Metric: 'Generated At', Value: new Date().toLocaleString('en-IN') },
  ];
}

function rmSummaryRows(rmName, rmRows) {
  const totalIcici = rmRows.reduce((s, r) => s + r['ICICI Bank Balance'], 0);
  const totalLiquidMf = rmRows.reduce((s, r) => s + r['Liquid MF Balance'], 0);
  const totalAxis = rmRows.reduce((s, r) => s + (r['Axis Demat Balance'] || 0), 0);
  return [
    { Metric: 'RM Name', Value: rmName },
    { Metric: 'Clients', Value: rmRows.length },
    { Metric: 'Total ICICI Balance', Value: totalIcici },
    { Metric: 'Total Liquid MF Balance', Value: totalLiquidMf },
    { Metric: 'Total Axis Balance', Value: totalAxis },
    { Metric: 'Total Liquidity', Value: totalIcici + totalLiquidMf + totalAxis },
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

const EXCEPTIONS_HEADER = ['Issue', 'Master Row', 'Source File', 'Sheet Name', 'Client Code', 'Client Name', 'RM Name', 'ICICI Acc. No.', 'Liquid MF Balance'];
const EXCEPTIONS_COL_WIDTHS = [{ wch: 36 }, { wch: 12 }, { wch: 30 }, { wch: 20 }, { wch: 16 }, { wch: 36 }, { wch: 28 }, { wch: 20 }, { wch: 18 }];

function appendReportInfoSheet(wb, generatedAt) {
  const infoSheet = XLSX.utils.json_to_sheet([
    { Field: 'Generated At', Value: (generatedAt || new Date()).toLocaleString('en-IN', { dateStyle: 'full', timeStyle: 'short' }) },
    { Field: 'Generated By', Value: 'Onza Liquidity Tracker' },
  ]);
  infoSheet['!cols'] = [{ wch: 20 }, { wch: 40 }];
  XLSX.utils.book_append_sheet(wb, infoSheet, 'Report Info');
}

export function exportExceptionsToXlsx(exceptions, generatedAt, autoResolved = []) {
  const wb = XLSX.utils.book_new();
  const sheet = XLSX.utils.json_to_sheet(exceptions, { header: EXCEPTIONS_HEADER });
  sheet['!cols'] = EXCEPTIONS_COL_WIDTHS;
  XLSX.utils.book_append_sheet(wb, sheet, 'Exceptions');
  if (autoResolved.length > 0) {
    const resolvedSheet = XLSX.utils.json_to_sheet(autoResolved, {
      header: ['Type', 'Client Code', 'Client Name', 'RM Name', 'ICICI Acc. No.', 'Amount', 'Resolved From'],
    });
    resolvedSheet['!cols'] = [{ wch: 40 }, { wch: 14 }, { wch: 36 }, { wch: 24 }, { wch: 18 }, { wch: 16 }, { wch: 20 }];
    XLSX.utils.book_append_sheet(wb, resolvedSheet, 'Auto-Resolved');
  }
  appendReportInfoSheet(wb, generatedAt);
  return XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
}

/**
 * Single-workbook export of the Z0 Client Master.
 */
export function exportMasterToXlsx(rows) {
  const wb = XLSX.utils.book_new();

  const sheet = XLSX.utils.json_to_sheet(rows, { header: MASTER_HEADER });
  sheet['!cols'] = MASTER_COL_WIDTHS;
  XLSX.utils.book_append_sheet(wb, sheet, 'Z0 Client Master');

  return XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
}
