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
  for (const row of cwRows) {
    const clientName = str(getField(row, 'Client Name', 'CLIENT NAME'));
    if (!clientName) continue;
    const key = clientName.toUpperCase();
    const bal = toNumber(getField(row, 'Ledger Bal BOD', 'LEDGER BAL BOD'));
    balances[key] = (balances[key] || 0) + bal;
  }
  return balances;
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
 */
export function buildDailyTracker(masterRows, z10Rows, caRows, cwRows, masterMeta = {}) {
  const safeMaster = normalizeMasterRows(masterRows);
  const safeZ10 = extractArray(z10Rows);
  const safeCa = extractArray(caRows, ['ICICI Balances', 'ICICI Balance']);
  const cw = extractArray(cwRows);

  assertColumns(safeZ10, Z10_COLS, 'Z10');

  // CA files from bank exports often have title rows before the real header.
  // Try to locate the "Account Number" header row dynamically; fall back to
  // the raw rows if they already carry the correct column names.
  const reshapedCa = normalizeCAColumns(reshapeByHeaderRow(safeCa, CA_COLS) ?? safeCa);
  assertColumns(reshapedCa, CA_COLS, 'Current Account Data');

  const warnings = [];
  const exceptions = [];
  const { balances: liquidMfBalances, names: z10Names } = buildLiquidMfBalances(safeZ10);
  const { mapping: caMapping, accountInfo: caAccountInfo } = buildCurrentAccountMapping(reshapedCa);
  const axisBalances = buildAxisBalances(cw);

  // Carry the master forward so any auto-added clients (Z10 orphans) flow
  // into the exported "Updated Z0 Client Master" too.
  const finalMasterRows = safeMaster.map((r) => ({ ...r }));

  const rows = [];
  let clientsWithoutRm = 0;
  let clientsWithoutIcici = 0;
  const matchedZ10Codes = new Set();
  // Track which ICICI account numbers have already been credited so a single
  // bank account shared across sub-accounts (Main / PMS / AIF) isn't summed
  // multiple times in the dashboard total.
  const creditedAccounts = new Set();
  let duplicateAccountCount = 0;

  safeMaster.forEach((mRow, index) => {
    const clientCode = str(mRow['WS Client Code']);
    if (!clientCode) return;

    const excelRow = index + 2;
    const rmName = str(mRow['RM Name']);
    // 'N/A' means the client intentionally has no ICICI account — skip balance
    // lookup and suppress the "Missing Current Account Balance" exception.
    const rawIciciField = str(mRow['ICICI Acc. No.']);
    const isNaAccount = isNoAccountValue(rawIciciField);
    const iciciAccount = isNaAccount ? '' : normalizeAccount(mRow['ICICI Acc. No.']);
    const iciciStripped = iciciAccount.replace(/^0+/, '') || '0';
    const rawBalance = iciciAccount ? (caMapping[iciciAccount] ?? caMapping[iciciStripped] ?? 0) : 0;

    // Only credit the balance to the FIRST master row that uses this account.
    const accountKey = iciciAccount + '|' + iciciStripped;
    const isDuplicateAccount =
      iciciAccount && rawBalance > 0 && creditedAccounts.has(accountKey);
    const iciciBalance = isDuplicateAccount ? 0 : rawBalance;
    if (iciciAccount && rawBalance > 0) creditedAccounts.add(accountKey);

    const liquidMf = liquidMfBalances[clientCode] || 0;
    if (clientCode in liquidMfBalances) matchedZ10Codes.add(clientCode);

    const masterNameKey = str(mRow['Client Name']).toUpperCase();
    const axisBalance = masterNameKey ? (axisBalances[masterNameKey] || 0) : 0;

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

    if (!rmName) {
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

    rows.push({
      'RM Name': rmName,
      'WS Client Code': clientCode,
      'Client Name': str(mRow['Client Name']),
      'ICICI Acc. No.': isNaAccount ? 'N/A' : iciciAccount,
      'ICICI Bank Balance': iciciBalance,
      'Liquid MF Balance': liquidMf,
      'Axis Demat Balance': axisBalance,
      'Total Liquidity': totalLiquidity,
    });
  });

  // Auto-add Z10 clients whose code has no Master record — they would
  // otherwise vanish from the dashboard total. They appear as orphan rows
  // (blank RM, blank ICICI account) and are flagged in the exception report.
  let orphanLiquidMf = 0;
  for (const [code, bal] of Object.entries(liquidMfBalances)) {
    if (matchedZ10Codes.has(code)) continue;
    orphanLiquidMf += bal;
    const clientName = z10Names[code] || '';
    rows.push({
      'RM Name': '',
      'WS Client Code': code,
      'Client Name': clientName,
      'ICICI Acc. No.': '',
      'ICICI Bank Balance': 0,
      'Liquid MF Balance': bal,
      'Axis Demat Balance': 0,
      'Total Liquidity': bal,
    });
    finalMasterRows.push({
      'WS Client Code': code,
      'Client Name': clientName,
      'RM Name': '',
      'ICICI Acc. No.': '',
      Status: 'New (from Z10)',
    });
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

  // Auto-add ICICI accounts with a balance that aren't on any Master row.
  // Without this they'd be silently dropped, making the dashboard total
  // diverge from the sum of the bank's Effective Balance column.
  let orphanIciciCash = 0;
  for (const [paddedAcct, info] of Object.entries(caAccountInfo)) {
    const stripped = paddedAcct.replace(/^0+/, '') || '0';
    if (creditedAccounts.has(paddedAcct + '|' + stripped)) continue;
    if (!info.balance) continue;
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

  rows.sort((a, b) => {
    const rmA = a['RM Name'];
    const rmB = b['RM Name'];
    if (rmA === '' && rmB !== '') return 1;
    if (rmB === '' && rmA !== '') return -1;
    if (rmA !== rmB) return rmA.localeCompare(rmB);
    return b['Total Liquidity'] - a['Total Liquidity'];
  });

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
  };

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

  return {
    rows,
    summary,
    warnings,
    exceptions,
    finalMasterRows,
    generatedAt: new Date(),
  };
}

// -----------------------------------------------------------------------------
// Dynamic Master — built directly from Z0 rows
// -----------------------------------------------------------------------------

/**
 * Builds the Client Master from Z0 rows alone. Bank account leading zeros are
 * stripped so the account matches the Current Account Data file exactly.
 */
export function buildDynamicMaster(z0Rows) {
  const z0 = extractArray(z0Rows);
  const dynamicMaster = [];

  z0.forEach((r) => {
    const code = String(r['CLIENTCODE'] || r['CLIENT CODE'] || '').trim();
    if (!code) return;

    const rawBank = String(r['CLIENT BANK ACCOUNT'] || r['BANK ACCOUNT'] || '').trim();
    const cleanBank = rawBank.replace(/^0+/, '');

    dynamicMaster.push({
      'WS Client Code': code,
      'Client Name': r['CLIENTNAME'] || r['CLIENT NAME'] || 'Unknown',
      'RM Name': r['RMNAME'] || r['RM NAME'] || 'Unassigned',
      'ICICI Acc. No.': cleanBank || 'NONE',
    });
  });

  return { rows: dynamicMaster };
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

export function exportExceptionsToXlsx(exceptions, generatedAt) {
  const wb = XLSX.utils.book_new();
  const sheet = XLSX.utils.json_to_sheet(exceptions, { header: EXCEPTIONS_HEADER });
  sheet['!cols'] = EXCEPTIONS_COL_WIDTHS;
  XLSX.utils.book_append_sheet(wb, sheet, 'Exceptions');
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
