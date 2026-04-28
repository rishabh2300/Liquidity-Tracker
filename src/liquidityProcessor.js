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

    const clientId = str(getField(row, 'WS CLIENT ID', 'WS client id'));
    if (!clientId) continue;
    balances[clientId] = (balances[clientId] || 0) + toNumber(getField(row, 'MKTVALUE'));
    if (!names[clientId]) {
      names[clientId] = str(getField(row, 'CLIENT NAME', 'Client Name'));
    }
  }
  return { balances, names };
}

function buildCurrentAccountMapping(caRows) {
  const mapping = {};
  // Per-account metadata so orphan rows can show the bank's account name.
  const accountInfo = {};

  for (const row of caRows) {
    const raw = str(row['Account Number'] ?? row['ACCOUNT_NUMBER'] ?? row['Account No'] ?? '');
    if (!raw) continue;

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
    const stripped = raw.replace(/^0+/, '') || '0';

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

const MASTER_COLS = ['WS Client Code', 'Client Name', 'RM Name', 'ICICI Acc. No.'];

/**
 * Bank recon files use different column names for account/balance and often
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
 * Builds the daily tracker using the Client Master as the source of truth.
 * Z10 contributes Liquid MF balances (Mutual Funds + Cash only).
 * Current Account Data contributes ICICI cash balances.
 */
export function buildDailyTracker(masterRows, z10Rows, caRows, masterMeta = {}) {
  const safeMaster = extractArray(masterRows);
  const safeZ10 = extractArray(z10Rows);
  const safeCa = extractArray(caRows, ['ICICI Balances', 'ICICI Balance']);

  assertColumns(safeMaster, MASTER_COLS, 'Client Master');
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

  // Carry the master forward so any auto-added clients (Z10 orphans) flow
  // into the exported "Updated Client Master" too.
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
    const iciciAccount = normalizeAccount(mRow['ICICI Acc. No.']);
    const iciciStripped = iciciAccount.replace(/^0+/, '') || '0';
    const rawBalance = caMapping[iciciAccount] ?? caMapping[iciciStripped] ?? 0;

    // Only credit the balance to the FIRST master row that uses this account.
    const accountKey = iciciAccount + '|' + iciciStripped;
    const isDuplicateAccount =
      iciciAccount && rawBalance > 0 && creditedAccounts.has(accountKey);
    const iciciBalance = isDuplicateAccount ? 0 : rawBalance;
    if (iciciAccount && rawBalance > 0) creditedAccounts.add(accountKey);

    const liquidMf = liquidMfBalances[clientCode] || 0;
    if (clientCode in liquidMfBalances) matchedZ10Codes.add(clientCode);

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
    if (!iciciAccount || (!(iciciAccount in caMapping) && !(iciciStripped in caMapping))) {
      clientsWithoutIcici += 1;
      exceptions.push({
        'Issue': 'Missing Current Account Balance',
        'Master Row': excelRow,
        ...baseMeta,
        'Client Code': clientCode,
        'Client Name': str(mRow['Client Name']),
        'RM Name': rmName,
        'ICICI Acc. No.': iciciAccount,
      });
    }

    rows.push({
      'RM Name': rmName,
      'WS Client Code': clientCode,
      'Client Name': str(mRow['Client Name']),
      'ICICI Acc. No.': iciciAccount,
      'ICICI Bank Balance': iciciBalance,
      'Liquid MF Balance': liquidMf,
      'Total Liquidity': iciciBalance + liquidMf,
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

  const summary = {
    totalClients: rows.length,
    totalRMs: new Set(rows.map((r) => r['RM Name']).filter(Boolean)).size,
    totalIcici: rows.reduce((s, r) => s + r['ICICI Bank Balance'], 0),
    totalLiquidMf: rows.reduce((s, r) => s + r['Liquid MF Balance'], 0),
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
// Tab 2 — Update Master (diff against Bank Recon)
// -----------------------------------------------------------------------------

const RECON_COLS_V1 = ['Bank Code', 'Client Name', 'Bank Account'];
const RECON_COLS_V2 = ['CLIENT_CODE', 'CLIENTNAME', 'BANK_ACCOUNT'];

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
  const safeRecon = extractArray(reconRows, [
    'WS Balances', 'WS Balance', 'Bank Recon 150426',
  ]);

  assertColumns(safeMaster, MASTER_COLS, 'Client Master');

  // Accept either the legacy header format (Bank Code / Client Name / Bank Account)
  // or the export format (CLIENT_CODE / CLIENTNAME / BANK_ACCOUNT).
  const presentKeys = safeRecon.length > 0 ? Object.keys(safeRecon[0]) : [];
  const presentLower = new Set(presentKeys.map((k) => k.toLowerCase()));
  const usesV2Headers = presentLower.has('client_code');
  assertColumns(safeRecon, usesV2Headers ? RECON_COLS_V2 : RECON_COLS_V1, 'Bank Recon');

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

  // Pre-pass: count occurrences of each Bank Code to detect duplicates.
  const reconCodeCount = new Map();
  for (const r of safeRecon) {
    const code = str(getField(r, 'Bank Code', 'CLIENT_CODE'));
    if (!code) continue;
    reconCodeCount.set(code, (reconCodeCount.get(code) || 0) + 1);
  }

  let added = 0;
  let updated = 0;
  const mappingReport = [];

  for (const r of safeRecon) {
    const code = str(getField(r, 'Bank Code', 'CLIENT_CODE'));
    const reconAccount = normalizeAccount(getField(r, 'Bank Account', 'BANK_ACCOUNT'));
    const reconName = str(getField(r, 'Client Name', 'CLIENTNAME'));

    const reportRow = {
      'Recon Client Code': code,
      'Recon Client Name': reconName,
      'Recon Bank Account': reconAccount,
      'Mapping Status': '',
      'Action Taken': '',
      'Detailed Note': '',
    };

    if (!code) {
      reportRow['Mapping Status'] = 'Error: Missing Client Code';
      reportRow['Action Taken'] = 'Skipped';
      reportRow['Detailed Note'] = 'Row has no client code — cannot map to Master';
      mappingReport.push(reportRow);
      continue;
    }

    const count = reconCodeCount.get(code);
    if (count > 1) {
      reportRow['Mapping Status'] = 'Error: Duplicate Client Code';
      reportRow['Action Taken'] = 'Skipped — Duplicate';
      reportRow['Detailed Note'] = `Client Code appears ${count} times in the Recon file; manual review required`;
      mappingReport.push(reportRow);
      continue;
    }

    if (!byCode.has(code)) {
      byCode.set(code, {
        'WS Client Code': code,
        'Client Name': reconName,
        'RM Name': '',
        'ICICI Acc. No.': reconAccount,
        Status: 'New',
      });
      added += 1;
      reportRow['Mapping Status'] = 'Unmapped';
      reportRow['Action Taken'] = 'Added to Master as New';
      reportRow['Detailed Note'] = 'Client Code not found in existing Master; added with blank RM Name';
      mappingReport.push(reportRow);
      continue;
    }

    const existing = byCode.get(code);
    if (reconAccount && existing['ICICI Acc. No.'] !== reconAccount) {
      const oldAccount = existing['ICICI Acc. No.'] || '(blank)';
      existing['ICICI Acc. No.'] = reconAccount;
      existing.Status = 'Updated';
      updated += 1;
      reportRow['Mapping Status'] = 'Mapped';
      reportRow['Action Taken'] = 'Updated Existing Master Account';
      reportRow['Detailed Note'] = `Account changed from ${oldAccount} to ${reconAccount}`;
    } else {
      reportRow['Mapping Status'] = 'Mapped';
      reportRow['Action Taken'] = 'No Change (Already up-to-date)';
      reportRow['Detailed Note'] = reconAccount
        ? 'Recon account matches existing Master — no update needed'
        : 'No account number in Recon row; existing Master value kept';
    }
    mappingReport.push(reportRow);
  }

  const rows = Array.from(byCode.values()).sort((a, b) =>
    a['WS Client Code'].localeCompare(b['WS Client Code'])
  );

  return {
    rows,
    mappingReport,
    generatedAt: new Date(),
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

const MAPPING_REPORT_HEADER = [
  'Recon Client Code',
  'Recon Client Name',
  'Recon Bank Account',
  'Mapping Status',
  'Action Taken',
  'Detailed Note',
];

const MAPPING_REPORT_COL_WIDTHS = [
  { wch: 20 }, { wch: 36 }, { wch: 20 }, { wch: 30 }, { wch: 36 }, { wch: 60 },
];

/**
 * Single-workbook export of the detailed mapping report.
 */
export function exportMappingReportToXlsx(mappingReport, generatedAt) {
  const wb = XLSX.utils.book_new();
  const sheet = XLSX.utils.json_to_sheet(mappingReport, { header: MAPPING_REPORT_HEADER });
  sheet['!cols'] = MAPPING_REPORT_COL_WIDTHS;
  XLSX.utils.book_append_sheet(wb, sheet, 'Mapping Report');
  appendReportInfoSheet(wb, generatedAt);
  return XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
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
