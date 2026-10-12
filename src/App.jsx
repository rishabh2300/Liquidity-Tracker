import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  parseFile,
  parseWorkbookGrids,
  classifyWorkbook,
  buildDailyTracker,
  buildDynamicMaster,
  exportDailyTrackerZip,
  exportMasterToXlsx,
  exportExceptionsToXlsx,
  mergeTrackerRows,
  normalizeAccount,
  normalizeNameKey,
} from './liquidityProcessor';

// localStorage key for orphan→client merge mappings saved from the fixer.
const MANUAL_MERGES_KEY = 'onza-lt-manual-merges';
const EMPTY_MANUAL_MERGES = { axisNames: {}, accounts: {} };

function loadManualMerges() {
  try {
    const parsed = JSON.parse(localStorage.getItem(MANUAL_MERGES_KEY));
    return {
      axisNames: { ...(parsed?.axisNames || {}) },
      accounts: { ...(parsed?.accounts || {}) },
    };
  } catch {
    return { ...EMPTY_MANUAL_MERGES, axisNames: {}, accounts: {} };
  }
}

function saveManualMerges(merges) {
  try {
    localStorage.setItem(MANUAL_MERGES_KEY, JSON.stringify(merges));
  } catch {
    // Storage unavailable (private mode) — mappings still apply this session.
  }
}

const str = (v) => (v === null || v === undefined ? '' : String(v).trim());
import { saveAs } from 'file-saver';
import logo from './assets/gold_logo.png';
import './App.css';

// Upload roles — each dropped file is identified by its CONTENTS
// (classifyWorkbook), never by file name or sheet order.
const ROLE_META = [
  {
    key: 'recon',
    label: 'Bank Recon',
    required: true,
    hint: 'WS Balances + ICICI Balances — sheets found automatically',
  },
  {
    key: 'z10',
    label: 'Z10 Holdings',
    required: true,
    hint: 'Raw holdings table found automatically (pivot sheets ignored)',
  },
  {
    key: 'cw',
    label: 'Client Wise (Axis)',
    required: true,
    hint: 'Ledger Bal BOD per Axis client',
  },
  {
    key: 'z2',
    label: 'Z2 Client Master',
    required: false,
    hint: 'Optional — auto-fills RM names & unmatched accounts',
  },
  {
    key: 'ca',
    label: 'Current Account Data',
    required: false,
    hint: "Optional — raw ICICI portal export; its balances override the Bank Recon's ICICI Balances sheet",
  },
];

const INR = new Intl.NumberFormat('en-IN', {
  style: 'currency',
  currency: 'INR',
  maximumFractionDigits: 0,
});

export default function App() {
  // Files identified by content, keyed by role: { recon, z10, cw, z2, ca },
  // each entry { file, cls } where cls carries the extracted table rows.
  const [uploads, setUploads] = useState({});
  const [fileNotes, setFileNotes] = useState([]);
  const [classifying, setClassifying] = useState(false);
  const [result, setResult] = useState(null);
  const [masterResult, setMasterResult] = useState(null);
  const [lastGeneratedAt, setLastGeneratedAt] = useState(null);
  const [error, setError] = useState('');
  const [processing, setProcessing] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [rmFilter, setRmFilter] = useState('all');
  const [search, setSearch] = useState('');
  // Parsed source rows retained so "Apply Fixes" can rebuild the tracker
  // without re-reading the original files.
  const [parsedSources, setParsedSources] = useState(null);
  // User-entered fixes for exception rows, keyed by exception index in
  // result.exceptions. Cleared whenever a new build runs.
  const [exceptionFixes, setExceptionFixes] = useState({});
  const [applyingFixes, setApplyingFixes] = useState(false);
  // Group keys ticked for a manual merge, in click order — the FIRST ticked
  // client keeps its identity. Cleared whenever a new result is built.
  const [mergeSelection, setMergeSelection] = useState([]);
  // Saved orphan→client mappings (Axis names / ICICI accounts), persisted in
  // localStorage and applied on every build.
  const [manualMerges, setManualMerges] = useState(loadManualMerges);
  const fixerRef = useRef(null);
  const dropInputRef = useRef(null);
  // Mirror of `uploads` that async handlers can read/write without racing on
  // stale closures (two rapid drops must both see each other's files).
  const uploadsRef = useRef({});

  const allReady = Boolean(uploads.recon && uploads.z10 && uploads.cw);

  const runBuild = (u, mergesOverride = null) => {
    if (!u.recon || !u.z10 || !u.cw) return;
    setProcessing(true);
    setError('');
    try {
      const { wsRows, caRows: reconCaRows, wsSheet } = u.recon.cls;
      const z10Rows = u.z10.cls.z10Rows;
      const cwRows = u.cw.cls.cwRows;
      const z2Rows = u.z2 ? u.z2.cls.z2Rows : null;
      // A separately uploaded Current Account export (the raw ICICI portal
      // table) overrides the recon's ICICI Balances sheet as the cash source,
      // so the tracker can run on today's balances before the recon is done.
      const caUpload = u.ca || null;
      const caRows = caUpload ? caUpload.cls.caRows : reconCaRows;

      const masterResult = buildDynamicMaster(wsRows, z10Rows);
      const masterMeta = {
        fileName: u.recon.file.name,
        sheetName: wsSheet || 'WS Balances',
        asOnDate: masterResult.asOnDate,
        reconBalanceDate: u.recon.cls.balanceDate || '',
        ...(caUpload
          ? {
              currentAccount: {
                fileName: caUpload.file.name,
                sheetName: caUpload.cls.sheet || '',
                exportedAt: caUpload.cls.exportedAt || '',
              },
            }
          : {}),
      };
      const dailyTrackerResult = buildDailyTracker(
        masterResult.rows,
        z10Rows,
        caRows,
        cwRows,
        masterMeta,
        z2Rows,
        mergesOverride ?? manualMerges
      );

      setLastGeneratedAt((prev) => result?.generatedAt ?? prev);
      setMasterResult(masterResult);
      setResult(dailyTrackerResult);
      setParsedSources({ z10Rows, caRows, cwRows, masterMeta, z2Rows });
      setExceptionFixes({});
    } catch (err) {
      setError(err.message || String(err));
      setResult(null);
      setMasterResult(null);
      setParsedSources(null);
      setExceptionFixes({});
    } finally {
      setProcessing(false);
    }
  };

  // Parses and identifies every dropped file by content, then builds the
  // tracker automatically once the three required reports are present.
  const handleFilesAdded = async (fileList) => {
    const list = Array.from(fileList || []);
    if (!list.length) return;
    setClassifying(true);
    setError('');
    const notes = [];
    const additions = {};
    try {
      for (const file of list) {
        try {
          const parsed = await parseWorkbookGrids(file);
          const cls = classifyWorkbook(parsed);
          if (['recon', 'z10', 'cw', 'z2', 'ca'].includes(cls.role)) {
            const displaced = additions[cls.role] || uploadsRef.current[cls.role];
            if (displaced && (displaced.file.name !== file.name || displaced.file.size !== file.size)) {
              const label = ROLE_META.find((r) => r.key === cls.role)?.label || cls.role;
              notes.push({
                name: file.name,
                level: 'warn',
                message: `Also recognized as ${label} — it replaced "${displaced.file.name}". Remove one via the role card if that's wrong.`,
              });
            }
            additions[cls.role] = { file, cls };
          } else if (cls.role === 'clhr') {
            notes.push({
              name: file.name,
              level: 'info',
              message: 'Recognized as Client Level Holding Report — not needed for the tracker, ignored.',
            });
          } else {
            notes.push({
              name: file.name,
              level: 'warn',
              message:
                cls.hint ||
                'Not recognized. Expected: Bank Recon, Z10 Holdings, Client Wise (Axis), Z2 Client Master, or Current Account Data.',
            });
          }
        } catch (err) {
          notes.push({ name: file.name, level: 'error', message: err.message || String(err) });
        }
      }
      // Merge through the ref so concurrent drops both land, then only
      // auto-build when this drop actually contributed something new —
      // an unrecognized file must not wipe in-progress exception fixes.
      uploadsRef.current = { ...uploadsRef.current, ...additions };
      const merged = uploadsRef.current;
      setUploads(merged);
      setFileNotes(notes);
      if (Object.keys(additions).length > 0 && merged.recon && merged.z10 && merged.cw) {
        runBuild(merged);
      }
    } finally {
      setClassifying(false);
    }
  };

  const removeUpload = (key) => {
    const next = { ...uploadsRef.current };
    delete next[key];
    uploadsRef.current = next;
    setUploads(next);
    setFileNotes([]);
    setError('');
    // Removing an optional file (Z2 / Current Account Data) leaves the three
    // required reports in place — rebuild on them right away, so dropping the
    // Current Account override falls straight back to the recon's own sheet.
    if (next.recon && next.z10 && next.cw) {
      runBuild(next);
      return;
    }
    setResult(null);
    setMasterResult(null);
    setParsedSources(null);
    setExceptionFixes({});
  };

  const handleReset = () => {
    uploadsRef.current = {};
    setUploads({});
    setFileNotes([]);
    setResult(null);
    setMasterResult(null);
    setParsedSources(null);
    setExceptionFixes({});
    setError('');
    setRmFilter('all');
    setSearch('');
  };

  const updateFix = (idx, field, value) => {
    setExceptionFixes((prev) => ({
      ...prev,
      [idx]: { ...prev[idx], [field]: value },
    }));
  };

  const fixableExceptions = useMemo(() => {
    if (!result?.exceptions) return [];
    return result.exceptions
      .map((exc, idx) => ({ exc, idx }))
      .filter(({ exc }) => exc['Issue'] !== 'Duplicate ICICI Account (balance shown on first master row only)');
  }, [result]);

  const handleApplyFixes = () => {
    if (!result || !parsedSources) return;
    setApplyingFixes(true);
    setError('');
    try {
      // Start from the latest finalMasterRows (already includes Z10 orphans
      // auto-added during the previous run) and drop the Status field.
      const masterByCode = new Map();
      const extraRows = []; // rows without a code (rare; preserve as-is)
      for (const r of result.finalMasterRows) {
        const code = String(r['WS Client Code'] || '').trim();
        const cleaned = {
          'WS Client Code': code,
          'Client Name': r['Client Name'] || '',
          'RM Name': r['RM Name'] || '',
          'ICICI Acc. No.': r['ICICI Acc. No.'] || '',
        };
        if (code) masterByCode.set(code, cleaned);
        else extraRows.push(cleaned);
      }

      // Apply user-entered fixes onto the master.
      result.exceptions.forEach((exc, idx) => {
        const fix = exceptionFixes[idx];
        if (!fix) return;
        const issue = exc['Issue'];
        const excCode = String(exc['Client Code'] || '').trim();

        if (issue === 'ICICI Balance Without Master Record (auto-added)') {
          const newCode = String(fix.clientCode || '').trim();
          if (!newCode) return; // need a code to add to master
          masterByCode.set(newCode, {
            'WS Client Code': newCode,
            'Client Name': String(fix.clientName || exc['Client Name'] || '').trim(),
            'RM Name': String(fix.rmName || '').trim(),
            'ICICI Acc. No.': exc['ICICI Acc. No.'] || '',
          });
          return;
        }

        if (excCode && masterByCode.has(excCode)) {
          const existing = masterByCode.get(excCode);
          if (fix.rmName !== undefined && fix.rmName !== '') {
            existing['RM Name'] = String(fix.rmName).trim();
          }
          if (fix.iciciAccount !== undefined && fix.iciciAccount !== '') {
            existing['ICICI Acc. No.'] = String(fix.iciciAccount).trim();
          }
        }
      });

      const patchedMaster = [...masterByCode.values(), ...extraRows];

      const dailyTrackerResult = buildDailyTracker(
        patchedMaster,
        parsedSources.z10Rows,
        parsedSources.caRows,
        parsedSources.cwRows,
        parsedSources.masterMeta,
        parsedSources.z2Rows,
        manualMerges
      );

      setResult(dailyTrackerResult);
      setExceptionFixes({});
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setApplyingFixes(false);
    }
  };

  // Reads a corrected Exception Report (.xlsx) and pre-fills the fixer table
  // from it, so exceptions can be fixed in Excel instead of typed in one by one.
  const handleUploadFixedExcel = async (e) => {
    const file = e.target.files?.[0];
    e.target.value = ''; // allow re-selecting the same file later
    if (!file || !result?.exceptions?.length) return;
    setError('');
    try {
      const data = await parseFile(file);
      const uploadedRows = Array.isArray(data)
        ? data
        : data['Exceptions'] || data[Object.keys(data)[0]] || [];
      if (uploadedRows.length === 0) {
        throw new Error(
          `No rows found in "${file.name}". Upload the corrected Exception Report (.xlsx).`
        );
      }

      const str = (v) => String(v ?? '').trim();
      // Uploaded rows indexed by client code, normalized ICICI account, and
      // uppercased client name. Account/name cover rows with no client code
      // (e.g. auto-added ICICI balances, where the code is what's being fixed).
      const byCode = new Map();
      const byAccount = new Map();
      const byName = new Map();
      for (const row of uploadedRows) {
        const code = str(row['Client Code']);
        const acct = normalizeAccount(row['ICICI Acc. No.']);
        const nameKey = str(row['Client Name']).toUpperCase();
        if (code && !byCode.has(code)) byCode.set(code, row);
        if (acct && acct !== 'N/A' && !byAccount.has(acct)) byAccount.set(acct, row);
        if (nameKey && !byName.has(nameKey)) byName.set(nameKey, row);
      }

      let matched = 0;
      const loadedFixes = {};
      result.exceptions.forEach((exc, idx) => {
        const allow = FIXABLE_FIELDS[exc['Issue']];
        if (!allow) return;
        const excCode = str(exc['Client Code']);
        const excAcct = normalizeAccount(exc['ICICI Acc. No.']);
        const excName = str(exc['Client Name']).toUpperCase();
        const row =
          (excCode && byCode.get(excCode)) ||
          (excAcct && excAcct !== 'N/A' && byAccount.get(excAcct)) ||
          (excName && byName.get(excName)) ||
          null;
        if (!row) return;
        matched += 1;

        // Only pick up fields this issue allows, and only where the uploaded
        // value actually differs from what the exception already had.
        const fix = {};
        if (allow.clientCode && str(row['Client Code']) && str(row['Client Code']) !== excCode) {
          fix.clientCode = str(row['Client Code']);
        }
        if (allow.clientName && str(row['Client Name']) && str(row['Client Name']) !== str(exc['Client Name'])) {
          fix.clientName = str(row['Client Name']);
        }
        if (allow.rmName && str(row['RM Name']) && str(row['RM Name']) !== str(exc['RM Name'])) {
          fix.rmName = str(row['RM Name']);
        }
        const rowAcct = normalizeAccount(row['ICICI Acc. No.']);
        if (allow.iciciAccount && rowAcct && rowAcct !== excAcct) {
          fix.iciciAccount = rowAcct;
        }
        if (Object.keys(fix).length > 0) loadedFixes[idx] = fix;
      });

      if (matched === 0) {
        throw new Error(
          `No rows in "${file.name}" matched the current exceptions. ` +
            'Make sure you uploaded the corrected Exception Report for this run.'
        );
      }

      const fixCount = Object.keys(loadedFixes).length;
      setExceptionFixes((prev) => {
        const merged = { ...prev };
        for (const [idx, fix] of Object.entries(loadedFixes)) {
          merged[idx] = { ...merged[idx], ...fix };
        }
        return merged;
      });
      fixerRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      setTimeout(() => {
        alert(
          fixCount > 0
            ? `✅ Fixes successfully loaded from Excel! ${fixCount} row${fixCount === 1 ? '' : 's'} populated — review the table, then click "Apply fixes & rebuild".`
            : `Matched ${matched} row${matched === 1 ? '' : 's'} in "${file.name}", but no new values were found to apply.`
        );
      }, 300);
    } catch (err) {
      setError(err.message || String(err));
    }
  };

  const handleExportZip = async () => {
    if (!result) return;
    setExporting(true);
    try {
      const blob = await exportDailyTrackerZip(result.rows, result.summary);
      const today = new Date().toISOString().slice(0, 10);
      saveAs(blob, `Onza_Liquidity_Tracker_${today}.zip`);
      setTimeout(() => {
        alert('✅ Download Complete! Your Liquidity Tracker ZIP has been saved to your downloads folder.');
      }, 500);
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setExporting(false);
    }
  };

  const handleExportMaster = () => {
    if (!masterResult) return;
    // Prefer the daily-tracker's finalMasterRows so Z10-only clients
    // (auto-added during the pipeline) are included in the export.
    const sourceRows = result?.finalMasterRows ?? masterResult.rows;
    const exportRows = sourceRows.map(({ Status, ...rest }) => rest);
    const buffer = exportMasterToXlsx(exportRows);
    const blob = new Blob([buffer], {
      type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    });
    const today = new Date().toISOString().slice(0, 10);
    saveAs(blob, `Z0_ClientMaster_${today}.xlsx`);
    setTimeout(() => {
      alert('✅ Download Complete! Your updated Z0 Client Master has been saved to your downloads folder.');
    }, 500);
  };

  const handleDownloadExceptions = () => {
    if (!result?.exceptions?.length && !result?.autoResolved?.length) return;
    const buffer = exportExceptionsToXlsx(result.exceptions, result.generatedAt, result.autoResolved ?? []);
    const blob = new Blob([buffer], {
      type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    });
    saveAs(blob, 'Daily_Exceptions.xlsx');
    setTimeout(() => {
      alert('✅ Download Complete! Your Exception Report has been saved to your downloads folder.');
    }, 500);
  };

  const rmOptions = useMemo(() => {
    if (!result) return [];
    return Array.from(new Set(result.rows.map((r) => r['RM Name']).filter(Boolean))).sort();
  }, [result]);

  // If a merge/rebuild removes the last row for the filtered RM, that RM
  // vanishes from rmOptions — reset the filter so the table doesn't silently
  // empty behind a select stuck on a value that no longer exists.
  useEffect(() => {
    if (rmFilter !== 'all' && !rmOptions.includes(rmFilter)) setRmFilter('all');
  }, [rmOptions, rmFilter]);

  const filteredRows = useMemo(() => {
    if (!result) return [];
    const q = search.trim().toLowerCase();
    return result.rows.filter((r) => {
      if (rmFilter !== 'all' && r['RM Name'] !== rmFilter) return false;
      if (!q) return true;
      return (
        r['Client Name'].toLowerCase().includes(q) ||
        r['WS Client Code'].toLowerCase().includes(q) ||
        r['ICICI Acc. No.'].toLowerCase().includes(q)
      );
    });
  }, [result, rmFilter, search]);

  const filteredTotal = useMemo(
    () => filteredRows.reduce((s, r) => s + r['Total Liquidity'], 0),
    [filteredRows]
  );
  const displayEntries = useMemo(() => buildClientDisplayEntries(filteredRows), [filteredRows]);
  const allDisplayEntries = useMemo(
    () => buildClientDisplayEntries(result?.rows ?? []),
    [result]
  );

  // Any new/merged result invalidates the ticked rows.
  useEffect(() => {
    setMergeSelection([]);
  }, [result]);

  const toggleMergeSelect = (key) => {
    setMergeSelection((prev) =>
      prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]
    );
  };

  const handleMergeSelected = () => {
    if (!result || mergeSelection.length < 2) return;
    const entriesByKey = new Map(allDisplayEntries.map((e) => [e.key, e]));
    const selected = mergeSelection.map((k) => entriesByKey.get(k)).filter(Boolean);
    if (selected.length < 2) return;

    const primary = selected[0];
    // mergeTrackerRows keeps the first ticked group's FIRST underlying row —
    // quote that exact row (not the group's synthetic display aggregate) so
    // the dialog matches what actually survives.
    const survivor = primary.rows[0];
    const recordCount = selected.reduce((s, e) => s + e.rows.length, 0);
    const names = selected.map((e) => e.row['Client Name'] || '(blank name)');
    const survivorLabel = `${survivor['Client Name'] || '(blank name)'}` +
      (survivor['WS Client Code'] ? ` (code ${survivor['WS Client Code']}` : ' (') +
      (survivor['RM Name'] ? `, RM ${survivor['RM Name']})` : ')');
    const ok = window.confirm(
      `Merge ${recordCount} record(s):\n\n- ${names.join('\n- ')}\n\n` +
        `${survivorLabel} keeps its name, code, RM and account; ` +
        'balances are added together and the other rows are removed.\n\n' +
        'Merges apply to the current build only — rebuilding from files starts fresh.'
    );
    if (!ok) return;

    setResult(mergeTrackerRows(result, selected.map((e) => e.rows)));
  };

  // Existing clients (with a WS code) an orphan exception can be merged into.
  const mergeTargets = useMemo(() => {
    if (!result) return [];
    const seen = new Map();
    for (const r of result.rows) {
      const code = str(r['WS Client Code']);
      if (code && !seen.has(code)) {
        seen.set(code, `${r['Client Name'] || '(no name)'} — ${code}`);
      }
    }
    return Array.from(seen, ([code, label]) => ({ code, label })).sort((a, b) =>
      a.label.localeCompare(b.label)
    );
  }, [result]);

  // Rebuild the tracker from the current (already-patched) master, re-applying
  // the given saved merge mappings. Used when a mapping is added or removed.
  const rebuildWithMerges = (merges) => {
    if (!result || !parsedSources) return;
    setError('');
    try {
      const master = result.finalMasterRows.map(({ Status, ...r }) => r);
      const rebuilt = buildDailyTracker(
        master,
        parsedSources.z10Rows,
        parsedSources.caRows,
        parsedSources.cwRows,
        parsedSources.masterMeta,
        parsedSources.z2Rows,
        merges
      );
      setResult(rebuilt);
      // Resolving a merge removes an exception and shifts the positional
      // indexes the fixer keys typed fixes by — clear them, as the other
      // rebuild paths do, so no typed fix lands on the wrong client.
      setExceptionFixes({});
    } catch (err) {
      setError(err.message || String(err));
    }
  };

  // Maps an orphan exception's balance onto an existing client, saves the
  // mapping (localStorage, applied on every future build), and rebuilds.
  const saveOrphanMerge = (exc, targetCode) => {
    const code = str(targetCode);
    if (!code) return;
    const next = {
      axisNames: { ...manualMerges.axisNames },
      accounts: { ...manualMerges.accounts },
    };
    const issue = exc['Issue'];
    if (issue.startsWith('Axis Demat Balance Without Master Record')) {
      const key = normalizeNameKey(exc['Client Name']);
      if (!key) return;
      next.axisNames[key] = code;
    } else if (issue.startsWith('ICICI Balance Without Master Record')) {
      const key = normalizeAccount(exc['ICICI Acc. No.']);
      if (!key || key === 'N/A') return;
      next.accounts[key] = code;
    } else {
      return;
    }
    setManualMerges(next);
    saveManualMerges(next);
    rebuildWithMerges(next);
  };

  const clearOrphanMerges = () => {
    if (!window.confirm('Clear all saved orphan→client mappings on this computer?')) return;
    const empty = { axisNames: {}, accounts: {} };
    setManualMerges(empty);
    saveManualMerges(empty);
    rebuildWithMerges(empty);
  };

  const savedMergeCount =
    Object.keys(manualMerges.axisNames).length + Object.keys(manualMerges.accounts).length;

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-inner">
          <div className="brand">
            <img src={logo} alt="Onza" className="brand-logo" />
            <div>
              <h1 className="app-title">Liquidity Tracker</h1>
              <p className="app-subtitle">
                One-click pipeline: build daily liquidity tracker from Z0 master and bank data
              </p>
            </div>
          </div>
          <div className="date-chip">
            {new Date().toLocaleDateString('en-IN', {
              weekday: 'long',
              day: 'numeric',
              month: 'long',
              year: 'numeric',
            })}{' '}
            &bull;{' '}
            {new Date().toLocaleTimeString('en-IN', {
              hour: '2-digit',
              minute: '2-digit',
            })}
          </div>
        </div>
      </header>

      <main className="app-main">
        <section className="panel">
          <div className="panel-header">
            <span className="panel-label">Step 01</span>
            <h2 className="panel-title">Upload source files</h2>
          </div>

          <div
            className="dropzone"
            onDragOver={(e) => e.preventDefault()}
            onDrop={(e) => {
              e.preventDefault();
              handleFilesAdded(e.dataTransfer.files);
            }}
            onClick={() => dropInputRef.current?.click()}
          >
            <input
              ref={dropInputRef}
              type="file"
              multiple
              accept=".csv,.xlsx,.xlsm,.xls"
              style={{ display: 'none' }}
              onChange={(e) => {
                handleFilesAdded(e.target.files);
                e.target.value = '';
              }}
            />
            <p className="dropzone-title">
              {classifying ? 'Reading files…' : 'Drop all daily files here — or click to browse'}
            </p>
            <p className="dropzone-hint">
              Any file names, any order. Each file is identified by its contents, and the
              tracker builds itself once the three required reports are in — adding or
              removing an optional file rebuilds it.
            </p>
          </div>

          <div className="file-grid file-grid-5">
            {ROLE_META.map((role) => (
              <RoleCard
                key={role.key}
                role={role}
                upload={uploads[role.key]}
                onClear={() => removeUpload(role.key)}
              />
            ))}
          </div>

          {fileNotes.length > 0 && (
            <div className="alert alert-notes">
              {fileNotes.map((n, i) => (
                <div key={i}>
                  <strong>{n.name}:</strong> {n.message}
                </div>
              ))}
            </div>
          )}

          <div className="actions" style={{ marginTop: '16px' }}>
            <button
              className="btn btn-primary"
              onClick={() => runBuild(uploads)}
              disabled={!allReady || processing || classifying}
            >
              {processing ? 'Building tracker…' : 'Rebuild tracker'}
            </button>
            <button
              className="btn btn-secondary"
              onClick={handleReset}
              disabled={processing}
            >
              Reset
            </button>
          </div>

          {lastGeneratedAt && (
            <p className="panel-meta" style={{ marginTop: '12px' }}>
              Last report generated:{' '}
              {lastGeneratedAt.toLocaleString('en-IN', {
                dateStyle: 'full',
                timeStyle: 'short',
              })}
            </p>
          )}

          {error && <div className="alert alert-error">{error}</div>}
          {result?.warnings?.length > 0 && (
            <div className="alert alert-info">
              <strong>Build notes</strong>
              <ul className="notes-list">
                {result.warnings.map((w, i) => (
                  <li key={i}>{w}</li>
                ))}
              </ul>
            </div>
          )}
          {(result?.exceptions?.length > 0 || result?.autoResolved?.length > 0) && (
            <div
              className={`alert ${result.exceptions.length > 0 ? 'alert-warn' : 'alert-info'}`}
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                marginTop: '1rem',
              }}
            >
              <div>
                {result.exceptions.length > 0 ? (
                  <>
                    <strong>⚠ Exceptions Found:</strong> {result.exceptions.length} records have
                    missing mapping data (e.g., missing RM Name or Bank Account Number).
                    {result.autoResolved?.length > 0 && (
                      <> Another {result.autoResolved.length} were auto-resolved — see the report.</>
                    )}
                  </>
                ) : (
                  <>
                    <strong>✓ No exceptions.</strong> {result.autoResolved.length} unmatched
                    record(s) were auto-resolved (Z2 / standing rules) — no action needed.
                  </>
                )}
              </div>
              <div className="actions">
                {fixableExceptions.length > 0 && (
                  <button
                    className="btn btn-primary"
                    onClick={() =>
                      fixerRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
                    }
                  >
                    Fix them
                  </button>
                )}
                <button className="btn btn-secondary" onClick={handleDownloadExceptions}>
                  Download Exception Report
                </button>
              </div>
            </div>
          )}
        </section>

        {result && (
          <>
            <section className="summary-row">
              <SummaryCard
                label="Clients"
                value={allDisplayEntries.length.toLocaleString('en-IN')}
              />
              <SummaryCard
                label="Client Partners"
                value={result.summary.totalRMs.toLocaleString('en-IN')}
              />
              <SummaryCard label="ICICI Cash" value={INR.format(result.summary.totalIcici)} />
              <SummaryCard
                label="Liquid MFs"
                value={INR.format(result.summary.totalLiquidMf)}
              />
              <SummaryCard
                label="Total Axis Balance"
                value={INR.format(result.summary.axisTotal)}
              />
              <SummaryCard
                label="Total Liquidity"
                value={INR.format(result.summary.totalLiquidity)}
                emphasis
              />
            </section>

            <section className="panel">
              <div className="panel-header panel-header-split">
                <div>
                  <span className="panel-label">Step 02</span>
                  <h2 className="panel-title">Consolidated tracker</h2>
                </div>
                <div className="actions">
                  <button
                    className="btn btn-primary"
                    onClick={handleExportZip}
                    disabled={exporting}
                  >
                    {exporting ? 'Packaging ZIP…' : 'Export Liquidity ZIP (RM Wise)'}
                  </button>
                  <button className="btn btn-primary" onClick={handleExportMaster}>
                    Export Updated Z0 Client Master
                  </button>
                </div>
              </div>

              <div className="filters">
                <label className="filter">
                  <span className="filter-label">Filter by RM</span>
                  <select value={rmFilter} onChange={(e) => setRmFilter(e.target.value)}>
                    <option value="all">All Client Partners</option>
                    {rmOptions.map((rm) => (
                      <option key={rm} value={rm}>
                        {rm}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="filter filter-search">
                  <span className="filter-label">Search</span>
                  <input
                    type="search"
                    placeholder="Client name, code, or account"
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                  />
                </label>
                <div className="filter-meta">
                  Showing <strong>{displayEntries.length.toLocaleString('en-IN')}</strong> of{' '}
                  {allDisplayEntries.length.toLocaleString('en-IN')} clients from{' '}
                  {filteredRows.length.toLocaleString('en-IN')} records · Subtotal{' '}
                  <strong>{INR.format(filteredTotal)}</strong>
                </div>
                <button
                  className="btn btn-secondary"
                  onClick={handleMergeSelected}
                  disabled={mergeSelection.length < 2}
                  title="Tick two or more rows — the first ticked client keeps its name, code, RM and account"
                >
                  Merge Selected Clients
                  {mergeSelection.length > 0 ? ` (${mergeSelection.length})` : ''}
                </button>
              </div>

              <DailyTrackerTable
                entries={displayEntries}
                mergeSelection={mergeSelection}
                onToggleMergeSelect={toggleMergeSelect}
              />
            </section>

            {fixableExceptions.length > 0 && (
              <ExceptionFixerSection
                ref={fixerRef}
                fixableExceptions={fixableExceptions}
                fixes={exceptionFixes}
                onChange={updateFix}
                onApply={handleApplyFixes}
                onUploadFixed={handleUploadFixedExcel}
                applying={applyingFixes}
                rmOptions={rmOptions}
                mergeTargets={mergeTargets}
                manualMerges={manualMerges}
                onSaveMerge={saveOrphanMerge}
                savedMergeCount={savedMergeCount}
                onClearMerges={clearOrphanMerges}
              />
            )}
          </>
        )}

        {!result && !error && (
          <section className="panel panel-empty">
            <p className="empty-title">Ready to build</p>
            <p className="empty-body">
              Drop the daily files above — Bank Recon, Z10 Holdings, and Client Wise
              (plus the optional Z2 Client Master for automatic RM mapping, and the optional
              Current Account Data export to use today's ICICI balances instead of the
              recon's sheet). Each file is recognized by its contents and the tracker builds
              itself; no naming, ordering, or slot-picking needed.
            </p>
          </section>
        )}
      </main>

      <footer className="app-footer">
        <span>Onza · Internal tool · Processes run locally on this device</span>
      </footer>
    </div>
  );
}

// -----------------------------------------------------------------------------
// Sub-components
// -----------------------------------------------------------------------------

function RoleCard({ role, upload, onClear }) {
  return (
    <div className={`file-slot ${upload ? 'file-slot-filled' : ''}`}>
      <div className="file-slot-header">
        <span className="file-slot-label">
          {role.label}
          {!role.required && <span className="role-optional"> · optional</span>}
        </span>
        {upload && (
          <button className="file-slot-clear" onClick={onClear} aria-label="Remove file">
            ×
          </button>
        )}
      </div>
      <p className="file-slot-hint">{role.hint}</p>

      {upload ? (
        <div className="file-slot-file">
          <span className="file-name">{upload.file.name}</span>
          <span className="file-size">
            {upload.cls?.exportedAt
              ? `exported ${upload.cls.exportedAt.split(' ')[0]}`
              : `${(upload.file.size / 1024).toFixed(1)} KB`}
          </span>
        </div>
      ) : (
        <p className="file-slot-waiting">
          {role.required ? 'Waiting for file…' : 'Not uploaded'}
        </p>
      )}
    </div>
  );
}

function SummaryCard({ label, value, emphasis }) {
  return (
    <div className={`summary-card ${emphasis ? 'summary-card-emphasis' : ''}`}>
      <span className="summary-label">{label}</span>
      <span className="summary-value">{value}</span>
    </div>
  );
}

const FIXABLE_FIELDS = {
  'Missing RM Name': { rmName: true },
  'Missing Current Account Balance': { iciciAccount: true },
  'Missing Current Account Number': { iciciAccount: true },
  'Current Account Not Found In ICICI Balances': { iciciAccount: true },
  'Liquid MF Holding Without Master Record (auto-added)': { rmName: true, iciciAccount: true },
  'ICICI Balance Without Master Record (auto-added)': {
    clientCode: true, clientName: true, rmName: true,
  },
};

const MERGEABLE_ISSUE_PREFIXES = [
  'Axis Demat Balance Without Master Record',
  'ICICI Balance Without Master Record',
];
const isMergeableIssue = (issue) =>
  MERGEABLE_ISSUE_PREFIXES.some((p) => String(issue).startsWith(p));

const ExceptionFixerSection = React.forwardRef(function ExceptionFixerSection(
  {
    fixableExceptions, fixes, onChange, onApply, onUploadFixed, applying, rmOptions,
    mergeTargets = [], onSaveMerge = () => {}, savedMergeCount = 0, onClearMerges = () => {},
  },
  ref
) {
  const uploadInputRef = useRef(null);
  const filledCount = Object.values(fixes).filter((f) =>
    Object.values(f || {}).some((v) => String(v ?? '').trim() !== '')
  ).length;

  return (
    <section className="panel" ref={ref}>
      <div className="panel-header panel-header-split">
        <div>
          <span className="panel-label">Optional</span>
          <h2 className="panel-title">Fix exceptions and rebuild</h2>
          <p className="panel-meta" style={{ marginTop: 6 }}>
            Fill in the missing details below — or upload the corrected Exception
            Report via <em>Upload Fixed Excel</em> — and click{' '}
            <em>Apply fixes &amp; rebuild</em>. For a duplicate that&rsquo;s really an
            existing client under a different name (e.g. an Axis name mismatch), pick
            that client under <em>Merge into existing client</em> — its balance folds in
            and the mapping is <strong>remembered on this computer</strong> for every
            future build.
          </p>
        </div>
        <div className="actions">
          <input
            ref={uploadInputRef}
            type="file"
            accept=".xlsx,.xls,.csv"
            onChange={onUploadFixed}
            style={{ display: 'none' }}
          />
          {savedMergeCount > 0 && (
            <button className="btn btn-secondary" onClick={onClearMerges} disabled={applying}>
              Clear saved mappings ({savedMergeCount})
            </button>
          )}
          <button
            className="btn btn-secondary"
            onClick={() => uploadInputRef.current?.click()}
            disabled={applying}
          >
            Upload Fixed Excel
          </button>
          <button
            className="btn btn-primary"
            onClick={onApply}
            disabled={applying || filledCount === 0}
          >
            {applying
              ? 'Applying…'
              : `Apply fixes & rebuild${filledCount ? ` (${filledCount})` : ''}`}
          </button>
        </div>
      </div>

      <datalist id="rm-options">
        {rmOptions.map((rm) => (
          <option key={rm} value={rm} />
        ))}
      </datalist>

      <div className="table-wrap">
        <table className="tracker-table">
          <thead>
            <tr>
              <th>Issue</th>
              <th>Client Code</th>
              <th>Client Name</th>
              <th>RM Name</th>
              <th>ICICI Acc. No.</th>
              <th>Merge into existing client</th>
            </tr>
          </thead>
          <tbody>
            {fixableExceptions.map(({ exc, idx }) => {
              const allow = FIXABLE_FIELDS[exc['Issue']] || {};
              const fix = fixes[idx] || {};
              const mergeable = isMergeableIssue(exc['Issue']);
              return (
                <tr key={idx}>
                  <td>{exc['Issue']}</td>
                  <td className="mono">
                    {allow.clientCode ? (
                      <input
                        className="fix-input"
                        value={fix.clientCode ?? ''}
                        onChange={(e) => onChange(idx, 'clientCode', e.target.value)}
                        placeholder="Code…"
                      />
                    ) : (
                      exc['Client Code'] || <span className="muted">—</span>
                    )}
                  </td>
                  <td>
                    {allow.clientName ? (
                      <input
                        className="fix-input"
                        value={fix.clientName ?? ''}
                        onChange={(e) => onChange(idx, 'clientName', e.target.value)}
                        placeholder={exc['Client Name'] || 'Name…'}
                      />
                    ) : (
                      exc['Client Name'] || <span className="muted">—</span>
                    )}
                  </td>
                  <td>
                    {allow.rmName ? (
                      <input
                        className="fix-input"
                        list="rm-options"
                        value={fix.rmName ?? ''}
                        onChange={(e) => onChange(idx, 'rmName', e.target.value)}
                        placeholder="RM…"
                      />
                    ) : (
                      exc['RM Name'] || <span className="muted">—</span>
                    )}
                  </td>
                  <td className="mono">
                    {allow.iciciAccount ? (
                      <div className="fix-account-cell">
                        <input
                          className="fix-input"
                          value={
                            fix.iciciAccount === 'N/A'
                              ? ''
                              : (fix.iciciAccount ?? exc['ICICI Acc. No.'] ?? '')
                          }
                          disabled={fix.iciciAccount === 'N/A'}
                          onChange={(e) => onChange(idx, 'iciciAccount', e.target.value)}
                          placeholder="Account…"
                        />
                        <button
                          className={`btn-na${fix.iciciAccount === 'N/A' ? ' btn-na-active' : ''}`}
                          onClick={() =>
                            onChange(idx, 'iciciAccount', fix.iciciAccount === 'N/A' ? '' : 'N/A')
                          }
                        >
                          N/A
                        </button>
                      </div>
                    ) : (
                      exc['ICICI Acc. No.'] || <span className="muted">—</span>
                    )}
                  </td>
                  <td>
                    {mergeable ? (
                      <select
                        className="fix-input merge-select"
                        value=""
                        disabled={applying}
                        onChange={(e) => {
                          const code = e.target.value;
                          if (code) onSaveMerge(exc, code);
                        }}
                      >
                        <option value="">Keep separate…</option>
                        {mergeTargets.map((t) => (
                          <option key={t.code} value={t.code}>
                            {t.label}
                          </option>
                        ))}
                      </select>
                    ) : (
                      <span className="muted">—</span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
});

const NUMERIC_COLS = new Set([
  'ICICI Bank Balance', 'Liquid MF Balance', 'Axis Demat Balance', 'Total Liquidity',
]);

function clientGroupKey(row, index) {
  const name = String(row['Client Name'] || '').trim();
  if (!name) {
    return `__blank__:${row['WS Client Code'] || row['ICICI Acc. No.'] || index}`;
  }
  return name
    .replace(/\s+ex[-\s]?broker$/i, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase();
}

function preferredClientName(rows) {
  const withoutExBroker = rows.find((row) =>
    !/\s+ex[-\s]?broker$/i.test(String(row['Client Name'] || '').trim())
  );
  return (withoutExBroker || rows[0])?.['Client Name'] || '';
}

function compactValues(values) {
  const unique = Array.from(new Set(values.map((v) => String(v || '').trim()).filter(Boolean)));
  if (unique.length <= 1) return unique[0] || '';
  return `${unique[0]} +${unique.length - 1}`;
}

function aggregateClientRows(rows) {
  const first = rows[0] || {};
  const rmNames = rows.map((row) => row['RM Name']);
  return {
    'RM Name': compactValues(rmNames),
    'WS Client Code': compactValues(rows.map((row) => row['WS Client Code'])),
    'Client Name': rows.length > 1 ? preferredClientName(rows) : first['Client Name'],
    'ICICI Acc. No.': compactValues(rows.map((row) => row['ICICI Acc. No.'])),
    'ICICI Bank Balance': rows.reduce((s, row) => s + (row['ICICI Bank Balance'] || 0), 0),
    'Liquid MF Balance': rows.reduce((s, row) => s + (row['Liquid MF Balance'] || 0), 0),
    'Axis Demat Balance': rows.reduce((s, row) => s + (row['Axis Demat Balance'] || 0), 0),
    'Total Liquidity': rows.reduce((s, row) => s + (row['Total Liquidity'] || 0), 0),
  };
}

function buildClientDisplayEntries(rows) {
  const groups = new Map();
  rows.forEach((row, index) => {
    const key = clientGroupKey(row, index);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  });

  return Array.from(groups.entries()).map(([key, groupRows]) => ({
    key,
    rows: groupRows,
    row: groupRows.length > 1 ? aggregateClientRows(groupRows) : groupRows[0],
    isGrouped: groupRows.length > 1,
  }));
}

function DailyTrackerTable({ entries, mergeSelection = [], onToggleMergeSelect = () => {} }) {
  const [sort, setSort] = useState({ col: null, dir: 'desc' });
  const [expanded, setExpanded] = useState(() => new Set());

  const toggleSort = (col) => {
    setSort((prev) => ({
      col,
      dir: prev.col === col && prev.dir === 'desc' ? 'asc' : 'desc',
    }));
  };

  const toggleExpanded = (key) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const sortedEntries = useMemo(() => {
    if (!sort.col) return entries;
    return [...entries].sort((a, b) => {
      const av = a.row[sort.col];
      const bv = b.row[sort.col];
      if (NUMERIC_COLS.has(sort.col)) {
        const diff = (av || 0) - (bv || 0);
        return sort.dir === 'asc' ? diff : -diff;
      }
      const cmp = String(av || '').localeCompare(String(bv || ''));
      return sort.dir === 'asc' ? cmp : -cmp;
    });
  }, [entries, sort]);

  if (entries.length === 0) {
    return <div className="empty-table">No rows match the current filters.</div>;
  }

  const SortTh = ({ col, children, className }) => {
    const active = sort.col === col;
    return (
      <th
        className={`sortable${active ? ' sort-active' : ''}${className ? ' ' + className : ''}`}
        onClick={() => toggleSort(col)}
      >
        {children}
        <span className="sort-icon">
          {active ? (sort.dir === 'asc' ? ' ↑' : ' ↓') : ' ↕'}
        </span>
      </th>
    );
  };

  let currentRm = null;
  let bandIndex = 0;

  return (
    <div className="table-wrap">
      <table className="tracker-table">
        <thead>
          <tr>
            <th className="select-col" aria-label="Select rows to merge" />
            <SortTh col="RM Name">RM / Client Partner</SortTh>
            <SortTh col="WS Client Code">WS Client Code</SortTh>
            <SortTh col="Client Name">Client Name</SortTh>
            <SortTh col="ICICI Acc. No.">ICICI Acc. No.</SortTh>
            <SortTh col="ICICI Bank Balance" className="num">ICICI Balance</SortTh>
            <SortTh col="Liquid MF Balance" className="num">Liquid MF</SortTh>
            <SortTh col="Axis Demat Balance" className="num">Axis Demat</SortTh>
            <SortTh col="Total Liquidity" className="num">Total Liquidity</SortTh>
          </tr>
        </thead>
        <tbody>
          {sortedEntries.flatMap((entry, i) => {
            const r = entry.row;
            if (r['RM Name'] !== currentRm) {
              currentRm = r['RM Name'];
              bandIndex += 1;
            }
            const isOpen = expanded.has(entry.key);
            const parent = (
              <tr
                key={`${entry.key}-parent`}
                className={`band-${bandIndex % 2}${entry.isGrouped ? ' grouped-parent' : ''}`}
              >
                <td className="select-col">
                  <input
                    type="checkbox"
                    checked={mergeSelection.includes(entry.key)}
                    onChange={() => onToggleMergeSelect(entry.key)}
                    aria-label={`Select ${r['Client Name'] || 'row'} for merge`}
                  />
                </td>
                <td>{r['RM Name'] || <span className="muted">—</span>}</td>
                <td className="mono">{r['WS Client Code']}</td>
                <td>
                  <div className="client-cell">
                    {entry.isGrouped && (
                      <button
                        className="row-toggle"
                        onClick={() => toggleExpanded(entry.key)}
                        aria-label={isOpen ? 'Collapse client rows' : 'Expand client rows'}
                      >
                        {isOpen ? '▾' : '▸'}
                      </button>
                    )}
                    <span>{r['Client Name']}</span>
                    {entry.isGrouped && (
                      <span className="duplicate-pill">{entry.rows.length} records</span>
                    )}
                  </div>
                </td>
                <td className="mono">
                  {r['ICICI Acc. No.'] || <span className="muted">—</span>}
                </td>
                <td className="num">{INR.format(r['ICICI Bank Balance'])}</td>
                <td className="num">{INR.format(r['Liquid MF Balance'])}</td>
                <td className="num">{INR.format(r['Axis Demat Balance'] || 0)}</td>
                <td className="num strong">{INR.format(r['Total Liquidity'])}</td>
              </tr>
            );
            if (!entry.isGrouped || !isOpen) return [parent];
            const children = entry.rows.map((child, childIndex) => (
              <tr key={`${entry.key}-child-${childIndex}`} className="grouped-child">
                <td className="select-col" />
                <td>{child['RM Name'] || <span className="muted">—</span>}</td>
                <td className="mono">{child['WS Client Code']}</td>
                <td>{child['Client Name']}</td>
                <td className="mono">
                  {child['ICICI Acc. No.'] || <span className="muted">—</span>}
                </td>
                <td className="num">{INR.format(child['ICICI Bank Balance'])}</td>
                <td className="num">{INR.format(child['Liquid MF Balance'])}</td>
                <td className="num">{INR.format(child['Axis Demat Balance'] || 0)}</td>
                <td className="num strong">{INR.format(child['Total Liquidity'])}</td>
              </tr>
            ));
            return [parent, ...children];
          })}
        </tbody>
      </table>
    </div>
  );
}
