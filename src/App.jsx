import React, { useMemo, useRef, useState } from 'react';
import {
  parseFile,
  buildDailyTracker,
  buildUpdatedMaster,
  exportDailyTrackerZip,
  exportMasterToXlsx,
  exportMappingReportToXlsx,
  exportExceptionsToXlsx,
} from './liquidityProcessor';
import { saveAs } from 'file-saver';
import logo from './assets/gold_logo.png';
import './App.css';

const FILE_SLOTS = [
  {
    key: 'master',
    label: 'Client Master',
    hint: 'Columns: WS Client Code, Client Name, RM Name, ICICI Acc. No.',
  },
  {
    key: 'recon',
    label: 'Bank Recon (multi-sheet)',
    hint: 'Sheets: "WS Balances" (for Master) + "ICICI Balances" (for Current Account)',
  },
  {
    key: 'z10',
    label: 'Z10 Holding / AssetClass',
    hint: 'Columns: WS client id, Security Type Description, ASTCLSNAME, MKTVALUE',
  },
  {
    key: 'cw',
    label: 'Client Wise (Axis Ledger)',
    hint: 'Columns: Client Name, Ledger Bal BOD (joined to Master by uppercased Client Name)',
  },
];

const INR = new Intl.NumberFormat('en-IN', {
  style: 'currency',
  currency: 'INR',
  maximumFractionDigits: 0,
});

export default function App() {
  const [files, setFiles] = useState({ master: null, recon: null, z10: null, cw: null });
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
  const fixerRef = useRef(null);

  const allReady = files.master && files.recon && files.z10 && files.cw;

  const setFile = (key, file) => {
    setFiles((prev) => ({ ...prev, [key]: file }));
    setResult(null);
    setMasterResult(null);
    setParsedSources(null);
    setExceptionFixes({});
    setError('');
  };

  const handleProcess = async () => {
    if (!allReady) return;
    setProcessing(true);
    setError('');
    try {
      const [masterData, reconData, z10Data, cwData] = await Promise.all([
        parseFile(files.master),
        parseFile(files.recon),
        parseFile(files.z10),
        parseFile(files.cw),
      ]);

      const firstSheetRows = (data) =>
        Array.isArray(data) ? data : data[Object.keys(data)[0]] || [];

      const masterRows = firstSheetRows(masterData);
      const z10Rows = firstSheetRows(z10Data);
      const cwRows = firstSheetRows(cwData);

      const reconRows = Array.isArray(reconData)
        ? reconData
        : reconData['WS Balances'] ||
          reconData['WS Balance'] ||
          reconData['Bank Recon 150426'] ||
          [];
      const caRows = Array.isArray(reconData)
        ? []
        : reconData['ICICI Balances'] || reconData['ICICI Balance'] || [];

      const masterSheetName = Array.isArray(masterData)
        ? null
        : Object.keys(masterData)[0] || null;

      const updatedMasterResult = buildUpdatedMaster(masterRows, reconRows);

      const masterMeta = { fileName: files.master.name, sheetName: masterSheetName };
      const dailyTrackerResult = buildDailyTracker(
        updatedMasterResult.rows,
        z10Rows,
        caRows,
        cwRows,
        masterMeta
      );

      setLastGeneratedAt((prev) => result?.generatedAt ?? prev);
      setMasterResult(updatedMasterResult);
      setResult(dailyTrackerResult);
      setParsedSources({ z10Rows, caRows, cwRows, masterMeta });
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

  const handleReset = () => {
    setFiles({ master: null, recon: null, z10: null, cw: null });
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
        parsedSources.masterMeta
      );

      setResult(dailyTrackerResult);
      setExceptionFixes({});
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setApplyingFixes(false);
    }
  };

  const handleExportZip = async () => {
    if (!result) return;
    setExporting(true);
    try {
      const blob = await exportDailyTrackerZip(result.rows, result.summary);
      const today = new Date().toISOString().slice(0, 10);
      saveAs(blob, `Onza_Liquidity_Tracker_${today}.zip`);
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
    saveAs(blob, `Client_Master_${today}.xlsx`);
  };

  const handleDownloadExceptions = () => {
    if (!result?.exceptions?.length) return;
    const buffer = exportExceptionsToXlsx(result.exceptions, result.generatedAt);
    const blob = new Blob([buffer], {
      type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    });
    saveAs(blob, 'Daily_Exceptions.xlsx');
  };

  const handleDownloadMappingReport = () => {
    if (!masterResult?.mappingReport?.length) return;
    const buffer = exportMappingReportToXlsx(
      masterResult.mappingReport,
      masterResult.generatedAt
    );
    const blob = new Blob([buffer], {
      type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    });
    const today = new Date().toISOString().slice(0, 10);
    saveAs(blob, `Mapping_Report_${today}.xlsx`);
  };

  const rmOptions = useMemo(() => {
    if (!result) return [];
    return Array.from(new Set(result.rows.map((r) => r['RM Name']).filter(Boolean))).sort();
  }, [result]);

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

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-inner">
          <div className="brand">
            <img src={logo} alt="Onza" className="brand-logo" />
            <div>
              <h1 className="app-title">Liquidity Tracker</h1>
              <p className="app-subtitle">
                One-click pipeline: reconcile master, then build daily tracker
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

          <div className="file-grid file-grid-4">
            {FILE_SLOTS.map((slot) => (
              <FileSlot
                key={slot.key}
                slot={slot}
                file={files[slot.key]}
                onSelect={(f) => setFile(slot.key, f)}
                onClear={() => setFile(slot.key, null)}
              />
            ))}
          </div>

          <div className="actions">
            <button
              className="btn btn-primary"
              onClick={handleProcess}
              disabled={!allReady || processing}
            >
              {processing ? 'Building tracker…' : 'Build tracker'}
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
          {result?.exceptions?.length > 0 && (
            <div
              className="alert alert-warn"
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                marginTop: '1rem',
              }}
            >
              <div>
                <strong>⚠ Exceptions Found:</strong> {result.exceptions.length} records have
                missing mapping data (e.g., missing RM Name or Bank Balance).
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
                value={result.summary.totalClients.toLocaleString('en-IN')}
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
                    Export Updated Client Master
                  </button>
                  {masterResult?.mappingReport?.length > 0 && (
                    <button
                      className="btn btn-secondary"
                      onClick={handleDownloadMappingReport}
                    >
                      Download Mapping Report
                    </button>
                  )}
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
                  Showing <strong>{filteredRows.length.toLocaleString('en-IN')}</strong> of{' '}
                  {result.rows.length.toLocaleString('en-IN')} · Subtotal{' '}
                  <strong>{INR.format(filteredTotal)}</strong>
                </div>
              </div>

              <DailyTrackerTable rows={filteredRows} />
            </section>

            {fixableExceptions.length > 0 && (
              <ExceptionFixerSection
                ref={fixerRef}
                fixableExceptions={fixableExceptions}
                fixes={exceptionFixes}
                onChange={updateFix}
                onApply={handleApplyFixes}
                applying={applyingFixes}
                rmOptions={rmOptions}
              />
            )}
          </>
        )}

        {!result && !error && (
          <section className="panel panel-empty">
            <p className="empty-title">Ready to build</p>
            <p className="empty-body">
              Upload the three source files above and select <em>Build tracker</em>. The app
              will reconcile the master against the bank recon, then generate today&rsquo;s
              liquidity view in one step.
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

function FileSlot({ slot, file, onSelect, onClear }) {
  const inputRef = useRef(null);

  const onDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    const f = e.dataTransfer.files?.[0];
    if (f) onSelect(f);
  };

  return (
    <div
      className={`file-slot ${file ? 'file-slot-filled' : ''}`}
      onDragOver={(e) => e.preventDefault()}
      onDrop={onDrop}
    >
      <div className="file-slot-header">
        <span className="file-slot-label">{slot.label}</span>
        {file && (
          <button className="file-slot-clear" onClick={onClear} aria-label="Remove file">
            ×
          </button>
        )}
      </div>
      <p className="file-slot-hint">{slot.hint}</p>

      {file ? (
        <div className="file-slot-file">
          <span className="file-name">{file.name}</span>
          <span className="file-size">{(file.size / 1024).toFixed(1)} KB</span>
        </div>
      ) : (
        <>
          <input
            ref={inputRef}
            type="file"
            accept=".csv,.xlsx,.xlsm,.xls"
            onChange={(e) => e.target.files?.[0] && onSelect(e.target.files[0])}
            style={{ display: 'none' }}
          />
          <button
            className="file-slot-trigger"
            onClick={() => inputRef.current?.click()}
          >
            Choose file or drop here
          </button>
        </>
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
  'Liquid MF Holding Without Master Record (auto-added)': { rmName: true, iciciAccount: true },
  'ICICI Balance Without Master Record (auto-added)': {
    clientCode: true, clientName: true, rmName: true,
  },
};

const ExceptionFixerSection = React.forwardRef(function ExceptionFixerSection(
  { fixableExceptions, fixes, onChange, onApply, applying, rmOptions },
  ref
) {
  const filledCount = Object.values(fixes).filter((f) =>
    Object.values(f || {}).some((v) => String(v ?? '').trim() !== '')
  ).length;

  return (
    <section className="panel">
      <div className="panel-header panel-header-split">
        <div>
          <span className="panel-label">Optional</span>
          <h2 className="panel-title">Fix exceptions and rebuild</h2>
          <p className="panel-meta" style={{ marginTop: 6 }}>
            Fill in the missing details below and click <em>Apply fixes &amp; rebuild</em>.
            The patched master is used for the new tracker run, and is also what gets
            included in <em>Export Updated Client Master</em>.
          </p>
        </div>
        <div className="actions">
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
            </tr>
          </thead>
          <tbody>
            {fixableExceptions.map(({ exc, idx }) => {
              const allow = FIXABLE_FIELDS[exc['Issue']] || {};
              const fix = fixes[idx] || {};
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
                          value={fix.iciciAccount === 'N/A' ? '' : (fix.iciciAccount ?? '')}
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

function DailyTrackerTable({ rows }) {
  const [sort, setSort] = useState({ col: null, dir: 'desc' });

  const toggleSort = (col) => {
    setSort((prev) => ({
      col,
      dir: prev.col === col && prev.dir === 'desc' ? 'asc' : 'desc',
    }));
  };

  const sortedRows = useMemo(() => {
    if (!sort.col) return rows;
    return [...rows].sort((a, b) => {
      const av = a[sort.col];
      const bv = b[sort.col];
      if (NUMERIC_COLS.has(sort.col)) {
        const diff = (av || 0) - (bv || 0);
        return sort.dir === 'asc' ? diff : -diff;
      }
      const cmp = String(av || '').localeCompare(String(bv || ''));
      return sort.dir === 'asc' ? cmp : -cmp;
    });
  }, [rows, sort]);

  if (rows.length === 0) {
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
          {sortedRows.map((r, i) => {
            if (r['RM Name'] !== currentRm) {
              currentRm = r['RM Name'];
              bandIndex += 1;
            }
            return (
              <tr key={`${r['WS Client Code']}-${i}`} className={`band-${bandIndex % 2}`}>
                <td>{r['RM Name'] || <span className="muted">—</span>}</td>
                <td className="mono">{r['WS Client Code']}</td>
                <td>{r['Client Name']}</td>
                <td className="mono">
                  {r['ICICI Acc. No.'] || <span className="muted">—</span>}
                </td>
                <td className="num">{INR.format(r['ICICI Bank Balance'])}</td>
                <td className="num">{INR.format(r['Liquid MF Balance'])}</td>
                <td className="num">{INR.format(r['Axis Demat Balance'] || 0)}</td>
                <td className="num strong">{INR.format(r['Total Liquidity'])}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
