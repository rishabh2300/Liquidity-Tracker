import { useMemo, useRef, useState } from 'react';
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
];

const INR = new Intl.NumberFormat('en-IN', {
  style: 'currency',
  currency: 'INR',
  maximumFractionDigits: 0,
});

export default function App() {
  const [files, setFiles] = useState({ master: null, recon: null, z10: null });
  const [result, setResult] = useState(null);
  const [masterResult, setMasterResult] = useState(null);
  const [lastGeneratedAt, setLastGeneratedAt] = useState(null);
  const [error, setError] = useState('');
  const [processing, setProcessing] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [rmFilter, setRmFilter] = useState('all');
  const [search, setSearch] = useState('');

  const allReady = files.master && files.recon && files.z10;

  const setFile = (key, file) => {
    setFiles((prev) => ({ ...prev, [key]: file }));
    setResult(null);
    setMasterResult(null);
    setError('');
  };

  const handleProcess = async () => {
    if (!allReady) return;
    setProcessing(true);
    setError('');
    try {
      const [masterData, reconData, z10Data] = await Promise.all([
        parseFile(files.master),
        parseFile(files.recon),
        parseFile(files.z10),
      ]);

      const firstSheetRows = (data) =>
        Array.isArray(data) ? data : data[Object.keys(data)[0]] || [];

      const masterRows = firstSheetRows(masterData);
      const z10Rows = firstSheetRows(z10Data);

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
        masterMeta
      );

      setLastGeneratedAt((prev) => result?.generatedAt ?? prev);
      setMasterResult(updatedMasterResult);
      setResult(dailyTrackerResult);
    } catch (err) {
      setError(err.message || String(err));
      setResult(null);
      setMasterResult(null);
    } finally {
      setProcessing(false);
    }
  };

  const handleReset = () => {
    setFiles({ master: null, recon: null, z10: null });
    setResult(null);
    setMasterResult(null);
    setError('');
    setRmFilter('all');
    setSearch('');
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
    const exportRows = masterResult.rows.map(({ Status, ...rest }) => rest);
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

          <div className="file-grid">
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
              <button className="btn btn-secondary" onClick={handleDownloadExceptions}>
                Download Exception Report
              </button>
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

function DailyTrackerTable({ rows }) {
  if (rows.length === 0) {
    return <div className="empty-table">No rows match the current filters.</div>;
  }

  let currentRm = null;
  let bandIndex = 0;

  return (
    <div className="table-wrap">
      <table className="tracker-table">
        <thead>
          <tr>
            <th>RM / Client Partner</th>
            <th>WS Client Code</th>
            <th>Client Name</th>
            <th>ICICI Acc. No.</th>
            <th className="num">ICICI Balance</th>
            <th className="num">Liquid MF</th>
            <th className="num">Total Liquidity</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => {
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
                <td className="num strong">{INR.format(r['Total Liquidity'])}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
