import { useMemo, useRef, useState } from 'react';
import {
  parseFile,
  buildDailyTracker,
  buildUpdatedMaster,
  exportDailyTrackerZip,
  exportMasterToXlsx,
} from './liquidityProcessor';
import { saveAs } from 'file-saver';
import logo from './assets/gold_logo.png';
import './App.css';

const DAILY_SLOTS = [
  {
    key: 'master',
    label: 'Client Master',
    hint: 'Columns: WS Client Code, Client Name, RM Name, ICICI Acc. No.',
  },
  {
    key: 'z10',
    label: 'Z10 Holding / AssetClass',
    hint: 'Columns: WS client id, Security Type Description, ASTCLSNAME, MKTVALUE',
  },
  {
    key: 'ca',
    label: 'Current Account Data',
    hint: 'Columns: Account Number, Available Balance',
  },
];

const MASTER_SLOTS = [
  {
    key: 'master',
    label: 'Existing Client Master',
    hint: 'Columns: WS Client Code, Client Name, RM Name, ICICI Acc. No.',
  },
  {
    key: 'recon',
    label: 'Bank Recon (WS Balance)',
    hint: 'Columns: Bank Code, Client Name, Bank Account',
  },
];

const INR = new Intl.NumberFormat('en-IN', {
  style: 'currency',
  currency: 'INR',
  maximumFractionDigits: 0,
});

export default function App() {
  const [tab, setTab] = useState('daily');

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-inner">
          <div className="brand">
            <img src={logo} alt="Onza" className="brand-logo" />
            <div>
              <h1 className="app-title">Liquidity Tracker</h1>
              <p className="app-subtitle">Daily client liquidity, organised by Client Partner</p>
            </div>
          </div>
          <div className="date-chip">
            {new Date().toLocaleDateString('en-IN', {
              weekday: 'long',
              day: 'numeric',
              month: 'long',
              year: 'numeric',
            })}
          </div>
        </div>
      </header>

      <main className="app-main">
        <nav className="tabs" role="tablist">
          <button
            role="tab"
            aria-selected={tab === 'daily'}
            className={`tab ${tab === 'daily' ? 'tab-active' : ''}`}
            onClick={() => setTab('daily')}
          >
            Daily Tracker
          </button>
          <button
            role="tab"
            aria-selected={tab === 'master'}
            className={`tab ${tab === 'master' ? 'tab-active' : ''}`}
            onClick={() => setTab('master')}
          >
            Update Master
          </button>
        </nav>

        {tab === 'daily' ? <DailyTrackerTab /> : <UpdateMasterTab />}
      </main>

      <footer className="app-footer">
        <span>Onza · Internal tool · Processes run locally on this device</span>
      </footer>
    </div>
  );
}

// -----------------------------------------------------------------------------
// Tab 1 — Daily Tracker
// -----------------------------------------------------------------------------

function DailyTrackerTab() {
  const [files, setFiles] = useState({ master: null, z10: null, ca: null });
  const [result, setResult] = useState(null);
  const [error, setError] = useState('');
  const [processing, setProcessing] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [rmFilter, setRmFilter] = useState('all');
  const [search, setSearch] = useState('');

  const allReady = files.master && files.z10 && files.ca;

  const setFile = (key, file) => {
    setFiles((prev) => ({ ...prev, [key]: file }));
    setResult(null);
    setError('');
  };

  const handleProcess = async () => {
    if (!allReady) return;
    setProcessing(true);
    setError('');
    setResult(null);
    try {
      const [masterRows, z10Rows, caRows] = await Promise.all([
        parseFile(files.master),
        parseFile(files.z10),
        parseFile(files.ca),
      ]);
      setResult(buildDailyTracker(masterRows, z10Rows, caRows));
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setProcessing(false);
    }
  };

  const handleReset = () => {
    setFiles({ master: null, z10: null, ca: null });
    setResult(null);
    setError('');
    setRmFilter('all');
    setSearch('');
  };

  const handleExport = async () => {
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
    <>
      <section className="panel">
        <div className="panel-header">
          <span className="panel-label">Step 01</span>
          <h2 className="panel-title">Upload source files</h2>
        </div>

        <div className="file-grid">
          {DAILY_SLOTS.map((slot) => (
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

        {error && <div className="alert alert-error">{error}</div>}
        {result?.warnings?.length > 0 && (
          <div className="alert alert-warn">
            {result.warnings.map((w, i) => (
              <div key={i}>⚠ {w}</div>
            ))}
          </div>
        )}
      </section>

      {result && (
        <>
          <section className="summary-row">
            <SummaryCard label="Clients" value={result.summary.totalClients.toLocaleString('en-IN')} />
            <SummaryCard label="Client Partners" value={result.summary.totalRMs.toLocaleString('en-IN')} />
            <SummaryCard label="ICICI Cash" value={INR.format(result.summary.totalIcici)} />
            <SummaryCard label="Liquid MFs" value={INR.format(result.summary.totalLiquidMf)} />
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
              <button
                className="btn btn-primary"
                onClick={handleExport}
                disabled={exporting}
              >
                {exporting ? 'Packaging ZIP…' : 'Export ZIP (master + per-RM)'}
              </button>
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
          <p className="empty-title">No tracker built yet</p>
          <p className="empty-body">
            Upload the three source files above and select <em>Build tracker</em> to generate
            today&rsquo;s liquidity view.
          </p>
        </section>
      )}
    </>
  );
}

// -----------------------------------------------------------------------------
// Tab 2 — Update Master
// -----------------------------------------------------------------------------

function UpdateMasterTab() {
  const [files, setFiles] = useState({ master: null, recon: null });
  const [result, setResult] = useState(null);
  const [error, setError] = useState('');
  const [processing, setProcessing] = useState(false);
  const [statusFilter, setStatusFilter] = useState('all');

  const allReady = files.master && files.recon;

  const setFile = (key, file) => {
    setFiles((prev) => ({ ...prev, [key]: file }));
    setResult(null);
    setError('');
  };

  const handleProcess = async () => {
    if (!allReady) return;
    setProcessing(true);
    setError('');
    setResult(null);
    try {
      const [masterRows, reconRows] = await Promise.all([
        parseFile(files.master),
        parseFile(files.recon),
      ]);
      setResult(buildUpdatedMaster(masterRows, reconRows));
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setProcessing(false);
    }
  };

  const handleReset = () => {
    setFiles({ master: null, recon: null });
    setResult(null);
    setError('');
    setStatusFilter('all');
  };

  const handleExport = () => {
    if (!result) return;
    const exportRows = result.rows.map(({ Status, ...rest }) => rest);
    const buffer = exportMasterToXlsx(exportRows);
    const blob = new Blob([buffer], {
      type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    });
    const today = new Date().toISOString().slice(0, 10);
    saveAs(blob, `Client_Master_${today}.xlsx`);
  };

  const filteredRows = useMemo(() => {
    if (!result) return [];
    if (statusFilter === 'all') return result.rows;
    return result.rows.filter((r) => r.Status.toLowerCase() === statusFilter);
  }, [result, statusFilter]);

  return (
    <>
      <section className="panel">
        <div className="panel-header">
          <span className="panel-label">Step 01</span>
          <h2 className="panel-title">Upload reconciliation files</h2>
        </div>

        <div className="file-grid file-grid-2">
          {MASTER_SLOTS.map((slot) => (
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
            {processing ? 'Reconciling…' : 'Reconcile Master'}
          </button>
          <button
            className="btn btn-secondary"
            onClick={handleReset}
            disabled={processing}
          >
            Reset
          </button>
        </div>

        {error && <div className="alert alert-error">{error}</div>}
      </section>

      {result && (
        <>
          <section className="summary-row">
            <SummaryCard label="Total Records" value={result.summary.totalRecords.toLocaleString('en-IN')} />
            <SummaryCard label="Added" value={result.summary.added.toLocaleString('en-IN')} />
            <SummaryCard label="Updated" value={result.summary.updated.toLocaleString('en-IN')} />
            <SummaryCard label="Unchanged" value={result.summary.unchanged.toLocaleString('en-IN')} />
          </section>

          <section className="panel">
            <div className="panel-header panel-header-split">
              <div>
                <span className="panel-label">Step 02</span>
                <h2 className="panel-title">Updated Client Master</h2>
              </div>
              <button className="btn btn-primary" onClick={handleExport}>
                Export Updated Master
              </button>
            </div>

            <div className="filters">
              <label className="filter">
                <span className="filter-label">Filter by status</span>
                <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
                  <option value="all">All</option>
                  <option value="new">New</option>
                  <option value="updated">Updated</option>
                  <option value="unchanged">Unchanged</option>
                </select>
              </label>
              <div className="filter-meta">
                Showing <strong>{filteredRows.length.toLocaleString('en-IN')}</strong> of{' '}
                {result.rows.length.toLocaleString('en-IN')}
              </div>
            </div>

            <MasterTable rows={filteredRows} />
          </section>
        </>
      )}

      {!result && !error && (
        <section className="panel panel-empty">
          <p className="empty-title">No reconciliation run yet</p>
          <p className="empty-body">
            Upload the existing Client Master and the latest Bank Recon, then select{' '}
            <em>Reconcile Master</em>.
          </p>
        </section>
      )}
    </>
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
                <td className="mono">{r['ICICI Acc. No.'] || <span className="muted">—</span>}</td>
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

function MasterTable({ rows }) {
  if (rows.length === 0) {
    return <div className="empty-table">No rows match the current filter.</div>;
  }

  return (
    <div className="table-wrap">
      <table className="tracker1-table">
        <thead>
          <tr>
            <th>WS Client Code</th>
            <th>Client Name</th>
            <th>RM Name</th>
            <th>ICICI Acc. No.</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={`${r['WS Client Code']}-${i}`} className={`band-${i % 2}`}>
              <td className="mono">{r['WS Client Code']}</td>
              <td>{r['Client Name']}</td>
              <td>{r['RM Name'] || <span className="muted">—</span>}</td>
              <td className="mono">{r['ICICI Acc. No.'] || <span className="muted">—</span>}</td>
              <td>
                <span className={`status-badge status-${r.Status.toLowerCase()}`}>
                  {r.Status}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
