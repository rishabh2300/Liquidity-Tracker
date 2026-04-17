import { useMemo, useRef, useState } from 'react';
import {
  parseFile,
  buildLiquidityTracker,
  exportToRmZip,
} from './liquidityProcessor';
import { saveAs } from 'file-saver';
import logo from './assets/gold_logo.png';
import './App.css';

// File-slot configuration. `stringColumns` tells PapaParse which fields
// to read as strings — critical for account numbers to survive parsing.
const FILE_SLOTS = [
  {
    key: 'ws',
    label: 'WS Balance',
    hint: 'Columns: CLIENT_CODE, CLIENTNAME, BANK_ACCOUNT',
    stringColumns: ['BANK_ACCOUNT', 'CLIENT_CODE'],
  },
  {
    key: 'z10',
    label: 'Z10 Holding / AssetClass',
    hint: 'Columns: WS client id, RMNAME, Security Type Description, ASTCLSNAME, MKTVALUE',
    stringColumns: ['WS client id'],
  },
  {
    key: 'icici',
    label: 'ICICI Balance',
    hint: 'Columns: Account Number, Available Balance',
    stringColumns: ['Account Number'],
  },
];

const INR = new Intl.NumberFormat('en-IN', {
  style: 'currency',
  currency: 'INR',
  maximumFractionDigits: 0,
});

export default function App() {
  const [files, setFiles] = useState({ ws: null, z10: null, icici: null });
  const [result, setResult] = useState(null);
  const [error, setError] = useState('');
  const [processing, setProcessing] = useState(false);
  const [rmFilter, setRmFilter] = useState('all');
  const [search, setSearch] = useState('');
  const [exporting, setExporting] = useState(false);

  const allFilesReady = files.ws && files.z10 && files.icici;

  const handleFileSelect = (key, file) => {
    setFiles((prev) => ({ ...prev, [key]: file }));
    setResult(null);
    setError('');
  };

  const handleProcess = async () => {
    if (!allFilesReady) return;
    setProcessing(true);
    setError('');
    setResult(null);

    try {
      const [wsRows, z10Rows, iciciRows] = await Promise.all([
        parseFile(files.ws, FILE_SLOTS[0].stringColumns),
        parseFile(files.z10, FILE_SLOTS[1].stringColumns),
        parseFile(files.icici, FILE_SLOTS[2].stringColumns),
      ]);

      const tracker = buildLiquidityTracker(wsRows, z10Rows, iciciRows);
      setResult(tracker);
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setProcessing(false);
    }
  };

  const handleReset = () => {
    setFiles({ ws: null, z10: null, icici: null });
    setResult(null);
    setError('');
    setRmFilter('all');
    setSearch('');
  };

  const handleExport = async () => {
    if (!result) return;
    setExporting(true);
    try {
      const blob = await exportToRmZip(result.rows, result.summary);
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
        r.CLIENTNAME.toLowerCase().includes(q) ||
        r.CLIENT_CODE.toLowerCase().includes(q) ||
        r.BANK_ACCOUNT.toLowerCase().includes(q)
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
        <div className="accent-bar" />
      </header>

      <main className="app-main">
        {/* Upload section */}
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
                onSelect={(f) => handleFileSelect(slot.key, f)}
                onClear={() => handleFileSelect(slot.key, null)}
              />
            ))}
          </div>

          <div className="actions">
            <button
              className="btn btn-primary"
              onClick={handleProcess}
              disabled={!allFilesReady || processing}
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

        {/* Results */}
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
                  <select
                    value={rmFilter}
                    onChange={(e) => setRmFilter(e.target.value)}
                  >
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

              <TrackerTable rows={filteredRows} />
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

function TrackerTable({ rows }) {
  if (rows.length === 0) {
    return <div className="empty-table">No rows match the current filters.</div>;
  }

  // Group consecutive rows by RM for visual banding
  let currentRm = null;
  let bandIndex = 0;

  return (
    <div className="table-wrap">
      <table className="tracker-table">
        <thead>
          <tr>
            <th>RM / Client Partner</th>
            <th>Client Code</th>
            <th>Client Name</th>
            <th>Bank Account</th>
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
              <tr key={`${r.CLIENT_CODE}-${i}`} className={`band-${bandIndex % 2}`}>
                <td>{r['RM Name'] || <span className="muted">—</span>}</td>
                <td className="mono">{r.CLIENT_CODE}</td>
                <td>{r.CLIENTNAME}</td>
                <td className="mono">{r.BANK_ACCOUNT}</td>
                <td className="num">{INR.format(r['ICICI Bank Balance'])}</td>
                <td className="num">{INR.format(r['Liquid_MF_Balance'])}</td>
                <td className="num strong">{INR.format(r['Total Liquidity'])}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
