import { describe, it, expect } from 'vitest';
import {
  normalizeAccount,
  buildDailyTracker,
  buildDynamicMaster,
  findTable,
  classifyWorkbook,
  mergeTrackerRows,
  findCurrentAccountExportDate,
  parseSourceDate,
} from './liquidityProcessor';

describe('normalizeAccount()', () => {
  it('pads standard account numbers to 12 digits', () => {
    expect(normalizeAccount('123456')).toBe('000000123456');
  });

  it('strips trailing .0 from Excel numeric coercions', () => {
    expect(normalizeAccount('123456.0')).toBe('000000123456');
    expect(normalizeAccount('987.00')).toBe('000000000987');
  });

  it('handles null, undefined, and empty strings gracefully', () => {
    expect(normalizeAccount(null)).toBe('');
    expect(normalizeAccount(undefined)).toBe('');
    expect(normalizeAccount('')).toBe('');
  });

  it('handles numeric inputs directly', () => {
    expect(normalizeAccount(12345)).toBe('000000012345');
  });

  it('treats no-account placeholders consistently', () => {
    expect(normalizeAccount('NONE')).toBe('N/A');
    expect(normalizeAccount('N.A.')).toBe('N/A');
  });

  it('strips Excel text apostrophes before padding', () => {
    expect(normalizeAccount("'000201113896")).toBe('000201113896');
  });
});

describe('buildDailyTracker()', () => {
  const masterRows = [
    { 'WS Client Code': '100001', 'Client Name': 'Alice Corp', 'RM Name': 'Smith, John', 'ICICI Acc. No.': '1111' },
    { 'WS Client Code': '100002', 'Client Name': 'Bob LLC',    'RM Name': 'Doe, Jane',   'ICICI Acc. No.': '2222.0' },
    { 'WS Client Code': '100003', 'Client Name': 'Charlie Inc','RM Name': '',            'ICICI Acc. No.': '' },
  ];

  const z10Rows = [
    { 'WS client id': '100001', 'Security Type Description': 'Mutual Funds', 'ASTCLSNAME': 'Cash',  'MKTVALUE': '150000' },
    { 'WS client id': '100001', 'Security Type Description': 'Equity',       'ASTCLSNAME': 'Stock', 'MKTVALUE': '900000' },
    { 'WS client id': '100002', 'Security Type Description': 'Mutual Funds', 'ASTCLSNAME': 'Cash',  'MKTVALUE': '50,000' },
  ];

  const caRows = [
    { 'Account Number': '000000001111', 'Available Balance': '25000' },
    { 'Account Number': '000000002222', 'Available Balance': 10000 },
  ];

  it('throws on a Z0 Client Master missing required columns', () => {
    const bad = [{ Foo: 1 }];
    expect(() => buildDailyTracker(bad, z10Rows, caRows))
      .toThrow(/Z0 Client Master is missing required column\(s\):/);
  });

  it('accepts Z0 Client Master headers', () => {
    const z0MasterRows = [
      {
        CLIENTCODE: 'AABPL0985G',
        CLIENTID: '100001',
        CLIENTNAME: 'Alice Corp',
        RMNAME: 'Smith, John',
        'TRADING BANK ACCOUNT': '1111',
      },
      {
        CLIENTID: '100002',
        CLIENTNAME: 'Bob LLC',
        RMNAME: 'Doe, Jane',
        'TRADING BANK ACCOUNT': 'NONE',
        MANDATEBANKACCOUNTNO: "'2222",
      },
    ];

    const { rows } = buildDailyTracker(z0MasterRows, z10Rows, caRows);

    const alice = rows.find((r) => r['WS Client Code'] === '100001');
    expect(alice['Client Name']).toBe('Alice Corp');
    expect(alice['RM Name']).toBe('Smith, John');
    expect(alice['ICICI Acc. No.']).toBe('000000001111');

    const bob = rows.find((r) => r['WS Client Code'] === '100002');
    expect(bob['Client Name']).toBe('Bob LLC');
    expect(bob['ICICI Acc. No.']).toBe('000000002222');
  });

  it('does not flag Z0 rows with NONE accounts as missing balances', () => {
    const z0MasterRows = [
      {
        CLIENTCODE: '100004',
        CLIENTNAME: 'No Bank Client',
        RMNAME: 'Smith, John',
        'TRADING BANK ACCOUNT': 'NONE',
      },
    ];

    const { rows, exceptions } = buildDailyTracker(z0MasterRows, z10Rows, caRows);

    expect(rows[0]['ICICI Acc. No.']).toBe('N/A');
    expect(exceptions).not.toContainEqual(
      expect.objectContaining({ Issue: 'Missing Current Account Number' })
    );
    expect(exceptions).not.toContainEqual(
      expect.objectContaining({ Issue: 'Current Account Not Found In ICICI Balances' })
    );
  });

  it('merges balances using the Master as the base', () => {
    const { rows } = buildDailyTracker(masterRows, z10Rows, caRows);

    const alice = rows.find((r) => r['WS Client Code'] === '100001');
    expect(alice['RM Name']).toBe('Smith, John');
    expect(alice['ICICI Acc. No.']).toBe('000000001111');
    expect(alice['Liquid MF Balance']).toBe(150000);
    expect(alice['ICICI Bank Balance']).toBe(25000);
    expect(alice['Total Liquidity']).toBe(175000);

    const bob = rows.find((r) => r['WS Client Code'] === '100002');
    expect(bob['Liquid MF Balance']).toBe(50000);
    expect(bob['ICICI Bank Balance']).toBe(10000);
    expect(bob['Total Liquidity']).toBe(60000);
  });

  it('sorts by RM asc, Total Liquidity desc, blanks last', () => {
    const { rows } = buildDailyTracker(masterRows, z10Rows, caRows);
    expect(rows).toHaveLength(3);
    expect(rows[0]['RM Name']).toBe('Doe, Jane');
    expect(rows[1]['RM Name']).toBe('Smith, John');
    expect(rows[2]['WS Client Code']).toBe('100003');
    expect(rows[2]['RM Name']).toBe('');
  });

  it('generates correct summary statistics', () => {
    const { summary } = buildDailyTracker(masterRows, z10Rows, caRows);
    expect(summary.totalClients).toBe(3);
    expect(summary.totalRMs).toBe(2);
    expect(summary.totalIcici).toBe(35000);
    expect(summary.totalLiquidMf).toBe(200000);
    expect(summary.totalLiquidity).toBe(235000);
  });

  it('warns about missing RM names and missing Current Account matches', () => {
    const { warnings } = buildDailyTracker(masterRows, z10Rows, caRows);
    expect(warnings).toContain('1 client(s) in the Master have no RM Name.');
    expect(warnings).toContain('1 client(s) had no matching Current Account balance.');
  });
});

describe('buildDynamicMaster()', () => {
  it('maps WS Balances rows to the canonical master structure', () => {
    const ws = [
      {
        CLIENT_CODE: '100001',
        CLIENTNAME: 'Alice Corp',
        BANK_ACCOUNT: '000000001111',
      },
    ];
    const { rows } = buildDynamicMaster(ws);
    expect(rows).toEqual([
      {
        'WS Client Code': '100001',
        'Client Name': 'Alice Corp',
        'RM Name': 'Unassigned',
        'ICICI Acc. No.': '1111',
      },
    ]);
  });

  it('strips leading zeros from bank accounts', () => {
    const { rows } = buildDynamicMaster([
      { CLIENT_CODE: '100001', BANK_ACCOUNT: '00000123456' },
    ]);
    expect(rows[0]['ICICI Acc. No.']).toBe('123456');
  });

  it('falls back to NONE when the bank account is missing or all zeros', () => {
    const { rows } = buildDynamicMaster([
      { CLIENT_CODE: '100001', BANK_ACCOUNT: '' },
      { CLIENT_CODE: '100002', BANK_ACCOUNT: '0000' },
    ]);
    expect(rows[0]['ICICI Acc. No.']).toBe('NONE');
    expect(rows[1]['ICICI Acc. No.']).toBe('NONE');
  });

  it('skips rows without a client code', () => {
    const { rows } = buildDynamicMaster([
      { CLIENT_CODE: '', CLIENTNAME: 'No Code', BANK_ACCOUNT: '123' },
      { CLIENT_CODE: '100001', CLIENTNAME: 'Alice', BANK_ACCOUNT: '123' },
    ]);
    expect(rows).toHaveLength(1);
    expect(rows[0]['WS Client Code']).toBe('100001');
  });

  it('uses placeholder values when name is missing', () => {
    const { rows } = buildDynamicMaster([
      { CLIENT_CODE: '100001', BANK_ACCOUNT: '123' },
    ]);
    expect(rows[0]['Client Name']).toBe('Unknown');
    expect(rows[0]['RM Name']).toBe('Unassigned');
  });

  it('classifies non-ICICI accounts as N/A (other bank code or NONE + ledger code)', () => {
    const { rows } = buildDynamicMaster([
      { CLIENT_CODE: '100001', CLIENTNAME: 'A', BANK_ACCOUNT: '10444917642', BANK_CODE: 'SBI' },
      { CLIENT_CODE: '100002', CLIENTNAME: 'B', BANK_ACCOUNT: '50100927', BANK_CODE: 'None' },
      { CLIENT_CODE: '100003', CLIENTNAME: 'C', BANK_ACCOUNT: '000201113718', BANK_CODE: 'ICICI' },
    ]);
    expect(rows[0]['ICICI Acc. No.']).toBe('N/A');
    expect(rows[1]['ICICI Acc. No.']).toBe('N/A');
    expect(rows[2]['ICICI Acc. No.']).toBe('201113718');
  });

  it('finds RM names regardless of Z10 header casing', () => {
    const { rows } = buildDynamicMaster(
      [{ CLIENT_CODE: '100001', CLIENTNAME: 'Alice', BANK_ACCOUNT: '1111' }],
      [{ 'WS client id': '100001', 'RMNAME': 'Smith, John' }]
    );
    expect(rows[0]['RM Name']).toBe('Smith, John');
  });
});

describe('findTable() / classifyWorkbook()', () => {
  const z10Workbook = {
    kind: 'excel',
    sheetNames: ['Sheet2', 'Sheet1'],
    grids: {
      // Leftover pivot sheet FIRST — the shape that broke the old parser.
      Sheet2: [
        ['Sum of MKTVALUE', '', '', '', '', ''],
        ['SECURITY NAME', 'SECURITY TYPE DESCRIPTION', 'CLIENT NAME', 'RMNAME', 'LAST PRICE DATE', 'Total'],
        ['Fund A', 'AIF', 'Client X', 'RM 1', '30/06/2026', '100'],
      ],
      Sheet1: [
        ['WS CLIENT ID', 'SECURITY TYPE DESCRIPTION', 'ASTCLSNAME', 'MKTVALUE', 'RMNAME'],
        ['100001', 'Mutual Funds', 'Cash', '5000', 'RM 1'],
      ],
    },
  };

  it('skips pivot sheets and finds the real Z10 data sheet', () => {
    const table = findTable(z10Workbook, ['WS CLIENT ID', 'SECURITY TYPE DESCRIPTION', 'ASTCLSNAME', 'MKTVALUE']);
    expect(table.sheetName).toBe('Sheet1');
    expect(table.headerRowIndex).toBe(0);
    expect(table.rows).toHaveLength(1);
    expect(table.rows[0]['WS CLIENT ID']).toBe('100001');
  });

  it('requires exact header equality — pivot title "Sum of MKTVALUE" never matches MKTVALUE', () => {
    const pivotOnly = {
      kind: 'excel',
      sheetNames: ['Sheet2'],
      grids: { Sheet2: z10Workbook.grids.Sheet2 },
    };
    expect(findTable(pivotOnly, ['WS CLIENT ID', 'SECURITY TYPE DESCRIPTION', 'ASTCLSNAME', 'MKTVALUE'])).toBeNull();
  });

  it('finds a header row sitting below title rows', () => {
    const parsed = {
      kind: 'excel',
      sheetNames: ['S'],
      grids: {
        S: [
          ['OPERATIVE ACCOUNTS', '', ''],
          [],
          ['Account Number', 'Account Name', 'Available Balance'],
          ['000000001111', 'ALICE', '25000'],
        ],
      },
    };
    const table = findTable(parsed, ['Account Number', 'Available Balance']);
    expect(table.headerRowIndex).toBe(2);
    expect(table.rows).toHaveLength(1);
    expect(table.rows[0]['Account Number']).toBe('000000001111');
  });

  it('classifies each report by content fingerprint', () => {
    const recon = {
      kind: 'excel',
      sheetNames: ['Bank Recon 270726', 'WS Balances', 'ICICI Balances'],
      grids: {
        'Bank Recon 270726': [['Bank Code', 'Balance Date', 'Bank Account']],
        'WS Balances': [
          ['CLIENT_CODE', 'CLIENTNAME', 'BANK_ACCOUNT'],
          ['100001', 'Alice', '000000001111'],
        ],
        'ICICI Balances': [
          ['Account Number', 'Account Name', 'Effective Balance', 'Available Balance'],
          ['000000001111', 'ALICE', '25000', '25000'],
        ],
      },
    };
    const reconCls = classifyWorkbook(recon);
    expect(reconCls.role).toBe('recon');
    expect(reconCls.wsRows).toHaveLength(1);
    expect(reconCls.caRows).toHaveLength(1);

    expect(classifyWorkbook(z10Workbook).role).toBe('z10');

    const cw = {
      kind: 'excel',
      sheetNames: ['Sheet 1'],
      grids: {
        'Sheet 1': [
          ['Client Code', 'Client Name', 'Dealer Name', 'Ent Id', 'Ledger Bal BOD'],
          ['8452602', 'ROHIT  BHATIA', 'Onza', '', '62485.8'],
        ],
      },
    };
    expect(classifyWorkbook(cw).role).toBe('cw');

    const z2 = {
      kind: 'excel',
      sheetNames: ['Sheet1'],
      grids: {
        Sheet1: [
          ['CLIENTID', 'CLIENTNAME', 'CLIENTCODE', 'RMNAME', 'TRADING BANK ACCOUNT'],
          ['100001', 'Alice', '100001', 'Smith, John', '000000001111'],
        ],
      },
    };
    expect(classifyWorkbook(z2).role).toBe('z2');

    const clhr = {
      kind: 'excel',
      sheetNames: ['Report'],
      grids: {
        Report: [
          ['Orgranization Name', 'Ent Id', 'Ent Name', 'ISIN', 'Free Valuation'],
          ['ASL', '8691620', 'JAYANTH', 'INE090A01021', '2866300'],
        ],
      },
    };
    expect(classifyWorkbook(clhr).role).toBe('clhr');

    expect(classifyWorkbook({ kind: 'excel', sheetNames: ['X'], grids: { X: [['Foo', 'Bar']] } }).role).toBe('unknown');
  });
});

describe('buildDailyTracker() — business rules & Z2 enrichment', () => {
  const meta = { fileName: 'recon.xlsx', sheetName: 'WS Balances' };

  it('excludes test clients (RM "Test RM" / dummy account) from rows and totals', () => {
    const master = [
      { 'WS Client Code': '100007', 'Client Name': 'ABC', 'RM Name': 'Test RM', 'ICICI Acc. No.': '123456789123' },
      { 'WS Client Code': '100001', 'Client Name': 'Alice', 'RM Name': 'Smith', 'ICICI Acc. No.': '1111' },
    ];
    const z10 = [
      { 'WS CLIENT ID': '100007', 'SECURITY TYPE DESCRIPTION': 'Mutual Funds', 'ASTCLSNAME': 'Cash', 'MKTVALUE': '99999', 'RMNAME': 'Test RM' },
      { 'WS CLIENT ID': '100001', 'SECURITY TYPE DESCRIPTION': 'Mutual Funds', 'ASTCLSNAME': 'Cash', 'MKTVALUE': '5000', 'RMNAME': 'Smith' },
    ];
    const ca = [{ 'Account Number': '000000001111', 'Available Balance': '25000' }];

    const { rows, summary, stats, warnings } = buildDailyTracker(master, z10, ca, [], meta);
    expect(rows.find((r) => r['WS Client Code'] === '100007')).toBeUndefined();
    expect(summary.totalLiquidMf).toBe(5000);
    expect(stats.testClientsExcluded).toBe(1);
    expect(warnings.some((w) => w.includes('test client'))).toBe(true);
  });

  it("keeps Onza's corporate account as a row under Nikhil S Shah with no exception", () => {
    const master = [
      { 'WS Client Code': '100001', 'Client Name': 'Alice', 'RM Name': 'Smith', 'ICICI Acc. No.': '1111' },
    ];
    const z10 = [
      { 'WS CLIENT ID': '100001', 'SECURITY TYPE DESCRIPTION': 'Mutual Funds', 'ASTCLSNAME': 'Cash', 'MKTVALUE': '5000' },
    ];
    const ca = [
      { 'Account Number': '000000001111', 'Effective Balance': '25000' },
      { 'Account Number': '000205040390', 'Account Name': 'ONZA FINANCIAL SERVICES PVT LTD', 'Effective Balance': '6404' },
    ];

    const { rows, exceptions, autoResolved, summary } = buildDailyTracker(master, z10, ca, [], meta);
    const onza = rows.find((r) => r['ICICI Acc. No.'] === '000205040390');
    expect(onza).toBeDefined();
    expect(onza['RM Name']).toBe('Nikhil S Shah');
    expect(onza['ICICI Bank Balance']).toBe(6404);
    expect(summary.totalIcici).toBe(31404);
    expect(exceptions.filter((e) => e['ICICI Acc. No.'] === '000205040390')).toHaveLength(0);
    expect(autoResolved.some((a) => a['ICICI Acc. No.'] === '000205040390')).toBe(true);
  });

  it('fills missing/Unassigned RM names from Z2 and resolves orphans', () => {
    const master = [
      { 'WS Client Code': '100003', 'Client Name': 'Charlie Inc', 'RM Name': 'Unassigned', 'ICICI Acc. No.': 'NONE' },
    ];
    const z10 = [
      { 'WS CLIENT ID': '100003', 'SECURITY TYPE DESCRIPTION': 'Mutual Funds', 'ASTCLSNAME': 'Cash', 'MKTVALUE': '5000' },
    ];
    const ca = [
      { 'Account Number': '772201075263', 'Account Name': 'TINA KAPUR', 'Effective Balance': '100' },
    ];
    const cw = [{ 'Client Name': 'KENAI  ADVISORS LLP', 'Ledger Bal BOD': '2000' }];
    const z2Rows = [
      { CLIENTID: '100003', CLIENTCODE: '100003', CLIENTNAME: 'Charlie Inc', RMNAME: 'Doe, Jane', 'TRADING BANK ACCOUNT': 'NONE' },
      { CLIENTID: '100215', CLIENTCODE: '100215', CLIENTNAME: 'Tina Kapur', RMNAME: 'Rohit Bhatia', 'TRADING BANK ACCOUNT': '772201075263' },
      { CLIENTID: '100218', CLIENTCODE: '100218', CLIENTNAME: 'Kenai Advisors LLP', RMNAME: 'Prashant K Shastri', 'TRADING BANK ACCOUNT': 'NONE' },
    ];

    const { rows, exceptions, autoResolved, stats } = buildDailyTracker(master, z10, ca, cw, meta, z2Rows);

    expect(rows.find((r) => r['WS Client Code'] === '100003')['RM Name']).toBe('Doe, Jane');
    expect(stats.rmFilledFromZ2).toBe(1);

    const tina = rows.find((r) => r['WS Client Code'] === '100215');
    expect(tina).toBeDefined();
    expect(tina['RM Name']).toBe('Rohit Bhatia');
    expect(tina['ICICI Bank Balance']).toBe(100);

    const kenai = rows.find((r) => r['WS Client Code'] === '100218');
    expect(kenai).toBeDefined();
    expect(kenai['RM Name']).toBe('Prashant K Shastri');
    expect(kenai['Axis Demat Balance']).toBe(2000);

    expect(exceptions.filter((e) => e['Issue'].includes('Without Master Record'))).toHaveLength(0);
    expect(autoResolved).toHaveLength(2);
  });

  it('surfaces Unassigned RM as a Missing RM Name exception', () => {
    const master = [
      { 'WS Client Code': '100005', 'Client Name': 'Eve', 'RM Name': 'Unassigned', 'ICICI Acc. No.': 'NONE' },
    ];
    const z10 = [
      { 'WS CLIENT ID': '100005', 'SECURITY TYPE DESCRIPTION': 'Equity', 'ASTCLSNAME': 'Stock', 'MKTVALUE': '1' },
    ];
    const { exceptions } = buildDailyTracker(master, z10, [{ 'Account Number': '9', 'Available Balance': '0' }], [], meta);
    expect(exceptions.some((e) => e['Issue'] === 'Missing RM Name' && e['Client Code'] === '100005')).toBe(true);
  });

  it('joins Axis balances despite double internal spaces in names', () => {
    const master = [
      { 'WS Client Code': '100186', 'Client Name': 'Vellachi Murugappan', 'RM Name': 'Smith', 'ICICI Acc. No.': 'N/A' },
    ];
    const z10 = [
      { 'WS CLIENT ID': '100186', 'SECURITY TYPE DESCRIPTION': 'Equity', 'ASTCLSNAME': 'Stock', 'MKTVALUE': '1' },
    ];
    const cw = [{ 'Client Name': 'VELLACHI  MURUGAPPAN', 'Ledger Bal BOD': '1500' }];

    const { rows, exceptions } = buildDailyTracker(master, z10, [{ 'Account Number': '9', 'Available Balance': '0' }], cw, meta);
    expect(rows.find((r) => r['WS Client Code'] === '100186')['Axis Demat Balance']).toBe(1500);
    expect(exceptions.filter((e) => e['Issue'].startsWith('Axis Demat'))).toHaveLength(0);
  });

  it('classifies a CSV parsed as a grid, trimming padded headers', () => {
    const parsed = {
      kind: 'grid',
      sheetNames: ['CSV'],
      grids: {
        CSV: [
          [' Client Code ', ' Client Name ', ' Ledger Bal BOD '],
          ['8452602', 'ROHIT  BHATIA', '62485.8'],
        ],
      },
    };
    const cls = classifyWorkbook(parsed);
    expect(cls.role).toBe('cw');
    expect(cls.cwRows[0]['Client Name']).toBe('ROHIT  BHATIA');
    expect(cls.cwRows[0]['Ledger Bal BOD']).toBe('62485.8');
  });

  it('does not let excluded test-client balances re-enter via the orphan loops', () => {
    const master = [
      { 'WS Client Code': '100007', 'Client Name': 'ABC', 'RM Name': 'Test RM', 'ICICI Acc. No.': '4444' },
      { 'WS Client Code': '100001', 'Client Name': 'Alice', 'RM Name': 'Smith', 'ICICI Acc. No.': '1111' },
    ];
    const z10 = [
      { 'WS CLIENT ID': '100001', 'SECURITY TYPE DESCRIPTION': 'Equity', 'ASTCLSNAME': 'Stock', 'MKTVALUE': '1' },
    ];
    const ca = [
      { 'Account Number': '000000001111', 'Effective Balance': '25000' },
      { 'Account Number': '000000004444', 'Effective Balance': '9999' },
    ];
    const cw = [{ 'Client Name': 'ABC', 'Ledger Bal BOD': '777' }];

    const { rows, summary } = buildDailyTracker(master, z10, ca, cw, meta);
    expect(summary.totalIcici).toBe(25000);
    expect(summary.axisTotal).toBe(0);
    expect(rows.some((r) => r['ICICI Acc. No.'] === '000000004444')).toBe(false);
  });

  it('does not resurrect auto-resolved orphans as exceptions after an Apply-Fixes rebuild', () => {
    const master = [
      { 'WS Client Code': '100001', 'Client Name': 'Alice', 'RM Name': 'Smith', 'ICICI Acc. No.': '1111' },
    ];
    const z10 = [
      { 'WS CLIENT ID': '100001', 'SECURITY TYPE DESCRIPTION': 'Equity', 'ASTCLSNAME': 'Stock', 'MKTVALUE': '1' },
      { 'WS CLIENT ID': '100300', 'SECURITY TYPE DESCRIPTION': 'Mutual Funds', 'ASTCLSNAME': 'Cash', 'MKTVALUE': '7000' },
    ];
    const ca = [{ 'Account Number': '000000001111', 'Effective Balance': '25000' }];
    const z2Rows = [
      { CLIENTID: '100300', CLIENTCODE: '100300', CLIENTNAME: 'Bob Corp', RMNAME: 'Doe', 'TRADING BANK ACCOUNT': 'NONE' },
    ];

    const first = buildDailyTracker(master, z10, ca, [], meta, z2Rows);
    expect(first.exceptions).toHaveLength(0);

    // Apply-Fixes rebuild path: master rebuilt from finalMasterRows minus Status.
    const rebuiltMaster = first.finalMasterRows.map(({ Status, ...r }) => r);
    const second = buildDailyTracker(rebuiltMaster, z10, ca, [], meta, z2Rows);
    expect(second.exceptions.filter((e) => e['Issue'] === 'Missing Current Account Number')).toHaveLength(0);
  });

  it('never emits two rows for a client that is both a Z10 orphan and a Z2 account match', () => {
    const master = [
      { 'WS Client Code': '100001', 'Client Name': 'Alice', 'RM Name': 'Smith', 'ICICI Acc. No.': '1111' },
    ];
    const z10 = [
      { 'WS CLIENT ID': '100001', 'SECURITY TYPE DESCRIPTION': 'Equity', 'ASTCLSNAME': 'Stock', 'MKTVALUE': '1' },
      { 'WS CLIENT ID': '100300', 'SECURITY TYPE DESCRIPTION': 'Mutual Funds', 'ASTCLSNAME': 'Cash', 'MKTVALUE': '7000' },
    ];
    const ca = [
      { 'Account Number': '000000001111', 'Effective Balance': '25000' },
      { 'Account Number': '772201075263', 'Effective Balance': '300' },
    ];
    const z2Rows = [
      { CLIENTID: '100300', CLIENTCODE: '100300', CLIENTNAME: 'Bob Corp', RMNAME: 'Doe', 'TRADING BANK ACCOUNT': '772201075263' },
    ];

    const { rows, finalMasterRows } = buildDailyTracker(master, z10, ca, [], meta, z2Rows);
    expect(rows.filter((r) => r['WS Client Code'] === '100300')).toHaveLength(1);
    expect(finalMasterRows.filter((r) => r['WS Client Code'] === '100300')).toHaveLength(1);
  });

  it('attaches the Axis balance when an ICICI orphan is resolved via Z2', () => {
    const master = [
      { 'WS Client Code': '100001', 'Client Name': 'Alice', 'RM Name': 'Smith', 'ICICI Acc. No.': '1111' },
    ];
    const z10 = [
      { 'WS CLIENT ID': '100001', 'SECURITY TYPE DESCRIPTION': 'Equity', 'ASTCLSNAME': 'Stock', 'MKTVALUE': '1' },
    ];
    const ca = [
      { 'Account Number': '000000001111', 'Effective Balance': '25000' },
      { 'Account Number': '772201075263', 'Account Name': 'BOB CORP', 'Effective Balance': '300' },
    ];
    const cw = [{ 'Client Name': 'BOB  CORP', 'Ledger Bal BOD': '888' }];
    const z2Rows = [
      { CLIENTID: '100300', CLIENTCODE: '100300', CLIENTNAME: 'Bob Corp', RMNAME: 'Doe', 'TRADING BANK ACCOUNT': '772201075263' },
    ];

    const { rows, exceptions } = buildDailyTracker(master, z10, ca, cw, meta, z2Rows);
    const bob = rows.find((r) => r['WS Client Code'] === '100300');
    expect(bob['Axis Demat Balance']).toBe(888);
    expect(bob['Total Liquidity']).toBe(1188);
    expect(exceptions.filter((e) => e['Issue'].startsWith('Axis Demat'))).toHaveLength(0);
  });

  it('credits a shared account once even when the balance is negative', () => {
    const master = [
      { 'WS Client Code': '100001', 'Client Name': 'A', 'RM Name': 'X', 'ICICI Acc. No.': '3333' },
      { 'WS Client Code': '100002', 'Client Name': 'B', 'RM Name': 'X', 'ICICI Acc. No.': '3333' },
    ];
    const z10 = [
      { 'WS CLIENT ID': '100001', 'SECURITY TYPE DESCRIPTION': 'Equity', 'ASTCLSNAME': 'Stock', 'MKTVALUE': '1' },
    ];
    const ca = [{ 'Account Number': '000000003333', 'Effective Balance': '-500' }];

    const { summary } = buildDailyTracker(master, z10, ca, [], meta);
    expect(summary.totalIcici).toBe(-500);
  });
});

describe('mergeTrackerRows()', () => {
  const makeResult = () => {
    const rows = [
      { 'RM Name': 'Smith', 'WS Client Code': '100006', 'Client Name': 'Ramamoorthi Jagadish', 'ICICI Acc. No.': '000201113729', 'ICICI Bank Balance': 1000, 'Liquid MF Balance': 500, 'Axis Demat Balance': 0, 'Total Liquidity': 1500 },
      { 'RM Name': '', 'WS Client Code': '', 'Client Name': 'R JAGADISH', 'ICICI Acc. No.': '', 'ICICI Bank Balance': 0, 'Liquid MF Balance': 0, 'Axis Demat Balance': 250, 'Total Liquidity': 250 },
      { 'RM Name': 'Doe', 'WS Client Code': '100010', 'Client Name': 'Unrelated', 'ICICI Acc. No.': 'N/A', 'ICICI Bank Balance': 42, 'Liquid MF Balance': 0, 'Axis Demat Balance': 0, 'Total Liquidity': 42 },
    ];
    return {
      rows,
      summary: { totalClients: 3, totalRMs: 2, totalIcici: 1042, totalLiquidMf: 500, axisTotal: 250, totalLiquidity: 1792 },
    };
  };

  it('keeps the first-selected identity and sums all financial columns', () => {
    const result = makeResult();
    const merged = mergeTrackerRows(result, [[result.rows[0]], [result.rows[1]]]);

    expect(merged.rows).toHaveLength(2);
    const primary = merged.rows[0];
    expect(primary['Client Name']).toBe('Ramamoorthi Jagadish');
    expect(primary['WS Client Code']).toBe('100006');
    expect(primary['RM Name']).toBe('Smith');
    expect(primary['ICICI Acc. No.']).toBe('000201113729');
    expect(primary['ICICI Bank Balance']).toBe(1000);
    expect(primary['Axis Demat Balance']).toBe(250);
    expect(primary['Total Liquidity']).toBe(1750);
    expect(merged.rows.some((r) => r['Client Name'] === 'R JAGADISH')).toBe(false);
  });

  it('preserves grand totals and updates the client/RM counts', () => {
    const result = makeResult();
    const merged = mergeTrackerRows(result, [[result.rows[1]], [result.rows[0]]]);

    const total = merged.rows.reduce((s, r) => s + r['Total Liquidity'], 0);
    expect(total).toBe(1792);
    expect(merged.summary.totalClients).toBe(2);
    // Selection order matters: R JAGADISH was ticked first, so its blank RM wins.
    expect(merged.rows[0]['Client Name']).toBe('R JAGADISH');
    expect(merged.rows[0]['RM Name']).toBe('');
    expect(merged.summary.totalRMs).toBe(1);
  });

  it('returns the result unchanged for fewer than two rows', () => {
    const result = makeResult();
    expect(mergeTrackerRows(result, [[result.rows[0]]])).toBe(result);
    expect(mergeTrackerRows(result, [])).toBe(result);
  });

  it('does not mutate the original rows', () => {
    const result = makeResult();
    mergeTrackerRows(result, [[result.rows[0]], [result.rows[1]]]);
    expect(result.rows).toHaveLength(3);
    expect(result.rows[0]['Total Liquidity']).toBe(1500);
  });
});

describe('buildDailyTracker() — saved manual orphan merges', () => {
  const meta = { fileName: 'recon.xlsx', sheetName: 'WS Balances' };
  const master = [
    { 'WS Client Code': '100006', 'Client Name': 'Ramamoorthi Jagadish', 'RM Name': 'Prashant', 'ICICI Acc. No.': '1111' },
  ];
  const z10 = [
    { 'WS CLIENT ID': '100006', 'SECURITY TYPE DESCRIPTION': 'Equity', 'ASTCLSNAME': 'Stock', 'MKTVALUE': '1' },
  ];
  const ca = [
    { 'Account Number': '000000001111', 'Effective Balance': '5000' },
    { 'Account Number': '000205041622', 'Account Name': 'BIZBOOST BUSINESS SOLUTIONS LLP', 'Effective Balance': '700' },
  ];
  const cw = [{ 'Client Name': 'R JAGADISH', 'Ledger Bal BOD': '250' }];

  it('merges an Axis-name orphan into an existing client and drops the exception', () => {
    const merges = { axisNames: { 'R JAGADISH': '100006' }, accounts: {} };
    const { rows, exceptions, autoResolved } = buildDailyTracker(master, z10, ca, cw, meta, null, merges);

    const target = rows.find((r) => r['WS Client Code'] === '100006');
    expect(target['Axis Demat Balance']).toBe(250);
    expect(target['Total Liquidity']).toBe(5250);
    expect(rows.some((r) => r['Client Name'] === 'R JAGADISH')).toBe(false);
    expect(exceptions.filter((e) => e['Issue'].startsWith('Axis Demat'))).toHaveLength(0);
    expect(autoResolved.some((a) => a['Resolved From'] === 'Saved manual mapping')).toBe(true);
  });

  it('merges an ICICI-account orphan into an existing client', () => {
    const merges = { axisNames: {}, accounts: { '000205041622': '100006' } };
    const { rows, exceptions } = buildDailyTracker(master, z10, ca, cw, meta, null, merges);

    const target = rows.find((r) => r['WS Client Code'] === '100006');
    expect(target['ICICI Bank Balance']).toBe(5700);
    expect(rows.some((r) => r['ICICI Acc. No.'] === '000205041622')).toBe(false);
    expect(exceptions.filter((e) => e['Issue'].startsWith('ICICI Balance Without'))).toHaveLength(0);
  });

  it('preserves the grand total when merging (money only moves rows)', () => {
    const plain = buildDailyTracker(master, z10, ca, cw, meta, null, null);
    const merged = buildDailyTracker(master, z10, ca, cw, meta, null, {
      axisNames: { 'R JAGADISH': '100006' },
      accounts: { '000205041622': '100006' },
    });
    expect(merged.summary.totalLiquidity).toBeCloseTo(plain.summary.totalLiquidity, 5);
  });

  it('is a no-op when the target code does not exist', () => {
    const merges = { axisNames: { 'R JAGADISH': '999999' }, accounts: {} };
    const { rows, exceptions } = buildDailyTracker(master, z10, ca, cw, meta, null, merges);
    expect(rows.some((r) => r['Client Name'] === 'R JAGADISH')).toBe(true);
    expect(exceptions.filter((e) => e['Issue'].startsWith('Axis Demat'))).toHaveLength(1);
  });
});

describe('buildDailyTracker() — manual merge resolution ordering', () => {
  const meta = { fileName: 'recon.xlsx', sheetName: 'WS Balances' };

  it('resolves an ICICI merge onto a Z2-Axis-derived client created in a later loop', () => {
    // 100200 exists ONLY via the Z2 Axis-orphan path (created after the ICICI loop).
    const master = [
      { 'WS Client Code': '100006', 'Client Name': 'Anchor', 'RM Name': 'Prashant', 'ICICI Acc. No.': '1111' },
    ];
    const z10 = [
      { 'WS CLIENT ID': '100006', 'SECURITY TYPE DESCRIPTION': 'Equity', 'ASTCLSNAME': 'Stock', 'MKTVALUE': '1' },
    ];
    const ca = [
      { 'Account Number': '000000001111', 'Effective Balance': '5000' },
      { 'Account Number': '000000999888', 'Account Name': 'MYSTERY LLP', 'Effective Balance': '700' },
    ];
    const cw = [{ 'Client Name': 'SOME AXIS CLIENT', 'Ledger Bal BOD': '300' }];
    const z2Rows = [
      { CLIENTID: '100200', CLIENTCODE: '100200', CLIENTNAME: 'Some Axis Client', RMNAME: 'Rohit', 'TRADING BANK ACCOUNT': 'NONE' },
    ];
    const merges = { axisNames: {}, accounts: { '000000999888': '100200' } };

    const { rows, exceptions, autoResolved } = buildDailyTracker(master, z10, ca, cw, meta, z2Rows, merges);

    const target = rows.find((r) => r['WS Client Code'] === '100200');
    expect(target['ICICI Bank Balance']).toBe(700); // credited despite later creation
    expect(target['Axis Demat Balance']).toBe(300);
    expect(rows.some((r) => r['ICICI Acc. No.'] === '000000999888')).toBe(false);
    expect(exceptions.filter((e) => e['Issue'].startsWith('ICICI Balance Without'))).toHaveLength(0);
    expect(autoResolved.some((a) => a['Resolved From'] === 'Saved manual mapping' && a['ICICI Acc. No.'] === '000000999888')).toBe(true);
  });

  it('falls back to an orphan row + exception when the mapping target no longer exists', () => {
    const master = [
      { 'WS Client Code': '100006', 'Client Name': 'Anchor', 'RM Name': 'Prashant', 'ICICI Acc. No.': '1111' },
    ];
    const z10 = [
      { 'WS CLIENT ID': '100006', 'SECURITY TYPE DESCRIPTION': 'Equity', 'ASTCLSNAME': 'Stock', 'MKTVALUE': '1' },
    ];
    const ca = [
      { 'Account Number': '000000001111', 'Effective Balance': '5000' },
      { 'Account Number': '000000999888', 'Account Name': 'MYSTERY LLP', 'Effective Balance': '700' },
    ];
    const merges = { axisNames: {}, accounts: { '000000999888': '999999' } }; // 999999 gone

    const { rows, exceptions, summary } = buildDailyTracker(master, z10, ca, [], meta, null, merges);
    expect(rows.some((r) => r['ICICI Acc. No.'] === '000000999888')).toBe(true);
    expect(exceptions.filter((e) => e['Issue'].startsWith('ICICI Balance Without'))).toHaveLength(1);
    expect(summary.totalIcici).toBe(5700); // money still present and conserved
  });
})

describe('classifyWorkbook() — balances-only Current Account export', () => {
  // Shape of the raw ICICI portal download: two title rows, the table, a blank
  // row, then a "Date and Time" footer.
  const caWorkbook = {
    kind: 'excel',
    sheetNames: ['Sheet0'],
    grids: {
      Sheet0: [
        ['                                OPERATIVE ACCOUNTS', '', '', '', ''],
        [' OPERATIVE ACCOUNTS(2)', '', '', '', ''],
        ['Account Number', 'Account Name', 'Currency', 'Available Balance', 'Effective Balance'],
        ['000000001111', 'ALICE POA A/C', 'INR', '25000.00', '25000.00'],
        ['000000002222', 'BOB POA A/C', 'INR', '10000.00', '7000.00'],
        ['', '', '', '', ''],
        ['Date and Time: ', '08-05-2026     13:38:59', '', '', ''],
      ],
    },
  };

  it('classifies the raw ICICI portal export as "ca" with its rows and export stamp', () => {
    const cls = classifyWorkbook(caWorkbook);
    expect(cls.role).toBe('ca');
    expect(cls.sheet).toBe('Sheet0');
    expect(cls.caRows[0]['Account Number']).toBe('000000001111');
    expect(cls.caRows[1]['Effective Balance']).toBe('7000.00');
    expect(cls.exportedAt).toBe('08-05-2026 13:38:59');
  });

  it('feeds the build exactly like the recon ICICI sheet does (footer row is inert)', () => {
    const { caRows } = classifyWorkbook(caWorkbook);
    const master = [
      { 'WS Client Code': '100001', 'Client Name': 'Alice', 'RM Name': 'RM 1', 'ICICI Acc. No.': '1111' },
      { 'WS Client Code': '100002', 'Client Name': 'Bob', 'RM Name': 'RM 1', 'ICICI Acc. No.': '2222' },
    ];
    const z10 = [{ 'WS CLIENT ID': '100001', 'SECURITY TYPE DESCRIPTION': 'Equity', 'ASTCLSNAME': 'Stock', 'MKTVALUE': '1' }];
    const r = buildDailyTracker(master, z10, caRows, [], {}, null, null);
    expect(r.rows.map((row) => row['ICICI Bank Balance'])).toEqual([25000, 7000]);
    expect(r.summary.totalIcici).toBe(32000);
    expect(r.exceptions).toHaveLength(0);
  });

  // Real recon summary sheet: space-padded headers, SheetJS m/d/yy dates.
  const reconSummaryGrid = [
    ['Bank Code', 'Balance Date', 'Bank Account', 'Client Name', ' Sum of Bank Balance (Custody) ', ' Adjustments ', ' Sum of Bank Balance (System) ', ' Diff ', ' In-Transit ', 'Remarks'],
    ['ICICI', '7/14/26', '000000001111', 'ALICE POA A/C', ' 25,000.00 ', ' -   ', ' 25,000.00 ', ' -   ', ' -   ', 'No Action, Match'],
  ];
  const reconWorkbook = {
    kind: 'excel',
    sheetNames: ['Bank Recon 140726', 'WS Balances', 'ICICI Balances'],
    grids: {
      'Bank Recon 140726': reconSummaryGrid,
      'WS Balances': [
        ['CLIENT_CODE', 'CLIENTNAME', 'BANK_ACCOUNT', 'AS ON DATE'],
        ['100001', 'Alice', '000000001111', '12/07/2026'],
      ],
      'ICICI Balances': caWorkbook.grids.Sheet0.slice(2, 5),
    },
  };

  it('still classifies the full Bank Recon workbook as "recon", never "ca"', () => {
    const cls = classifyWorkbook(reconWorkbook);
    expect(cls.role).toBe('recon');
    expect(cls.caSheet).toBe('ICICI Balances');
    expect(cls.balanceDate).toBe('7/14/26');
  });

  it('leaves balanceDate empty for a recon without a summary sheet', () => {
    const twoSheets = {
      kind: 'excel',
      sheetNames: ['WS Balances', 'ICICI Balances'],
      grids: { 'WS Balances': reconWorkbook.grids['WS Balances'], 'ICICI Balances': reconWorkbook.grids['ICICI Balances'] },
    };
    const cls = classifyWorkbook(twoSheets);
    expect(cls.role).toBe('recon');
    expect(cls.balanceDate).toBe('');
  });

  it('rejects a Bank Recon whose WS Balances sheet is missing or mis-headed, with the old hint', () => {
    const withoutWs = {
      kind: 'excel',
      sheetNames: ['Bank Recon 140726', 'ICICI Balances'],
      grids: { 'Bank Recon 140726': reconSummaryGrid, 'ICICI Balances': reconWorkbook.grids['ICICI Balances'] },
    };
    const drifted = {
      kind: 'excel',
      sheetNames: reconWorkbook.sheetNames,
      grids: {
        ...reconWorkbook.grids,
        'WS Balances': [['CLIENT CODE', 'CLIENTNAME', 'BANK_ACCOUNT'], ['100001', 'Alice', '000000001111']],
      },
    };
    for (const wb of [withoutWs, drifted]) {
      const cls = classifyWorkbook(wb);
      expect(cls.role).toBe('unknown');
      expect(cls.hint).toContain('no WS Balances sheet');
    }
  });

  it('never accepts the recon summary sheet on its own as Current Account Data', () => {
    const summaryOnly = { kind: 'excel', sheetNames: ['Bank Recon 140726'], grids: { 'Bank Recon 140726': reconSummaryGrid } };
    const cls = classifyWorkbook(summaryOnly);
    expect(cls.role).toBe('unknown');
    expect(cls.hint).toContain('no WS Balances sheet');
  });

  it('reads the footer stamp of a full-size export, well past findTable\'s 40-row scan window', () => {
    const accounts = Array.from({ length: 70 }, (_, i) => [
      String(201113700 + i).padStart(12, '0'), `CLIENT ${i} POA A/C`, 'INR', '100.00', '100.00',
    ]);
    const bigExport = {
      kind: 'excel',
      sheetNames: ['Sheet0'],
      grids: { Sheet0: [...caWorkbook.grids.Sheet0.slice(0, 3), ...accounts, ['', '', '', '', ''], ['Date and Time: ', '08-05-2026     13:38:59', '', '', '']] },
    };
    const cls = classifyWorkbook(bigExport);
    expect(cls.role).toBe('ca');
    expect(cls.caRows.filter((r) => /^\d{12}$/.test(r['Account Number']))).toHaveLength(70);
    expect(cls.exportedAt).toBe('08-05-2026 13:38:59');
  });

  it('keeps the hint for a WS-Balances-only workbook', () => {
    const wsOnly = {
      kind: 'excel',
      sheetNames: ['WS Balances'],
      grids: { 'WS Balances': [['CLIENT_CODE', 'CLIENTNAME', 'BANK_ACCOUNT'], ['100001', 'Alice', '000000001111']] },
    };
    const cls = classifyWorkbook(wsOnly);
    expect(cls.role).toBe('unknown');
    expect(cls.hint).toContain('no ICICI account sheet');
  });

  it('returns an empty export stamp when the footer is missing', () => {
    const noFooter = { kind: 'excel', sheetNames: ['S'], grids: { S: caWorkbook.grids.Sheet0.slice(0, 5) } };
    expect(classifyWorkbook(noFooter).exportedAt).toBe('');
    expect(findCurrentAccountExportDate(noFooter)).toBe('');
  });

  it('reads the footer stamp whether the label and value share a cell or not', () => {
    const oneCell = { kind: 'grid', sheetNames: ['CSV'], grids: { CSV: [['x'], ['Date and Time: 11-05-2026 11:28:28']] } };
    expect(findCurrentAccountExportDate(oneCell)).toBe('11-05-2026 11:28:28');
    const twoCells = { kind: 'grid', sheetNames: ['CSV'], grids: { CSV: [['Date & Time', '', '27-04-2026   11:23:19']] } };
    expect(findCurrentAccountExportDate(twoCells)).toBe('27-04-2026 11:23:19');
  });
});

describe('parseSourceDate()', () => {
  const iso = (d) => d.toISOString().slice(0, 10);

  it('reads day-first dates with a 4-digit year (WS Balances / ICICI footer)', () => {
    expect(iso(parseSourceDate('14/07/2026'))).toBe('2026-07-14');
    expect(iso(parseSourceDate('08-05-2026 13:38:59'))).toBe('2026-05-08');
    expect(iso(parseSourceDate('8-5-2026'))).toBe('2026-05-08');
  });

  it('reads ISO and SheetJS m/d/yy renderings of true date cells', () => {
    expect(iso(parseSourceDate('2026-07-14'))).toBe('2026-07-14');
    expect(iso(parseSourceDate('7/14/26'))).toBe('2026-07-14');
    expect(iso(parseSourceDate('14/7/26'))).toBe('2026-07-14');
  });

  it('reads month-name renderings of d-mmm-yy style cells', () => {
    expect(iso(parseSourceDate('14-Jul-26'))).toBe('2026-07-14');
    expect(iso(parseSourceDate('14-Jul-2026'))).toBe('2026-07-14');
    expect(iso(parseSourceDate('8-May-26 19:08'))).toBe('2026-05-08');
    expect(iso(parseSourceDate('14 July 2026'))).toBe('2026-07-14');
    expect(iso(parseSourceDate('Jul 14, 2026'))).toBe('2026-07-14');
    expect(iso(parseSourceDate('1-Sept-2026'))).toBe('2026-09-01');
    expect(parseSourceDate('14-Foo-2026')).toBeNull();
  });

  it('passes Date instances through and rejects blanks, text and impossible dates', () => {
    const d = new Date(Date.UTC(2026, 6, 14));
    expect(parseSourceDate(d)).toBe(d);
    expect(parseSourceDate('')).toBeNull();
    expect(parseSourceDate(undefined)).toBeNull();
    expect(parseSourceDate(null)).toBeNull();
    expect(parseSourceDate('as on date')).toBeNull();
    expect(parseSourceDate('31/02/2026')).toBeNull();
    expect(parseSourceDate('14/13/2026')).toBeNull();
    expect(parseSourceDate('08-05-20261')).toBeNull();
  });
});

describe('buildDynamicMaster() — as-on date', () => {
  it('returns the WS Balances AS ON DATE from the first populated client row', () => {
    const { asOnDate } = buildDynamicMaster(
      [
        { CLIENT_CODE: '', CLIENTNAME: '', BANK_ACCOUNT: '', 'AS ON DATE': '01/01/2000' },
        { CLIENT_CODE: '100001', CLIENTNAME: 'Alice', BANK_ACCOUNT: '000000001111', 'AS ON DATE': '' },
        { CLIENT_CODE: '100002', CLIENTNAME: 'Bob', BANK_ACCOUNT: '000000002222', 'AS ON DATE': '14/07/2026' },
      ],
      []
    );
    expect(asOnDate).toBe('14/07/2026');
  });

  it('returns an empty as-on date when the column is absent', () => {
    const { rows, asOnDate } = buildDynamicMaster(
      [{ CLIENT_CODE: '100001', CLIENTNAME: 'Alice', BANK_ACCOUNT: '000000001111' }],
      []
    );
    expect(rows).toHaveLength(1);
    expect(asOnDate).toBe('');
  });
});

describe('buildDailyTracker() — Current Account Data override', () => {
  const master = [
    { 'WS Client Code': '100001', 'Client Name': 'Alice Corp', 'RM Name': 'Smith, John', 'ICICI Acc. No.': '1111' },
  ];
  const z10 = [
    { 'WS CLIENT ID': '100001', 'SECURITY TYPE DESCRIPTION': 'Mutual Funds', 'ASTCLSNAME': 'Cash', 'MKTVALUE': '1000' },
  ];
  const ca = [
    { 'Account Number': '000000001111', 'Account Name': 'ALICE POA A/C', 'Available Balance': '25000', 'Effective Balance': '25000' },
    { 'Account Number': '000000009999', 'Account Name': 'ORPHAN POA A/C', 'Available Balance': '500', 'Effective Balance': '500' },
  ];
  const cw = [{ 'Client Name': 'ALICE CORP', 'Ledger Bal BOD': '0' }];
  const reconMeta = { fileName: 'Bank Recon_140726.xlsx', sheetName: 'WS Balances', asOnDate: '14/07/2026' };
  const withCa = (exportedAt) => ({
    ...reconMeta,
    currentAccount: { fileName: 'CurrentAccountdata (16).xlsx', sheetName: 'Sheet0', exportedAt },
  });
  const orphanOf = (r) => r.exceptions.find((e) => e['Issue'].startsWith('ICICI Balance Without Master Record'));
  const hasStale = (r) => r.warnings.some((w) => w.includes('BEFORE the WS Balances as-on date'));

  it('reports the recon as the balance source when no Current Account file is given', () => {
    const r = buildDailyTracker(master, z10, ca, cw, reconMeta, null, null);
    expect(r.stats.balanceSource).toBe('bank-recon');
    expect(r.warnings.some((w) => w.includes('ICICI balances taken from'))).toBe(false);
    expect(hasStale(r)).toBe(false);
    expect(orphanOf(r)['Source File']).toBe('Bank Recon_140726.xlsx');
    expect(orphanOf(r)['Sheet Name']).toBe('WS Balances');
  });

  it('names the Current Account file as the balance source and attributes ICICI orphans to it', () => {
    const r = buildDailyTracker(master, z10, ca, cw, withCa('15-07-2026 09:12:00'), null, null);
    expect(r.stats.balanceSource).toBe('current-account');
    const note = r.warnings.find((w) => w.startsWith('ICICI balances taken from'));
    expect(note).toBe(
      'ICICI balances taken from "CurrentAccountdata (16).xlsx" (exported 15-07-2026 09:12:00) ' +
        "instead of the Bank Recon's ICICI Balances sheet."
    );
    expect(hasStale(r)).toBe(false);
    expect(orphanOf(r)['Source File']).toBe('CurrentAccountdata (16).xlsx');
    expect(orphanOf(r)['Sheet Name']).toBe('Sheet0');
    // Only provenance changes — the numbers are what the recon path would give.
    expect(r.summary.totalIcici).toBe(25500);
    expect(r.rows[0]['ICICI Bank Balance']).toBe(25000);
  });

  it('warns when the Current Account export predates the WS Balances as-on date', () => {
    const r = buildDailyTracker(master, z10, ca, cw, withCa('08-05-2026 13:38:59'), null, null);
    const stale = r.warnings.find((w) => w.includes('BEFORE the WS Balances as-on date'));
    expect(stale).toContain('08/05/2026');
    expect(stale).toContain('14/07/2026');
  });

  it('does not warn on a same-day or newer export, or when either date is unreadable', () => {
    expect(hasStale(buildDailyTracker(master, z10, ca, cw, withCa('14-07-2026 18:00:00'), null, null))).toBe(false);
    expect(hasStale(buildDailyTracker(master, z10, ca, cw, withCa('15/07/2026'), null, null))).toBe(false);
    expect(hasStale(buildDailyTracker(master, z10, ca, cw, withCa(''), null, null))).toBe(false);
    const noAsOn = { ...withCa('08-05-2026 13:38:59'), asOnDate: '' };
    expect(hasStale(buildDailyTracker(master, z10, ca, cw, noAsOn, null, null))).toBe(false);
  });

  it('judges staleness against the later of the WS as-on date and the recon balance date, naming it', () => {
    // Monday recon: WS snapshot Sat 25/07, bank balances Mon 27/07 (SheetJS m/d/yy).
    const mondayRecon = { ...withCa('26-07-2026 08:00:00'), asOnDate: '25/07/2026', reconBalanceDate: '7/27/26' };
    const stale = buildDailyTracker(master, z10, ca, cw, mondayRecon, null, null).warnings.find((w) => w.includes('BEFORE'));
    expect(stale).toContain("BEFORE the Bank Recon's balance date (27/07/2026)");
    // Same export is fine once it is on or after the recon's balance date.
    const fresh = { ...mondayRecon, currentAccount: { ...mondayRecon.currentAccount, exportedAt: '27-07-2026 09:30:00' } };
    expect(buildDailyTracker(master, z10, ca, cw, fresh, null, null).warnings.some((w) => w.includes('BEFORE'))).toBe(false);
    // A recon balance date EARLIER than the WS as-on date never lowers the bar.
    const earlierSummary = { ...withCa('13-07-2026 08:00:00'), asOnDate: '14/07/2026', reconBalanceDate: '7/10/26' };
    expect(buildDailyTracker(master, z10, ca, cw, earlierSummary, null, null).warnings.find((w) => w.includes('BEFORE'))).toContain(
      'BEFORE the WS Balances as-on date (14/07/2026)'
    );
  });

  it('says so when the export date is missing or unreadable instead of staying silent', () => {
    const noStamp = buildDailyTracker(master, z10, ca, cw, withCa(''), null, null).warnings.find((w) => w.startsWith('ICICI balances taken from'));
    expect(noStamp).toContain('no export date found in the file, so its age was not checked');
    const oddStamp = buildDailyTracker(master, z10, ca, cw, withCa('yesterday'), null, null).warnings.find((w) => w.startsWith('ICICI balances taken from'));
    expect(oddStamp).toContain('(exported yesterday — date not recognised, so its age was not checked)');
    expect(hasStale(buildDailyTracker(master, z10, ca, cw, withCa('yesterday'), null, null))).toBe(false);
  });

  it('keeps master-row and Axis exceptions attributed to the recon when a Current Account file is present', () => {
    const wider = [
      ...master,
      { 'WS Client Code': '100002', 'Client Name': 'Bob LLC', 'RM Name': 'Doe, Jane', 'ICICI Acc. No.': '3333' },
      { 'WS Client Code': '100003', 'Client Name': 'Carol Ltd', 'RM Name': '', 'ICICI Acc. No.': 'N/A' },
    ];
    const cwWider = [...cw, { 'Client Name': 'DAVE ORPHAN', 'Ledger Bal BOD': '700' }];
    const r = buildDailyTracker(wider, z10, ca, cwWider, withCa('15-07-2026 09:12:00'), null, null);
    const byIssue = (issue) => r.exceptions.find((e) => e['Issue'].startsWith(issue));
    const notFound = byIssue('Current Account Not Found In ICICI Balances');
    expect(notFound['Client Code']).toBe('100002');
    expect(notFound['Master Row']).toBe(3);
    for (const exc of [notFound, byIssue('Missing RM Name'), byIssue('Axis Demat Balance Without Master Record')]) {
      expect(exc['Source File']).toBe('Bank Recon_140726.xlsx');
      expect(exc['Sheet Name']).toBe('WS Balances');
    }
    expect(orphanOf(r)['Source File']).toBe('CurrentAccountdata (16).xlsx');
  });

  it('builds the same tracker from raw parseFile-style rows (title rows, __EMPTY keys) as from classifier rows', () => {
    // What XLSX.utils.sheet_to_json() yields for the portal export when the first row is taken as the header.
    const raw = [
      { 'OPERATIVE ACCOUNTS': ' OPERATIVE ACCOUNTS(2)' },
      { 'OPERATIVE ACCOUNTS': 'Account Number', __EMPTY: 'Account Name', __EMPTY_1: 'Currency', __EMPTY_2: 'Available Balance', __EMPTY_3: 'Effective Balance' },
      { 'OPERATIVE ACCOUNTS': '000000001111', __EMPTY: 'ALICE POA A/C', __EMPTY_1: 'INR', __EMPTY_2: '99999', __EMPTY_3: '25000' },
      { 'OPERATIVE ACCOUNTS': '000000009999', __EMPTY: 'ORPHAN POA A/C', __EMPTY_1: 'INR', __EMPTY_2: '99999', __EMPTY_3: '500' },
      { 'OPERATIVE ACCOUNTS': 'Date and Time: ', __EMPTY: '08-05-2026     13:38:59' },
    ];
    const viaRaw = buildDailyTracker(master, z10, raw, cw, reconMeta, null, null);
    const viaClassifier = buildDailyTracker(master, z10, ca, cw, reconMeta, null, null);
    expect(viaRaw.summary.totalIcici).toBe(viaClassifier.summary.totalIcici); // Effective, not Available
    expect(viaRaw.rows).toEqual(viaClassifier.rows);
    expect(orphanOf(viaRaw)['Client Name']).toBe('ORPHAN POA A/C');
  });

  it('attributes a stale saved-mapping fallback orphan to the Current Account file too', () => {
    const merges = { axisNames: {}, accounts: { '000000009999': '100999' } }; // target code no longer exists
    const r = buildDailyTracker(master, z10, ca, cw, withCa('15-07-2026 09:12:00'), null, merges);
    expect(orphanOf(r)['Source File']).toBe('CurrentAccountdata (16).xlsx');
    expect(r.summary.totalIcici).toBe(25500);
  });
});
