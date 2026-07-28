import { describe, it, expect } from 'vitest';
import {
  normalizeAccount,
  buildDailyTracker,
  buildDynamicMaster,
  findTable,
  classifyWorkbook,
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
