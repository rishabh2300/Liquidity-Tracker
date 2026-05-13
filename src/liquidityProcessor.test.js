import { describe, it, expect } from 'vitest';
import {
  normalizeAccount,
  buildDailyTracker,
  buildDynamicMaster,
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
});
