import { describe, it, expect } from 'vitest';
import {
  normalizeAccount,
  buildDailyTracker,
  buildUpdatedMaster,
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
});

describe('buildDailyTracker()', () => {
  const masterRows = [
    { 'WS Client Code': 'C001', 'Client Name': 'Alice Corp', 'RM Name': 'Smith, John', 'ICICI Acc. No.': '1111' },
    { 'WS Client Code': 'C002', 'Client Name': 'Bob LLC',    'RM Name': 'Doe, Jane',   'ICICI Acc. No.': '2222.0' },
    { 'WS Client Code': 'C003', 'Client Name': 'Charlie Inc','RM Name': '',            'ICICI Acc. No.': '' },
  ];

  const z10Rows = [
    { 'WS client id': 'C001', 'Security Type Description': 'Mutual Funds', 'ASTCLSNAME': 'Cash',  'MKTVALUE': '150000' },
    { 'WS client id': 'C001', 'Security Type Description': 'Equity',       'ASTCLSNAME': 'Stock', 'MKTVALUE': '900000' },
    { 'WS client id': 'C002', 'Security Type Description': 'Mutual Funds', 'ASTCLSNAME': 'Cash',  'MKTVALUE': '50,000' },
  ];

  const caRows = [
    { 'Account Number': '000000001111', 'Available Balance': '25000' },
    { 'Account Number': '000000002222', 'Available Balance': 10000 },
  ];

  it('throws on a Client Master missing required columns', () => {
    const bad = [{ Foo: 1 }];
    expect(() => buildDailyTracker(bad, z10Rows, caRows))
      .toThrow(/Client Master is missing required column\(s\):/);
  });

  it('merges balances using the Master as the base', () => {
    const { rows } = buildDailyTracker(masterRows, z10Rows, caRows);

    const alice = rows.find((r) => r['WS Client Code'] === 'C001');
    expect(alice['RM Name']).toBe('Smith, John');
    expect(alice['ICICI Acc. No.']).toBe('000000001111');
    expect(alice['Liquid MF Balance']).toBe(150000);
    expect(alice['ICICI Bank Balance']).toBe(25000);
    expect(alice['Total Liquidity']).toBe(175000);

    const bob = rows.find((r) => r['WS Client Code'] === 'C002');
    expect(bob['Liquid MF Balance']).toBe(50000);
    expect(bob['ICICI Bank Balance']).toBe(10000);
    expect(bob['Total Liquidity']).toBe(60000);
  });

  it('sorts by RM asc, Total Liquidity desc, blanks last', () => {
    const { rows } = buildDailyTracker(masterRows, z10Rows, caRows);
    expect(rows).toHaveLength(3);
    expect(rows[0]['RM Name']).toBe('Doe, Jane');
    expect(rows[1]['RM Name']).toBe('Smith, John');
    expect(rows[2]['WS Client Code']).toBe('C003');
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

describe('buildUpdatedMaster()', () => {
  const masterRows = [
    { 'WS Client Code': 'C001', 'Client Name': 'Alice Corp', 'RM Name': 'Smith, John', 'ICICI Acc. No.': '1111' },
    { 'WS Client Code': 'C002', 'Client Name': 'Bob LLC',    'RM Name': 'Doe, Jane',   'ICICI Acc. No.': '2222' },
  ];

  it('appends new client codes from the Recon with a blank RM', () => {
    const recon = [
      { 'Bank Code': 'C003', 'Client Name': 'Charlie Inc', 'Bank Account': '3333' },
    ];
    const { rows, summary } = buildUpdatedMaster(masterRows, recon);

    const charlie = rows.find((r) => r['WS Client Code'] === 'C003');
    expect(charlie).toBeDefined();
    expect(charlie['RM Name']).toBe('');
    expect(charlie['ICICI Acc. No.']).toBe('000000003333');
    expect(charlie.Status).toBe('New');
    expect(summary.added).toBe(1);
    expect(summary.updated).toBe(0);
  });

  it('updates the ICICI Acc. No. when the Recon bank account differs', () => {
    const recon = [
      { 'Bank Code': 'C001', 'Client Name': 'Alice Corp', 'Bank Account': '9999' },
    ];
    const { rows, summary } = buildUpdatedMaster(masterRows, recon);

    const alice = rows.find((r) => r['WS Client Code'] === 'C001');
    expect(alice['ICICI Acc. No.']).toBe('000000009999');
    expect(alice.Status).toBe('Updated');
    expect(summary.updated).toBe(1);
    expect(summary.added).toBe(0);
  });

  it('leaves records unchanged when the Recon bank account matches', () => {
    const recon = [
      { 'Bank Code': 'C001', 'Client Name': 'Alice Corp', 'Bank Account': '000000001111' },
    ];
    const { rows, summary } = buildUpdatedMaster(masterRows, recon);
    const alice = rows.find((r) => r['WS Client Code'] === 'C001');
    expect(alice.Status).toBe('Unchanged');
    expect(summary.updated).toBe(0);
    expect(summary.unchanged).toBe(2);
  });

  it('throws on a Bank Recon missing required columns', () => {
    expect(() => buildUpdatedMaster(masterRows, [{ Foo: 1 }]))
      .toThrow(/Bank Recon is missing required column\(s\):/);
  });
});
