import { describe, it, expect } from 'vitest';
import { normalizeAccount, buildLiquidityTracker } from './liquidityProcessor';

describe('Liquidity Processor', () => {
  
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

  describe('buildLiquidityTracker()', () => {
    // Standard mock data representing the 3 input files
    const mockWsRows = [
      { CLIENT_CODE: 'C001', CLIENTNAME: 'Alice Corp', BANK_ACCOUNT: '1111' },
      { CLIENT_CODE: 'C002', CLIENTNAME: 'Bob LLC', BANK_ACCOUNT: '2222.0' },
      { CLIENT_CODE: 'C003', CLIENTNAME: 'Charlie Inc', BANK_ACCOUNT: '3333' } // No RM, No ICICI
    ];

    const mockZ10Rows = [
      // Valid Liquid MF for Alice
      { 'WS client id': 'C001', 'RMNAME': 'Smith, John', 'Security Type Description': 'Mutual Funds', 'ASTCLSNAME': 'Cash', 'MKTVALUE': '150000' },
      // Irrelevant asset class for Alice (should be ignored)
      { 'WS client id': 'C001', 'RMNAME': 'Smith, John', 'Security Type Description': 'Equity', 'ASTCLSNAME': 'Stock', 'MKTVALUE': '900000' },
      // Valid Liquid MF for Bob (formatted as string with commas to test parsing)
      { 'WS client id': 'C002', 'RMNAME': 'Doe, Jane', 'Security Type Description': 'Mutual Funds', 'ASTCLSNAME': 'Cash', 'MKTVALUE': '50,000' }
    ];

    const mockIciciRows = [
      { 'Account Number': '000000001111', 'Available Balance': '25000' }, // Alice's account
      { 'Account Number': '000000002222', 'Available Balance': 10000 }    // Bob's account (numeric balance)
    ];

    it('throws an error if a file is missing required columns', () => {
      const badWsRows = [{ 'WrongColumn': 'C001' }];
      expect(() => buildLiquidityTracker(badWsRows, mockZ10Rows, mockIciciRows))
        .toThrow(/WS Balance is missing required column\(s\): CLIENT_CODE, CLIENTNAME, BANK_ACCOUNT/);
    });

    it('accurately merges balances and formats output rows', () => {
      const { rows } = buildLiquidityTracker(mockWsRows, mockZ10Rows, mockIciciRows);
      
      // Alice: 150k MF + 25k ICICI = 175k
      const alice = rows.find(r => r.CLIENT_CODE === 'C001');
      expect(alice['RM Name']).toBe('Smith, John');
      expect(alice['Liquid_MF_Balance']).toBe(150000);
      expect(alice['ICICI Bank Balance']).toBe(25000);
      expect(alice['Total Liquidity']).toBe(175000);

      // Bob: 50k MF + 10k ICICI = 60k
      const bob = rows.find(r => r.CLIENT_CODE === 'C002');
      expect(bob['RM Name']).toBe('Doe, Jane');
      expect(bob['Liquid_MF_Balance']).toBe(50000); // Proves comma stripping works
      expect(bob['ICICI Bank Balance']).toBe(10000);
      expect(bob['Total Liquidity']).toBe(60000);
    });

    it('sorts by RM Name ascending, then Total Liquidity descending, with blanks last', () => {
      const { rows } = buildLiquidityTracker(mockWsRows, mockZ10Rows, mockIciciRows);
      
      expect(rows).toHaveLength(3);
      
      // 'Doe, Jane' (60k) comes before 'Smith, John' (175k) alphabetically
      expect(rows[0]['RM Name']).toBe('Doe, Jane');
      expect(rows[1]['RM Name']).toBe('Smith, John');
      
      // 'Charlie Inc' has no RM mapping, so it goes to the bottom despite having 0 liquidity
      expect(rows[2].CLIENT_CODE).toBe('C003');
      expect(rows[2]['RM Name']).toBe('');
    });

    it('generates the correct summary statistics', () => {
      const { summary } = buildLiquidityTracker(mockWsRows, mockZ10Rows, mockIciciRows);
      
      expect(summary.totalClients).toBe(3);
      expect(summary.totalRMs).toBe(2); // John, Jane (blanks don't count)
      expect(summary.totalLiquidMf).toBe(200000); // 150k + 50k
      expect(summary.totalIcici).toBe(35000);     // 25k + 10k
      expect(summary.totalLiquidity).toBe(235000);
    });

    it('generates warnings for missing RM and ICICI mappings', () => {
      const { warnings } = buildLiquidityTracker(mockWsRows, mockZ10Rows, mockIciciRows);
      
      // Charlie has no RM and no matching ICICI account
      expect(warnings.length).toBe(2);
      expect(warnings).toContain('1 client(s) had no RM mapping in Z10.');
      expect(warnings).toContain('1 client(s) had no matching ICICI account.');
    });
  });
});