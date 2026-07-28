# Onza Liquidity Tracker

Internal desktop/web tool for reconciling client liquidity data and producing a daily RM-wise liquidity tracker.

The app runs locally in the browser during development and is packaged as a Windows desktop app with Tauri.

## What It Does

- Reads the Z0 Client Master, Bank Recon, Z10 Holding / AssetClass, and Client Wise Axis Ledger files.
- Reconciles the master against Bank Recon client/account data.
- Builds a consolidated liquidity view by client and RM.
- Combines ICICI cash, Liquid MF, and Axis Demat balances.
- Flags missing mappings, missing RM names, duplicate accounts, and orphan balances.
- Lets users fix selected exceptions inside the app and rebuild.
- Groups duplicate client names in the on-screen tracker while keeping Excel exports row-level.
- Exports a daily liquidity ZIP with the master tracker and RM-wise workbooks.
- Exports updated Z0 master, mapping report, and exception report workbooks.

## Input Files

Drop all daily files into the single upload zone — file names, sheet names,
and sheet order don't matter. Every file is identified by its column
fingerprint (`classifyWorkbook`), and the tracker builds automatically once
the three required reports are present:

| Report | Required | Identified by |
| --- | --- | --- |
| Bank Recon | Yes | `WS Balances` table (`CLIENT_CODE`, `CLIENTNAME`, `BANK_ACCOUNT`) + ICICI account table (`Account Number`, balance column) |
| Z10 Holding / AssetClass | Yes | `WS CLIENT ID`, `SECURITY TYPE DESCRIPTION`, `ASTCLSNAME`, `MKTVALUE` (pivot sheets are skipped) |
| Client Wise (Axis) | Yes | `Client Name`, `Ledger Bal BOD` |
| Z2 Client Master | Optional | `CLIENTID`, `CLIENTCODE`, `CLIENTNAME`, `RMNAME`, `TRADING BANK ACCOUNT` |

When the Z2 Client Master is included, missing RM names and unmatched
accounts/holdings are auto-resolved from it and listed on an "Auto-Resolved"
sheet in the exception report.

Standing rules: test clients (RM "Test RM" / dummy account) are excluded from
the tracker and totals; Onza's own current account appears as a row under
Nikhil S Shah. WS client codes are validated as exactly six digits starting
with `1` — if a PAN appears in `CLIENTCODE`, the app falls back to the valid
WS code in `CLIENTID`.

## Outputs

- `Onza_Liquidity_Tracker_YYYY-MM-DD.zip`
  - Consolidated tracker workbook
  - RM-wise tracker workbooks
- `Z0_ClientMaster_YYYY-MM-DD.xlsx`
- `Mapping_Report_YYYY-MM-DD.xlsx`
- `Daily_Exceptions.xlsx`

All parsing and processing happens locally on the device.

## Local Development

Install dependencies:

```bash
npm install
```

Run the Vite development server:

```bash
npm run dev
```

Run tests:

```bash
npm test
```

Build the web frontend:

```bash
npm run build
```

## Tauri Desktop App

Run the desktop app in development:

```bash
npm run tauri:dev
```

Build the desktop app locally:

```bash
npm run tauri:build
```

Local Tauri builds require Rust and the platform-specific Tauri build dependencies.

## Releasing an Update

Installed desktop apps check for updates on every launch against the public
releases repo (`rishabh2300/liquidity-tracker-releases`) and offer to install
newer versions automatically. To ship a release:

1. Bump `version` in `src-tauri/tauri.conf.json` (e.g. `1.1.0` → `1.2.0`) and
   commit & push.
2. Tag and push the tag:

   ```bash
   git tag app-v1.2.0 && git push origin app-v1.2.0
   ```

3. GitHub Actions (`Build Windows Release`) builds the signed installers and
   uploads them with `latest.json` as a **draft** release on the public
   releases repo.
4. Publish the draft there — every installed PC gets the update prompt on its
   next launch.

Required repository secrets (Settings → Secrets and variables → Actions):

- `TAURI_SIGNING_PRIVATE_KEY` — contents of the updater signing key
  (`~/.tauri/liquidity-tracker-updater.key` on the machine that generated it,
  2026-07-28).
- `TAURI_SIGNING_PRIVATE_KEY_PASSWORD` — the key's password
  (`~/.tauri/liquidity-tracker-updater.password.txt` on the same machine).
- `RELEASES_TOKEN` — fine-grained PAT with read/write **Contents** permission
  on `liquidity-tracker-releases`, so CI in this (private) repo can publish
  releases to the public one.

Losing the signing key or its password means future updates can't be signed —
keep copies somewhere safe outside this machine.

## Tech Stack

- React
- Vite
- Tauri 2
- xlsx
- Papa Parse
- JSZip
- Vitest
