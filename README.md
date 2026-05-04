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

The app expects four source files:

| Slot | Expected File | Important Columns / Sheets |
| --- | --- | --- |
| Z0 Client Master | Z0 client master export | `CLIENTCODE`, `CLIENTID`, `CLIENTNAME`, `RMNAME`, `TRADING BANK ACCOUNT`, `MANDATEBANKACCOUNTNO` |
| Bank Recon | Multi-sheet bank recon workbook | `WS Balances`, `ICICI Balances` |
| Z10 Holding / AssetClass | Z10 holdings export | `WS client id`, `ASTCLSNAME`, `MKTVALUE` |
| Client Wise | Axis ledger export | `Client Name`, `Ledger Bal BOD` |

WS client codes are validated as exactly six digits starting with `1`. If a PAN appears in `CLIENTCODE`, the app falls back to the valid WS code in `CLIENTID`.

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

## Windows Release Workflow

GitHub Actions workflow: `Build Windows Release`

The workflow runs on:

- Pushes to `main`
- Manual `workflow_dispatch`

It builds the Windows release with `tauri-apps/tauri-action`.

Required release secrets:

- `TAURI_PRIVATE_KEY`
- `TAURI_KEY_PASSWORD`

The workflow creates a draft GitHub release named:

```text
Liquidity Tracker v__VERSION__
```

## Tech Stack

- React
- Vite
- Tauri 2
- xlsx
- Papa Parse
- JSZip
- Vitest
