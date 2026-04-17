# Onza Liquidity Tracker

Desktop (Tauri) app for the ops team. Reads three source files, produces the consolidated daily liquidity tracker, and exports to Excel. All processing is local — files never leave the device.

## Setup

Drop `src/` into your existing Vite + Tauri project and install the two data deps:

```bash
npm install papaparse xlsx
```

That's the full dependency list on top of what Vite + React + Tauri already give you.

## File layout

```
src/
├── App.jsx                  # Three-file upload UI, filters, export button
├── App.css                  # Onza brand tokens + component styles
├── liquidityProcessor.js    # Pure JS pipeline — mirrors the Python prototype
├── main.jsx
└── index.css
```

## Data pipeline

`liquidityProcessor.js` is a faithful translation of the Python reference. Each stage of the Python script has a named helper in the JS so they can be unit-tested independently:

| Python step                                  | JS function             |
|----------------------------------------------|-------------------------|
| `str.replace(r'\.0$', '').zfill(12)`         | `normalizeAccount()`    |
| RM mapping from Z10                          | `buildRmMapping()`      |
| Liquid MFs (Mutual Funds ∩ Cash) groupby sum | `buildLiquidMfBalances()`|
| ICICI account → Available Balance            | `buildIciciMapping()`   |
| Merge, fill, total, sort                     | `buildLiquidityTracker()`|

Sort order matches the Python: `RM Name` ascending (blank RMs last), then `Total Liquidity` descending.

## Schema guards

If any of the three files is missing a required column, the UI surfaces a specific error naming the file and the missing column — no silent zero-filled output. Warnings (not errors) are shown for clients with no RM mapping or no matching ICICI account, since these are legitimate edge cases ops may want to investigate.

## Parsing notes

- `BANK_ACCOUNT`, `CLIENT_CODE`, `WS client id`, and `Account Number` are all parsed as strings (both CSV via PapaParse `dynamicTyping`, and Excel via `raw: false`) to preserve leading zeros and avoid `.0` suffixes.
- `Available Balance` and `MKTVALUE` are cleaned of commas / currency symbols before coercion.
- Excel files read the first sheet only.

## Tauri packaging

Nothing in this code depends on Tauri APIs — file parsing is pure browser (FileReader + Papa + XLSX). That means `npm run dev` works standalone too, which is useful for iterating on the UI without rebuilding the Tauri shell each time.

If you later want to write the exported Excel to a user-chosen path via the Tauri dialog plugin instead of a browser download, swap the `handleExport` function's Blob-download logic for `@tauri-apps/plugin-dialog` + `@tauri-apps/plugin-fs`.
