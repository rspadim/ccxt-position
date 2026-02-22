# Simple Front

`apps/simple-front` is the local trading UI for `ccxt-position`.

## Stack

- `Vite` (dev server + build)
- Vanilla JavaScript (no React/Vue/Svelte)
- `Tabulator` for data grids
- `Web Awesome` (formerly Shoelace) for tabs/dialog components

## Main Files

- `apps/simple-front/index.html`: UI layout and forms
- `apps/simple-front/src/main.js`: behavior, API calls, WebSocket handling
- `apps/simple-front/src/style.css`: visual styles
- `apps/simple-front/src/i18n.json`: labels/translations

## UI Structure

- Trade panels and command forms (send/change/cancel/close/close-by/ccxt)
- Two monitor groups with tabs:
  - Position Monitor: open positions, open orders, deals, histories
  - System Monitor: ccxt orders, ccxt trades, ws events
- `Close By` action opens a modal dialog with source + target position selection

## Data Flow

- REST calls use `x-api-key` header from the login form
- Real-time updates come from `WS /ws`
- Grids are updated by:
  - explicit refresh actions
  - websocket incremental events
  - initial snapshot events

## Run

```powershell
cd apps/simple-front
npm install
npm run dev
```

Default local URL is usually `http://127.0.0.1:5173`.

## Notes

- The UI depends on the API stack running at `apps/api`.
- For multi-account table refresh, the UI sends `account_ids` (CSV) to position query endpoints.
