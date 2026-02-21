# Simple Front

Small Vite UI for local operations against `ccxt-position` API.

## Features

- API key + base URL + account/strategy selector (with browser history)
- WebSocket connect/subscribe (`position` and `ccxt`)
- 8 tables:
  - open positions
  - open orders
  - history positions
  - history orders
  - deals
  - ccxt trades
  - ccxt orders
  - ws events
- Forms for:
  - send order
  - cancel order
  - change order
  - direct ccxt call

## Run

```powershell
cd apps/simple-front
npm install
npm run dev
```

Open the URL shown by Vite (usually `http://127.0.0.1:5173`).
