# Front-end Screenshots

This document describes how to generate/update the UI screenshots used in `README.md`.

## Output Folder

- `docs/media/screenshots/`

## Prerequisites

1. Front-end dependencies:

```powershell
npm --prefix apps/front-end install
```

2. Playwright for Python (one-time):

```powershell
py -3.13 -m pip install playwright
py -3.13 -m playwright install chromium
```

3. Running services:

- Front-end URL (default): `http://127.0.0.1:5173`
- API URL (default): `http://127.0.0.1:8000`
- A valid API key with access to pages/data you want to capture

## Capture Script

Script:

- `scripts/capture_front_screenshots.py`

Example:

```powershell
py -3.13 scripts/capture_front_screenshots.py `
  --front-url http://127.0.0.1:5173 `
  --api-url http://127.0.0.1:8000 `
  --api-key <YOUR_API_KEY>
```

## Notes

- Script forces `en-US` before capture.
- It captures login, OMS, system console, admin, and risk pages.
- If `Symbol List` appears empty, use an account with available markets and rerun.
