# Testnet Environment

This folder provides an isolated Python flow to bootstrap and smoke-test Binance Testnet quickly.

## Files

- `.env.testnet.example`: required variables template
- `run.py`: bootstrap runner (Python)
- `runtime/context.json`: generated runtime context (not versioned)

## Setup

1. Copy template:

```bash
cp test/testnet/.env.testnet.example test/testnet/.env.testnet
```

2. Edit `test/testnet/.env.testnet` and fill:

- `BINANCE_TESTNET_API_KEY`
- `BINANCE_TESTNET_SECRET_KEY`
- `TESTNET_MASTER_KEY` (Fernet key from `python -m apps.api.cli generate-master-key`)
- `INTERNAL_API_KEY` (internal API key used by this project)

3. Run bootstrap:

```bash
python test/testnet/run.py
```

By default, bootstrap resets stack and volumes (`docker compose down -v`) to avoid API-key/user collisions across reruns.
To keep existing data, set:

```bash
TESTNET_RESET_STACK=0 python test/testnet/run.py
```

What it does:

- prepares `apps/api/config.docker.json`
- starts docker stack
- creates user + internal API key + account
- stores encrypted Binance testnet credentials
- runs a smoke `send_order` via `/position/commands`
- saves runtime context in `test/testnet/runtime/context.json`

## Multi-Scenario Validation

Run hedge/netting live scenarios with multiple `magic_id`, position reduce/reverse, and mirrored-account reconciliation (same exchange credentials in two account IDs):

```bash
python test/testnet/scenarios.py
python test/testnet/scenarios.py --verbose
```

Output is saved to:

- `test/testnet/runtime/scenarios.json`
- `test/testnet/runtime/scenarios.log`
- `test/testnet/runtime/scenarios-diagnostics.json` (on failures)
