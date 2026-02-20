# Testnet Environment

This folder provides an isolated Python flow to bootstrap and smoke-test Binance Testnet quickly.

## Files

- `.env.testnet.example`: required variables template
- `run.py`: bootstrap runner (Python)
- `runtime/context.json`: generated runtime context (not versioned)

## Setup

1. Copy template:

```bash
cp environments/testnet/.env.testnet.example environments/testnet/.env.testnet
```

2. Edit `environments/testnet/.env.testnet` and fill:

- `BINANCE_TESTNET_API_KEY`
- `BINANCE_TESTNET_SECRET_KEY`
- `TESTNET_MASTER_KEY` (Fernet key from `python -m apps.api.cli generate-master-key`)
- `INTERNAL_API_KEY` (internal API key used by this project)

3. Run bootstrap:

```bash
python environments/testnet/run.py
```

What it does:

- prepares `apps/api/config.docker.json`
- starts docker stack
- creates user + internal API key + account
- stores encrypted Binance testnet credentials
- runs a smoke `send_order` via `/position/commands`
- saves runtime context in `environments/testnet/runtime/context.json`

## Multi-Scenario Validation

Run hedge/netting live scenarios with multiple `magic_id` and position reduce/reverse:

```bash
python environments/testnet/scenarios.py
```

Output is saved to:

- `environments/testnet/runtime/scenarios.json`
