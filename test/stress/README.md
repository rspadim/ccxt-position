# API Stress Test

Script: `test/stress/api_stress.py`

## What it measures

- Throughput (req/s)
- Error rate
- Latency p50 / p95 / p99
- Breakdown by endpoint
- Top errors

Outputs:

- `test/stress/runtime/stress-report-*.json`
- `test/stress/runtime/stress-report-*.md`

## Quick start

```powershell
py -3.13 test/stress/api_stress.py --duration-seconds 30 --concurrency 12
```

If `--api-key` is omitted, it tries `test/testnet/.env.testnet` (`INTERNAL_API_KEY`).

## Useful profiles

1. Quick smoke

```powershell
py -3.13 test/stress/api_stress.py --duration-seconds 20 --concurrency 8
```

2. Baseline

```powershell
py -3.13 test/stress/api_stress.py --duration-seconds 60 --concurrency 16
```

3. Heavy

```powershell
py -3.13 test/stress/api_stress.py --duration-seconds 120 --concurrency 32
```

## Reduce noise

If your environment has many accounts, query endpoints become heavy. You can scope to a subset:

```powershell
py -3.13 test/stress/api_stress.py --account-ids 86,87,88 --duration-seconds 60 --concurrency 16
```
