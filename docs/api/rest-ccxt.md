# REST: CCXT Domain

## `POST /ccxt/{account_id}/{func}`

Generic function gateway to CCXT exchange methods.

## `POST /ccxt/multiple_commands`

Batch command execution for one or multiple accounts.

Notes:

- Domain is independent from position OMS tables.
- Raw exchange events are persisted before OMS projection.
