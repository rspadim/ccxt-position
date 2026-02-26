# Architecture Overview

`CCXT OMS` has two independent but connected domains:

- `ccxt` domain: exchange gateway and raw exchange synchronization
- `oms` domain: MT5-like OMS view (orders/deals/positions/history)

The `oms` domain is authoritative for OMS workflows and reconciliation decisions.
