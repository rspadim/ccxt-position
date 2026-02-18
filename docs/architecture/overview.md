# Architecture Overview

`ccxt-position` has two independent but connected domains:

- `ccxt` domain: exchange gateway and raw exchange synchronization
- `position` domain: MT5-like OMS view (orders/deals/positions/history)

The `position` domain is authoritative for OMS workflows and reconciliation decisions.
