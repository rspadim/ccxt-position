# Runtime Topology (Single Host)

Processes:

1. `api-http`: REST endpoints (multiprocess)
2. `api-ws`: websocket endpoint and subscriptions
3. `worker-position`: async command execution, projection, reconciliation

Concurrency model:

- Worker pool with account lock by `account_id`
- Optional `pool_id` account affinity for dedicated throughput classes
