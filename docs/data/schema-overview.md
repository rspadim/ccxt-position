# Schema Overview

Logical groups:

- Identity and access
- Account and credentials
- CCXT raw ingestion
- Position OMS projections
- Queue/outbox infrastructure
- Audit and reconciliation cursors

Dialect policy:

- `v0`: MySQL-only SQL scripts and repositories (`*_mysql`)
- `v1+`: add PostgreSQL raw SQL repositories behind `db_engine` switch
- No ORM is used; all persistence is raw SQL by engine-specific repository modules
