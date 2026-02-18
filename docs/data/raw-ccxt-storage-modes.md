# Raw CCXT Storage Modes

Each account can choose one mode:

- `shared`: write to global `ccxt_*_raw` tables
- `dedicated`: write to per-account raw tables

Reason:

- Some exchanges do not provide globally unique IDs
- Dedicated mode isolates problematic account/exchange combinations
