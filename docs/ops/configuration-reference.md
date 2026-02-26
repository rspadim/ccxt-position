# Configuration Reference

Dispatcher/API settings commonly used:

- `APP_ENV`
- `DB_ENGINE`
- `MYSQL_HOST`
- `MYSQL_PORT`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `MYSQL_DATABASE`
- `DISPATCHER_HOST`
- `DISPATCHER_PORT`
- `DISPATCHER_REQUEST_TIMEOUT_SECONDS`
- `DISPATCHER_POOL_SIZE_CCXT`
- `DISPATCHER_POOL_SIZE_CCXTPRO`
- `ENCRYPTION_MASTER_KEY`
- `REQUIRE_ENCRYPTED_CREDENTIALS`
- `LOG_DIR`

Notes:

- `DISPATCHER_POOL_SIZE_CCXT` and `DISPATCHER_POOL_SIZE_CCXTPRO` are mandatory dispatcher pool controls.
- Legacy `dispatcher_pool_size` was removed.
- `exchange_id` must be explicit (`ccxt.<exchange>` or `ccxtpro.<exchange>`); no implicit normalization is applied.
