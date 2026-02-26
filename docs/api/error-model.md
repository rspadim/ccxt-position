# Error Model

Standard error fields:

- `code`
- `message`
- `details`
- `request_id`

Recommended classes:

- `validation_error`
- `permission_denied`
- `risk_blocked`
- `exchange_error`
- `conflict`
- `internal_error`

Dispatcher/engine specific classes:

- `unsupported_engine`
- `engine_unavailable`
- `account_engine_mismatch`
