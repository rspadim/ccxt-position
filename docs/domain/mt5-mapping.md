# MT5 Mapping

Key MT5-like mapping:

- `position_id`: OMS position identity
- `strategy_id`: strategy identifier
- `reason`: source classification (`api`, `external`, ...)
- `close_by`: internal virtual netting operation
- `close_by` execution generates internal deals and adjusts positions without exchange order

Defaults:

- `strategy_id = 0` when omitted
- `position_id = 0` means automatic position resolution

