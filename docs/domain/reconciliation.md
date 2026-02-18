# Reconciliation

Reconciliation is part of the `position` domain.

Behavior:

- Poll exchange state periodically
- Import external operations with:
  - `magic_id = 0`
  - `reason = external`
  - `reconciled = false`
- Allow manual reassignment to target strategy/position with audit trail

Projection:

- External trades are persisted in `ccxt_trades_raw`
- New trades are projected to `position_deals`
- Positions are updated/created in `position_positions`
