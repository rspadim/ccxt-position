# Reconciliation: Orders + Deals + Post Trading (Spec)

## Goal

Make reconciliation deterministic and safe when exchange activity exists outside OMS command flow.

Target: if an order/trade exists in exchange but not in OMS, import it, link it, and project positions consistently.

## Current State (as of 2026-02-22)

- Dispatcher reconciliation loop imports `trades` (`fetch_my_trades`).
- It persists raw trades in `ccxt_trades_raw`.
- It projects each trade to OMS deals/positions.
- If no OMS order is linked, projection continues as external:
  - `order_id = null`
  - `strategy_id = 0`
  - position projection still happens.
- There is no periodic "import missing exchange orders" step in reconcile loop.

## Target State

Reconciliation must run in 2 phases per account/scope:

1. Import missing exchange orders.
2. Import missing exchange trades (deals) and project positions.

Both phases must persist raw CCXT payload first.

## Phase 1: Order Reconciliation

For each account:

- Pull exchange orders (`open` + configurable historical window).
- Upsert raw payload to `ccxt_orders_raw` (idempotent by fingerprint/hash).
- Try link to existing OMS order by priority:
  1. `exchange_order_id`
  2. `client_order_id`
  3. optional deterministic fallback (strictly controlled)
- If linked: refresh OMS order status/metadata from exchange snapshot.
- If not linked: create OMS order as external unmatched:
  - `reason = external`
  - `strategy_id = 0`
  - `position_id = 0`
  - `exchange_order_id` set
  - status from exchange mapping
  - mark as `reconciled = false` (see Data Model section)

## Phase 2: Trade Reconciliation

For each account:

- Pull exchange trades.
- Upsert raw payload to `ccxt_trades_raw` (idempotent).
- Resolve OMS order link using exchange/client ids.
- If order is resolved:
  - project deal with `order_id` and inherited strategy/position context.
- If order is unresolved:
  - project deal as external unmatched (`strategy_id = 0`, `order_id = null`, `reconciled = false`).
- Project net position after each accepted deal according to account mode (`hedge` or `netting`).

## Unreconciled Position Rule (Mandatory)

When exchange order is imported as unmatched (`reconciled = false`, `strategy_id = 0`), position projection must follow:

- create an OMS position dedicated to that order, also unmatched (`strategy_id = 0`);
- enforce `1 external unmatched order = 1 dedicated unmatched position`;
- do not merge unmatched position with other unmatched/existing positions during this phase.

Purpose: keep deterministic post-trade correction, so operator can later bind/close/merge explicitly (for example `close_by` or an explicit merge command).

## Post Trading Reassign (Order-Centric)

Post Trading reassign is order-only.

- Input: OMS order(s) unresolved or wrongly attributed.
- Action: assign `target_strategy_id` (+ optional `target_position_id`) to OMS order.
- Safety validations (mandatory):
  - permission by account
  - strategy belongs to account
  - optional target position exists
  - optional target position account/symbol/side compatible with order
  - do not overwrite already-assigned order without explicit override policy

### Deal/Position Impact After Reassign

Reassigning only OMS order metadata is insufficient when already projected deals were unresolved.

Required deterministic follow-up:

- Find impacted deals through raw trade linkage (join `oms_deals.exchange_trade_id` -> `ccxt_trades_raw.exchange_trade_id` -> `exchange_order_id`).
- Update affected deals `order_id/strategy_id/(position_id when applicable)`.
- Rebuild positions for impacted account+symbol from chronological deals, in one transaction boundary per account/symbol group.

If rebuild is not enabled yet, system must explicitly state "metadata only, no position replay".

## Netting Behavior

In `netting` mode, unresolved external deals can:

- increase position,
- reduce position,
- close position,
- reverse and open a new opposite position id.

This is expected and must remain deterministic during replay/rebuild.

Note: the "1 order = 1 dedicated unmatched position" rule above overrides normal netting aggregation while order is unresolved.

## Data Model Requirements (Proposed)

- Add `reconciled` flag to `oms_orders`:
  - `true`: linked/known OMS lifecycle
  - `false`: imported external unmatched
- Keep `reconciled` in `oms_deals`.
- Maintain raw tables as immutable event evidence (`ccxt_orders_raw`, `ccxt_trades_raw`).

## Safety and Audit Requirements

- Every reconciliation and reassign mutation must emit audit payload with before/after references.
- Idempotency keys:
  - raw tables: payload fingerprint/hash
  - projection: unique by `exchange_trade_id` per account for deals
- Reassign and replay must be transactional per account (or narrower unit) to prevent partial visible state.

## UI Implications

- Post Trading list is order-centric:
  - filter by account/date/status/reconciled
  - preview impacted rows before apply
  - apply result shows:
    - orders updated
    - deals relinked
    - positions rebuilt (count)
- Keep OMS Admin CRUD as manual escape hatch for exceptional corrections.

## Open Decisions

- Historical order fetch strategy per exchange (`fetch_orders` support variability).
- Override policy for already assigned orders (default should be deny).
- Rebuild scope granularity:
  - full account replay vs symbol-scoped replay from earliest affected trade.
