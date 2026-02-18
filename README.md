# ccxt-position

`ccxt-position` is a single-host OMS gateway that combines:

- A CCXT-like API surface (`/ccxt`) for direct exchange routing
- An MT5-like position engine (`/position`) for orders, deals, positions, and reconciliation

## Project Status

Specification-first bootstrap. Code implementation is intentionally minimal in this phase.

## Core Concepts

- `account`: an exchange credential set and runtime config
- `order`: command intent and exchange lifecycle state
- `deal`: executed trade event
- `position`: MT5-like tracked exposure
- `magic_id`: strategy/robot identifier (`0` = automatic/default)

## API Surfaces

- `POST /position/commands`: unified MT5-like command entrypoint
- `POST /ccxt/{account_id}/{func}`: CCXT function gateway
- `POST /ccxt/multiple_commands`: batch CCXT commands
- `WS /ws`: unified websocket envelope (`position_*` and `ccxt_*`)

## Documentation Index

- Architecture: `docs/architecture/overview.md`
- Runtime topology: `docs/architecture/runtime-topology.md`
- Data flow: `docs/architecture/data-flow.md`
- Domain mapping: `docs/domain/mt5-mapping.md`
- Position API: `docs/api/rest-position.md`
- CCXT API: `docs/api/rest-ccxt.md`
- WebSocket contract: `docs/api/websocket.md`
- Schema catalog: `docs/data/table-catalog.md`
- Security model: `docs/security/authentication.md`
- Operations: `docs/ops/deployment-single-host.md`
- Roadmap: `docs/roadmap/mvp-scope.md`
