# Data Flow

1. Client sends command to `POST /position/commands`
2. API validates ACL and risk controls
3. API persists command and local order intent
4. Worker sends order to CCXT/exchange
5. Exchange updates are stored in raw CCXT tables
6. Position projector updates orders/deals/positions tables
7. Events are written to `event_outbox`
8. WebSocket service delivers events to subscribers
