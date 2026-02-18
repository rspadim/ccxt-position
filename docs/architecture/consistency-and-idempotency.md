# Consistency and Idempotency

Principles:

- Persist first, then execute externally
- Use idempotency keys for raw trade/order ingestion
- Project raw exchange state into OMS state through deterministic rules
- Never mutate history destructively; write compensating/audit events
