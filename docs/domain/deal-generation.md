# Deal Generation

Deals can originate from:

1. Explicit trade events from exchange
2. Derived fills from order status transitions

Both paths must resolve to a single idempotent `position_deals` insertion model.
