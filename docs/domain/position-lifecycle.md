# Position Lifecycle

- Open on first matching filled exposure
- Increase/decrease by incoming deals
- Close when quantity reaches zero
- On reversal, close old position and open a new `position_id`

`position_mode` is account-level:

- `hedge`
- `netting`

Netting behavior:

- opposite-side trades reduce/close current symbol position
- if opposite trade size exceeds open qty, system closes old position and opens a new reversed `position_id`
