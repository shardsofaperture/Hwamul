# Rates Guide

Use **Rate cards** for contract structures and **Rate charges** for accessorial cost components.

## Key field conventions
- Dates: `YYYY-MM-DD`.
- Service scope: `P2P`, `P2D`, `D2P`, `D2D`.
- Flatrack condition flags in **Rate Test**: `OH`, `OW`, `OHW`.
- Keep `origin_type/origin_code` and `dest_type/dest_code` aligned with your lane coding standard.

## Recommended setup flow
1. Create carriers in **Admin → Carriers**.
2. Add rate cards by lane/scope/equipment/effective period.
3. Add rate charges for each card (base + accessorial rows).
4. Validate expected combinations in **Admin → Rate Test**.
5. Re-test whenever date windows, lane mappings, or charges change.

## Change-control tips
- Avoid overlapping effective windows for the same lane/scope/equipment tuple.
- Add short notes for major pricing changes to support review handoffs.
- Align customs maintenance with rate changes when duties/tariff programs also shift.
