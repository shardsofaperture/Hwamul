# Rates Guide

Use **Rate cards** for base pricing and **Rate charges** for accessorials.

- Effective dates use `YYYY-MM-DD`.
- Service scopes: `P2P`, `P2D`, `D2P`, `D2D`.
- Flatrack condition flags are available in **Rate Test** (`OH`, `OW`, `OHW`).
- Keep `origin_type/origin_code` and `dest_type/dest_code` aligned with your lane coding.

Recommended workflow:
1. Create carriers.
2. Create rate cards by lane/scope/equipment/date.
3. Add charges per card.
4. Validate in **Rate Test**.
5. Maintain customs reporting data in **Customs / HTS** for HTS and tariff tracking alongside freight rates.
