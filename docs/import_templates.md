# Import Templates

Use template download buttons in **Docs → Import Templates**.

Templates are generated from in-app field specifications, so column names/examples match validation logic.

## Suggested import flow
1. Download the template for the table you want to load.
2. Fill rows in a spreadsheet without changing headers.
3. Save as CSV.
4. Import from the matching admin page.

## Good practices
- Start with 2–3 test rows before doing large imports.
- Keep date fields in `YYYY-MM-DD` format.
- Use codes/IDs that already exist in upstream master data (e.g., supplier and SKU references).
- If validation fails, use the row/field message in UI to fix and retry.

## Pack-rule unit guidance (important)
- For `pack_rules` dimensions (`dim_l_m`, `dim_w_m`, `dim_h_m`), **centimeters are preferred** for pallet/crate uploads.
  - Example: length `120`, width `80`, height `90`.
- Legacy meter values are still accepted.
  - Example: length `1.20`, width `0.80`, height `0.90`.
- Avoid mixing unit styles within the same import file when possible.

## Supplier/SKU/Pack structure for uploads
- Model and upload data as **supplier-specific SKUs** (part number + supplier).
- Create suppliers first, then SKUs, then pack rules tied to those SKUs.
- Keep one default pack rule per supplier-specific SKU.
