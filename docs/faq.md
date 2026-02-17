# FAQ / Troubleshooting

## Why does Save fail?
Validation errors return the exact row and field. Correct those values and retry save.

## Why are my dates rejected?
Use `YYYY-MM-DD` format (example: `2026-01-31`).

## Why can't I find a SKU in Pack rules?
SKU records are supplier-specific. Confirm supplier selection and search filters.

## Why is no rate card selected in Rate Test?
Check:
- `is_active` is enabled.
- Effective date range covers test date.
- Scope/lane/equipment fields match exactly.
- Optional condition flags are set correctly.

## CSV import keeps failing. What should I do?
- Re-download the latest template from **Docs → Import Templates**.
- Verify headers are unchanged.
- Confirm referenced master data exists (supplier, SKU, carrier, etc.).

## Where is data stored?
Data is stored locally in `planner.db`.

## How do I move data between environments?
Use **Admin → Data management** to export/import data bundles.
