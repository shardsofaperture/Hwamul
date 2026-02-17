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
