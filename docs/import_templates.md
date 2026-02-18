# Import Templates

Use template download buttons in **Docs → Import Templates** or from upload screens like **Admin → Demand entry**.

Canonical source: persisted CSV files under `templates/`, regenerated at app startup by `seed.ensure_templates()` from `field_specs.TABLE_SPECS`. Docs and upload flows download these same persisted files, so headers/examples remain aligned with validation rules.

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

## Demand import columns (`demand_template.csv`)
Demand imports use user-facing mapping fields and are translated to `sku_id` during append.

| Column | Required | Description |
| --- | --- | --- |
| `part_number` | Yes | SKU part number from `sku_master`. |
| `supplier_code` | No* | Supplier code used to disambiguate duplicate part numbers across suppliers. |
| `need_date` | Yes | Date in `YYYY-MM-DD` format. |
| `qty` | Yes | Numeric quantity, `>= 0`. |
| `coo_override` | No | Optional COO override (ISO-2). |
| `priority` | No | Optional planning priority label. |
| `notes` | No | Optional planner notes. |
| `phase` | No | Optional phase (`Trial1`, `Trial2`, `Sample`, `Speed-up`, `Validation`, `SOP`). |
| `mode_override` | No | Optional mode override. |
| `service_scope` | No | Optional service scope (`P2P`, `P2D`, `D2P`, `D2D`). |
| `miles` | No | Optional trucking miles override, `>= 0`. |

\* `supplier_code` becomes required when a `part_number` exists under multiple suppliers.

### Demand example: single-supplier part numbers
```csv
part_number,need_date,qty,phase,priority
MFG-88421,2026-03-10,250,Trial1,HIGH
INT-100045,2026-03-17,96,SOP,NORMAL
```

### Demand example: multi-supplier part numbers (include `supplier_code`)
```csv
part_number,supplier_code,need_date,qty,phase,mode_override
AXLE-2200,SUP_A,2026-04-02,800,Sample,OCEAN
AXLE-2200,SUP_B,2026-04-02,400,Sample,AIR
```

## Pack-rule unit guidance (important)
- For `pack_rules` dimensions (`dim_l_cm`, `dim_w_cm`, `dim_h_cm`), use **centimeters** for pallet/cargo uploads with individual handling units.
  - Example: length `120`, width `80`, height `90`.
- Legacy meter values are still accepted.
  - Example: length `1.20`, width `0.80`, height `0.90`.
- Avoid mixing unit styles within the same import file when possible.

## Supplier/SKU/Pack structure for uploads
- Model and upload data as **supplier-specific SKUs** (part number + supplier).
- Create suppliers first, then SKUs, then pack rules tied to those SKUs.
- Keep one default pack rule per supplier-specific SKU.
