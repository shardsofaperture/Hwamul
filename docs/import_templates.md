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
- `units_per_pack` means **how many order units are inside one physical pack**.
  - Example: if SKU UOM is `KG` and one pallet is 500 kg, set `units_per_pack=500` and `kg_per_unit=1`.
  - Example: if SKU UOM is `EA` and one carton contains 120 pieces, set `units_per_pack=120` and `kg_per_unit=<weight per piece>`.
- In the **standard-pack master profile** workflow, the app uses `units_per_pack=1` and stores the full pack weight in `pack_kg` (mapped to `kg_per_unit`).
  - This is useful when you want one row = one standard pack with all dimensions/weight and do not want to manage per-unit math.
- For `pack_rules` dimensions (`dim_l_cm`, `dim_w_cm`, `dim_h_cm`), use **centimeters** for pallet/cargo uploads with individual handling units.
  - Example: length `120`, width `80`, height `90`.
- Legacy meter values are still accepted.
  - Example: length `1.20`, width `0.80`, height `0.90`.
- Avoid mixing unit styles within the same import file when possible.

## Pack master data import (`pack_mdm_template.csv`)
Use this when you want a canonical standard-pack contract mapped by SKU + vendor.

### Required columns
| Column | Required | Allowed values / notes |
| --- | --- | --- |
| `part_number` | Yes | SKU part number from `sku_master`. |
| `supplier_code` | Yes | Supplier code to tie the row to a supplier-specific SKU. |
| `pack_kg` | Yes | Numeric `>= 0.001`; gross weight per pack (kg). |
| `length_mm` | Yes | Numeric `>= 1`; pack length in millimeters. |
| `width_mm` | Yes | Numeric `>= 1`; pack width in millimeters. |
| `height_mm` | Yes | Numeric `>= 1`; pack height in millimeters. |
| `is_stackable` | Yes | Boolean `1` or `0`. |
| `ship_from_city` | Yes | Origin city (text). |
| `ship_from_port_code` | Yes | Uppercase alphanumeric code, 3-10 chars (e.g., `CNSHA`). |
| `ship_from_duns` | Yes | DUNS format, digits (optional dashes), 9-13 chars. |
| `ship_from_location_code` | Yes | Uppercase location code (`A-Z`, `0-9`, `.`, `_`, `-`). |
| `ship_to_locations` | Yes | Pipe-delimited location codes (e.g., `USLAX_DC01|USLGB_DC02`). |
| `allowed_modes` | Yes | Pipe-delimited mode codes (e.g., `OCEAN|TRUCK|AIR`). |
| `incoterm` | Yes | One of: `EXW`, `FCA`, `CPT`, `CIP`, `DAP`, `DPU`, `DDP`, `FAS`, `FOB`, `CFR`, `CIF`. |
| `incoterm_named_place` | Yes | Named place associated with the incoterm. |

### Optional columns
| Column | Required | Allowed values / notes |
| --- | --- | --- |
| `pack_name` | No | Display-only label; defaults to `STD_<part_number>`. |
| `pack_type` | No | Optional packaging type (text). |
| `qty_per_pack` | No | Numeric `>= 0.001` for external reference. |
| `pack_material` | No | Optional material descriptor. |
| `gross_weight_kg` | No | Numeric `>= 0.001` optional reference field. |
| `net_weight_kg` | No | Numeric `>= 0.001` optional reference field. |
| `max_stack` | No | Integer `>= 1`, used when `is_stackable=1`. |
| `stacking_notes` | No | Optional handling guidance. |
| `notes` | No | Optional freeform notes. |

Behavior on import:
- Rows are mapped to `sku_id` using `part_number + supplier_code`.
- Import writes one canonical standard-pack profile into `packaging_rules` (`pack_type=STANDARD`).
- Import sets fixed planning fields: `units_per_pack=1`, `pack_tare_kg=0`, `min_order_packs=1`, `increment_packs=1`.
- Dimensions are converted from mm to m before persistence.
- `pack_name` remains optional and display-only; it is not required for planning math.
- `is_default` is removed from the import contract for single-pack-per-part semantics.

## Supplier/SKU/Pack structure for uploads
- Model and upload data as **supplier-specific SKUs** (part number + supplier).
- Create suppliers first, then SKUs, then pack rules tied to those SKUs.
- For this canonical import, treat each supplier-specific SKU as single-pack-per-part (no default variant flag in file).
- Recommended operating model: treat the default pack rule as the SKU's **standard pack profile** (SKU + vendor + L/W/H cm + standard pack kg + stackability + conveyance allowance in UI).
- Planning rounds required kilograms up to whole standard packs and enforces at least 1 pack per order.
