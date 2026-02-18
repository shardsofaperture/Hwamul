# Import Templates

Use template download buttons in **Docs → Import Templates** or **Admin → Templates & Upload Hub**.

Canonical source: persisted CSV files under `templates/`, regenerated at app startup by `seed.ensure_templates()` from `field_specs.TABLE_SPECS`.

## Streamlined MDM template set
The app now prioritizes a non-redundant template set where **pack master upload** carries supplier + shipment-ready routing fields.

Primary uploads:
1. `pack_mdm_template.csv` (master pack data with supplier and freight routing)
2. `raw_bom_template.csv` (part_number + raw_qty input translated by pack rules)
3. `carrier_template.csv`, `rate_cards_template.csv`, `rate_charges_template.csv` (rate card and carrier data)
4. `lanes_template.csv` (lane defaults)

Legacy tables like separate supplier/SKU/pack-rule templates are intentionally de-emphasized in favor of pack MDM onboarding.

## Suggested import flow
1. Download the template for the dataset you want to load.
2. Fill rows in a spreadsheet without changing headers.
3. Save as CSV.
4. Import from **Templates & Upload Hub**.

## Pack master data import (`pack_mdm_template.csv`)
Use this when you want a canonical standard-pack contract mapped by SKU + supplier.

### Required columns
| Column | Required | Allowed values / notes |
| --- | --- | --- |
| `part_number` | Yes | SKU part number from `sku_master`. |
| `supplier_code` | Yes | Supplier code to tie the row to a supplier-specific SKU. |
| `pack_kg` | Yes | Numeric `>= 0.001`; gross weight per pack in kilograms (always kg, regardless of SKU UOM). |
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
| `hts_code` | No | Optional default HTS code captured on SKU master (format like `7208.39.0015`). |

Notes:
- `raw_qty`/demand quantity is interpreted in the SKU's `uom` (KG, METER, EA, etc.); this is a generic UOM field.
- `pack_kg` is always **kilograms per pack** and is separate from the SKU UOM.

## Raw BOM import (`raw_bom_template.csv`)
Use this upload when planners only have part-level required quantity and need pack conversion in-app.

| Column | Required | Description |
| --- | --- | --- |
| `part_number` | Yes | SKU part number from `sku_master`. |
| `raw_qty` | Yes | Raw required quantity before pack rounding. |
| `supplier_code` | No | Recommended when part exists under multiple suppliers. |
| `need_date` | No | Defaults to import date if omitted. |
| `phase`, `mode_override`, `service_scope`, `miles`, `notes` | No | Optional planning metadata. |

Behavior on import:
- Data is mapped to `sku_id` using `part_number` (+ `supplier_code` when needed).
- Rows are appended to `demand_lines` and translated to packs during planning using the default pack rule.

## Carrier and rate imports
- `carrier_template.csv` loads carrier master (`code`, `name`, `is_active`).
- `rate_cards_template.csv` loads lane/scope/equipment base rates and validity windows.
- `rate_charges_template.csv` loads accessorial charge lines linked to `rate_card_id`.

## Lane import (`lanes_template.csv`)
Lane data provides default service scope and miles fallbacks.

| Column | Required | Description |
| --- | --- | --- |
| `origin_code` | Yes | Origin node code. |
| `dest_code` | Yes | Destination node code. |
| `default_service_scope` | No | Optional fallback scope (`P2P`, `P2D`, `D2P`, `D2D`). |
| `default_miles` | No | Optional trucking miles default. |
