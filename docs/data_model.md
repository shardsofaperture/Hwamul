# Data Model Overview

Core entities used by planning workflows:

- **suppliers**: supplier master (`supplier_code`, `supplier_name`).
- **sku_master**: supplier-specific part numbers (`part_number`, `supplier_id`, `default_coo`).
- **packaging_rules**: pack variants per SKU (`pack_name`, dimensions, stack settings, defaults). Dimension uploads support cm (preferred for pallets/crates) and legacy m values.
- **lead_times** / **lead_time_overrides**: baseline and SKU-level transit assumptions.
- **equipment_presets**: mode/equipment dimensions and payload constraints.
- **rates**: legacy/simple freight rates table used in planning flows.
- **carrier**, **rate_card**, **rate_charge**: normalized contract and accessorial structures.
- **customs_hts_rates**: HTS/tariff records with effective windows, section flags, doc requirements, links, and notes.
- **demand_lines** / **tranche_allocations**: demand intake and allocation outcomes.

## Relationship highlights
- A supplier can have many SKUs.
- Each SKU can have many pack rules, with one default rule.
- Demand lines reference SKUs and can optionally use pack-rule overrides.
- Rate cards and charges are evaluated with lane + scope + equipment matching.
- Customs attributes are maintained as effective-dated records for reporting continuity.

## Data integrity notes
- Admin grids save through upsert/delete-diff behavior.
- Date-window checks are applied for key effective-dated tables.
- Export bundles include customs and planning history for auditability.
