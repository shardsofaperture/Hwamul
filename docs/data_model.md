# Data Model Overview

Core entities and relationships:

- **suppliers**: supplier master (`supplier_code`, `supplier_name`).
- **sku_master**: supplier-specific part numbers (`part_number`, `supplier_id`, `default_coo`).
- **packaging_rules**: pack variants per SKU (`pack_name`, dimensions, stack settings, defaults).
- **lead_times** / **lead_time_overrides**: baseline and SKU-level lead times.
- **equipment_presets**: planning equipment dimensions and payload constraints.
- **rates**: legacy freight rates table for planning modes/equipment.
- **carrier**, **rate_card**, **rate_charge**: normalized freight contract/rate engine structures.
- **customs_hts_rates**: HTS-based customs/tariff records with effective dates, section flags, documentation requirements, tariff change notes over time, documentation links, tips, and domestic supplier service needs (domestic trucking, port-to-ramp).
- **demand_lines** / **tranche_allocations**: inbound demand and sourcing allocations.

Notes:
- Most admin grids save via upsert + delete diffing.
- Effective date windows are validated in-app for key time-based tables.
- Export bundles include customs HTS history for auditability.
