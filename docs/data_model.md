# Data Model

- **suppliers**: supplier master records.
- **sku_master**: supplier-specific SKU records (`part_number`, `supplier_id`).
- **packaging_rules**: multiple pack rules per SKU; exactly one default per SKU.
- **lead_times / lead_time_overrides**: transit assumptions by COO/mode or SKU/mode.
- **demand_lines**: demand linked to `sku_id`, with optional `pack_rule_id` override.
- **rate_card / rate_charge**: contract rates and accessorial charges.

Relationship focus: suppliers + supplier-specific SKUs + supplier-specific pack rules drive planning outputs.
