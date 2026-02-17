# Feature Verification Matrix

| Required feature | Where it exists in code | How to use in UI | Proof output | Gaps and exact fix list |
|---|---|---|---|---|
| 7 production phases with need dates | Scenario phase master is provided in `scenario_data/phases.csv`; pipeline reads and enforces it in `run_acceptance_pipeline` (`acceptance_pipeline.py`). | Existing UI can capture need dates in **Admin → Demand entry**, but does not model named phases explicitly. | `outputs/acceptance/phase_cost_summary.csv` includes all 7 phases and phase-level totals. | **Gap:** no dedicated phase table in Streamlit UI. **Fix:** add `production_phases` table + admin screen + FK from `demand_lines` (`phase_id`). |
| 16 supplier-specific SKUs (identity = part_number + supplier) | Existing schema uniqueness in `sku_master` supports `(part_number, supplier_id)` and scenario provides 16 combinations in `scenario_data/skus.csv`. | **Admin → Suppliers**, then **Admin → SKUs** to maintain supplier-scoped SKUs. | `outputs/acceptance/shipment_plan.csv` contains 16 planned rows keyed by `part_number` + `supplier_code`. | No blocking gap. |
| Supplier-specific pack rules drive cube/weight/equipment counts | Pack rule math in `models.py` (`PackagingRule`, `rounded_order_packs`, `estimate_equipment_count`); per-SKU supplier pack rules in `scenario_data/pack_rules.csv`; used in `acceptance_pipeline.py`. | **Admin → Pack rules** allows per supplier-SKU defaults/overrides. | Shipment plan includes `weight_kg`, `volume_m3`, `intl_equipment_count`, `truck_equipment_count`. | No blocking gap. |
| Mode selection by phase (trial1/trial2 AIR, later OCEAN) plus domestic TRUCK legs | Phase defaults in `scenario_data/phases.csv`; mode assignment + domestic truck leg cost in `acceptance_pipeline.py`. | UI recommendation tab is line-by-line and does not auto-assign by phase. | Shipment plan output shows `default_mode` by phase + `domestic_scope` and truck counts. | **Gap:** no phase-driven auto-planner in UI. **Fix:** add planner action that batches `demand_lines` by phase and applies phase default mode policy before recommendation override. |
| Routing country A -> country B -> plant with scope handling (P2P/P2D/D2P/D2D) + accessorials | Rate selection and scope matching exist in `rate_engine.py`; routing dataset in `scenario_data/routing.csv`; scope-driven rating in `acceptance_pipeline.py`; accessorial math via `compute_rate_total`. | **Admin → Rate cards** and **Admin → Rate Test** can validate scope/routing combinations manually. | `outputs/acceptance/itemized_rate_breakdown.csv` contains BASE and ACCESSORIAL lines for international and domestic legs. | **Gap:** no persisted route master in DB/UI. **Fix:** add `routes` table and planner linkage to avoid manual route embedding. |
| Total logistics cost rollups per phase and overall | Rollups computed in `acceptance_pipeline.py`. | UI has exports but not phase rollup widget. | `phase_cost_summary.csv` plus printed overall total from acceptance runner. | **Gap:** no built-in dashboard rollup. **Fix:** add Planner summary tab aggregating by phase and grand total. |
| Customs reporting output from shipment plan with supplier+SKU fields and customs attributes | Customs reference table exists in DB/UI (`customs_hts_rates` in `db.py`/`app.py`), and export-ready report is produced in `acceptance_pipeline.py` from shipment plan joins. | **Admin → Customs / HTS** manages customs metadata today. | `outputs/acceptance/customs_report.csv` includes COO, HTS, value, qty/uom, weights, seller/consignee, and ports. | **Gap:** export is not currently wired to Planner export tab. **Fix:** add customs report generator button in Planner → Export using shipment-plan joins. |

## Acceptance command

Run one command:

```bash
python tests/run_acceptance_test.py
```

This loads the sample scenario, runs planning + costing + customs export generation, asserts required artifacts, and writes CSV outputs under `outputs/acceptance/`.
