# Hwamul Logistics Planner

Local planning app for logistics mode selection + cube-out.

## Tech Stack
- Python 3.11+
- Streamlit UI
- SQLite storage (local file `planner.db`)
- Dataclass-based strongly typed domain models

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

## Run
```bash
streamlit run app.py
```

## What the app includes
1. **Equipment presets** (53ft trailer, 40/20 dry, 40/20 reefer, Air) with editable dimensions, payload, and volumetric factor.
2. **Lead times** by `(country_of_origin, mode)` plus SKU-mode overrides and manual recommendation override.
3. **Rates configuration** for ocean/truck/air pricing models and fixed/surcharge fields.
4. **Master data** for SKUs and packaging rules.
5. **CSV import** for demand/BOM data.
6. **Allocation** with tranche split, pack rounding (MOQ/increments), and excess carry-forward.
7. **Recommendations** with ship-by date, feasibility, cost, utilization, and ranking.
8. **Shipment builder** for mode consolidation and equipment/cube estimate.
9. **Exports** for `shipment_plan.csv`, `booking_summary.csv`, and `excess_report.csv`.

## CSV formats
Templates are generated in `templates/` at startup.

### demand_template.csv
Columns:
- `sku` (string)
- `need_date` (YYYY-MM-DD)
- `qty` (number, base UOM)
- `coo_override` (optional string)
- `priority` (optional string)
- `notes` (optional string)

### bom_template.csv
Columns:
- `sku`
- `need_date`
- `qty`

## Tests
```bash
pytest
```

Tests cover:
- pack rounding logic
- air chargeable weight logic
- equipment/container count estimate

## Notes
- Schema migrations run at app startup.
- Seed data is automatically loaded if tables are empty.
- All persistence is local-only in SQLite.
