# Hwamul Logistics Planner

Local Streamlit app for supplier-specific logistics planning, rate management, and shipment recommendation.

## Run locally
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
streamlit run app.py
```

## Quick Start (in-app configuration from scratch)
Use the left sidebar and complete this order:

1. **Admin → Suppliers**: create supplier codes/names.
2. **Admin → SKUs**: create supplier-specific SKUs (`part_number + supplier_id`).
3. **Admin → Pack rules**: add pack rules per SKU, with one default rule.
4. **Admin → Lead times**: maintain COO+mode lead days and optional SKU overrides.
5. **Admin → Carriers** then **Admin → Rate cards**: define rate cards and charges.
6. **Admin → Demand entry**: enter demand rows (`sku_id`, `need_date`, `qty`) or import CSV.
7. **Planner** tabs: run allocation, recommendations, shipment builder, and exports.

The app now includes an in-app **Docs** section with setup guidance, data model, rates guide, templates, and FAQ.
It also supports **Customs / HTS** administration for tracking HTS codes, tariff rates, section flags, and documentation requirements over time.

## Optional CSV import workflow
- Open **Docs → Import Templates** and download generated CSV templates.
- Fill template rows using examples/field guides.
- Upload in the matching admin area (demand CSV import is built-in; other templates are for guided bulk prep).

## Windows packaging notes
This repo includes Windows desktop packaging via `launcher.py` + `ProductionPlanner.spec`.

Build:
```bash
pip install -r requirements-win.txt
pyinstaller --clean --noconfirm ProductionPlanner.spec
```

Run packaged app:
```bash
.\dist\ProductionPlanner.exe
```

Notes:
- SQLite path in packaged app: `%APPDATA%\ProductionPlanner\planner.db`.
- Requires Edge WebView2 runtime.

## Tests
```bash
pytest
```
