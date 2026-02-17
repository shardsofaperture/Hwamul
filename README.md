# Hwamul Logistics Planner

Local-first Streamlit application for supplier-specific logistics planning, rate management, shipment recommendation, and customs tracking.

## What this app helps you do
- Maintain supplier-specific SKU masters (location, Incoterms, UOM) and packaging rules.
- Plan inbound demand by lead time + rate assumptions.
- Build shipment recommendations and simple consolidation previews.
- Manage carriers, rate cards, and charge structures.
- Track customs / HTS tariff attributes across effective dates.
- Export planning snapshots and import/export full data bundles.

## Run locally
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
streamlit run app.py
```

## First-time setup checklist (UI)
Use the sidebar in this order for a clean initial setup:

1. **Admin → Suppliers**
   - Add each supplier using `supplier_code` + `supplier_name`.
2. **Admin → SKUs**
   - Add supplier-specific SKUs (`part_number + supplier_id`) and logistics defaults (`source_location`, `incoterm`, `uom`).
3. **Admin → Pack rules**
   - Add at least one default packing rule for each SKU.
4. **Admin → Lead times**
   - Enter baseline COO + mode lead times (optionally SKU overrides).
5. **Admin → Carriers**
   - Create carrier records before building rate cards.
6. **Admin → Rate cards** and **Admin → Rates**
   - Define dated pricing and charges by lane/scope/equipment.
7. **Admin → Customs / HTS**
   - Enter HTS, tariff, section flags, and documentation requirements.
8. **Admin → Demand entry**
   - Enter rows manually or import demand CSV.
9. **Planner tabs**
   - Run allocation, recommendations, cube-out calculations, shipment building, and exports.

## Built-in docs map
Open **Docs** in-app for operator guidance:
- **Quick Start**: setup sequence and day-one workflow.
- **Data Model**: core tables and relationships.
- **Rates Guide**: pricing/scopes/accessorial process.
- **Customs Guide**: HTS/tariff/documentation process.
- **Import Templates**: downloadable CSV templates.
- **FAQ/Troubleshooting**: common validation and data issues.

## CSV template workflow
1. Open **Docs → Import Templates**.
2. Download the relevant template.
3. Populate rows using the examples in the template.
4. Upload in the matching admin page (demand import is directly available in-app).

## Data storage and portability
- Data is stored locally in `planner.db`.
- Use **Admin → Data management** to export/import full data bundles.
- Use vacuum/purge options as needed to manage local DB size/history.

## Windows packaging notes
Desktop packaging is included via `launcher.py` + `ProductionPlanner.spec`.

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
- Packaged SQLite path: `%APPDATA%\ProductionPlanner\planner.db`.
- Requires Edge WebView2 runtime.

## Tests
```bash
pytest
```
