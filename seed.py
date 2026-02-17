"""Seed data and CSV template helpers."""
from __future__ import annotations

from pathlib import Path

from db import get_conn


def seed_if_empty() -> None:
    conn = get_conn()
    with conn:
        existing = conn.execute("SELECT COUNT(*) c FROM equipment_presets").fetchone()["c"]
        if existing:
            return
        conn.executemany(
            """
            INSERT INTO equipment_presets
            (name, mode, length_m, width_m, height_m, max_payload_kg, volumetric_factor, optional_constraints)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("53ft Trailer", "Truck", 16.15, 2.59, 2.70, 20000, None, "domestic dry van"),
                ("40 Dry", "Ocean", 12.03, 2.35, 2.39, 26700, None, None),
                ("20 Dry", "Ocean", 5.90, 2.35, 2.39, 28200, None, None),
                ("40 Reefer", "Ocean", 11.58, 2.29, 2.26, 27500, None, "temp controlled"),
                ("20 Reefer", "Ocean", 5.44, 2.29, 2.26, 21100, None, "temp controlled"),
                ("Air", "Air", 1, 1, 1, 50000, 167.0, "volumetric factor kg/m3 configurable"),
            ],
        )
        conn.executemany(
            "INSERT INTO lead_times(country_of_origin, mode, lead_days) VALUES (?, ?, ?)",
            [
                ("CN", "Ocean", 35),
                ("CN", "Air", 7),
                ("CN", "Truck", 20),
                ("MX", "Truck", 5),
                ("MX", "Air", 3),
            ],
        )
        conn.executemany(
            "INSERT INTO rates(mode, equipment_name, pricing_model, rate_value, minimum_charge, fixed_fee, surcharge, notes) VALUES (?,?,?,?,?,?,?,?)",
            [
                ("Ocean", "40 Dry", "per_container", 3000, None, 200, 0, None),
                ("Ocean", "20 Dry", "per_container", 1800, None, 200, 0, None),
                ("Truck", "53ft Trailer", "per_load", 1800, None, 150, 0, None),
                ("Truck", "53ft Trailer", "per_mile", 2.2, None, 100, 0, None),
                ("Air", "Air", "per_kg", 3.8, 250, 75, 0, None),
            ],
        )
        conn.execute("INSERT OR IGNORE INTO suppliers(supplier_code, supplier_name) VALUES (?,?)", ("DEFAULT", "Default Supplier"))
        supplier_id = conn.execute("SELECT supplier_id FROM suppliers WHERE supplier_code='DEFAULT'").fetchone()["supplier_id"]
        conn.executemany(
            "INSERT INTO sku_master(part_number, supplier_id, description, default_coo) VALUES (?,?,?,?)",
            [
                ("MFG-88421", supplier_id, "Sample Widget", "CN"),
                ("INT-100045", supplier_id, "Sample Gizmo", "MX"),
            ],
        )
        sku_map = {r["part_number"]: r["sku_id"] for r in conn.execute("SELECT sku_id, part_number FROM sku_master WHERE supplier_id=?", (supplier_id,)).fetchall()}
        conn.executemany(
            """
            INSERT INTO packaging_rules
            (sku_id, units_per_pack, kg_per_unit, pack_tare_kg, pack_length_m, pack_width_m, pack_height_m, min_order_packs, increment_packs, stackable)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (sku_map["MFG-88421"], 24, 1.2, 0.8, 0.55, 0.40, 0.35, 5, 5, 1),
                (sku_map["INT-100045"], 12, 2.1, 1.0, 0.60, 0.45, 0.40, 2, 2, 1),
            ],
        )


def ensure_templates() -> None:
    template_dir = Path("templates")
    template_dir.mkdir(exist_ok=True)
    (template_dir / "demand_template.csv").write_text(
        "part_number,supplier_code,need_date,qty,coo_override,priority,notes\nMFG-88421,DEFAULT,2026-03-10,250,,High,launch\n"
    )
    (template_dir / "bom_template.csv").write_text(
        "part_number,supplier_code,need_date,qty\nINT-100045,DEFAULT,2026-03-17,96\n"
    )
