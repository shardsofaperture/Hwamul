"""Seed data and CSV template helpers."""
from __future__ import annotations

from pathlib import Path
import csv
import io

from db import get_conn
from field_specs import TABLE_SPECS


TEMPLATE_SPECS: list[tuple[str, str]] = [
    ("suppliers", "suppliers_template.csv"),
    ("skus", "skus_template.csv"),
    ("pack_rules", "pack_rules_template.csv"),
    ("pack_rules_import", "pack_mdm_template.csv"),
    ("lead_times", "lead_times_template.csv"),
    ("demand_import", "demand_template.csv"),
    ("rate_cards", "rate_cards_template.csv"),
    ("rate_charges", "rate_charges_template.csv"),
    ("customs_hts", "customs_hts_template.csv"),
]


def seed_if_empty() -> None:
    conn = get_conn()
    with conn:
        existing = conn.execute("SELECT COUNT(*) c FROM equipment_presets").fetchone()["c"]
        if existing:
            return
        conn.executemany(
            """
            INSERT INTO equipment_presets
            (equipment_code, name, mode, equipment_class, length_m, width_m, height_m, internal_length_m, internal_width_m, internal_height_m, max_payload_kg, volumetric_factor, is_reefer, is_high_cube, active, optional_constraints)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("TRL_53_STD", "53' Trailer (STD)", "TRUCK", "TRAILER", 16.15, 2.59, 2.70, 16.15, 2.59, 2.70, 20000, None, 0, 0, 1, "domestic dry van"),
                ("CNT_20_DRY_STD", "20' Dry (STD)", "OCEAN", "CONTAINER", 5.90, 2.35, 2.39, 5.90, 2.35, 2.39, 28200, None, 0, 0, 1, None),
                ("CNT_40_DRY_STD", "40' Dry (STD)", "OCEAN", "CONTAINER", 12.03, 2.35, 2.39, 12.03, 2.35, 2.39, 26700, None, 0, 0, 1, None),
                ("CNT_40_DRY_HC", "40' Dry (High Cube)", "OCEAN", "CONTAINER", 12.03, 2.352, 2.698, 12.03, 2.352, 2.698, 26540, None, 0, 1, 1, "40ft high cube dry"),
                ("CNT_20_RF", "20' Reefer", "OCEAN", "CONTAINER", 5.44, 2.29, 2.26, 5.44, 2.29, 2.26, 21100, None, 1, 0, 1, "temp controlled"),
                ("CNT_40_RF", "40' Reefer", "OCEAN", "CONTAINER", 11.588, 2.280, 2.255, 11.588, 2.280, 2.255, 29580, None, 1, 0, 1, "temp controlled"),
                ("CNT_49_STD", "49' Standard (User-defined)", "OCEAN", "CONTAINER", 14.93, 2.50, 2.70, 14.93, 2.50, 2.70, 19500, None, 0, 0, 1, "user-defined 49ft standard"),
                ("AIR_STD", "Air Freight (Chargeable Weight)", "AIR", "ULD", 1, 1, 1, 1, 1, 1, 50000, 167.0, 0, 0, 1, "volumetric factor kg/m3 configurable"),
            ],
        )
        conn.executemany(
            "INSERT OR IGNORE INTO phase_defaults(phase, default_mode, default_service_scope, manual_lead_override) VALUES (?,?,?,?)",
            [("Trial1", "AIR", "P2D", None), ("Trial2", "AIR", "P2D", None), ("Sample", "OCEAN", "P2D", None), ("Speed-up", "OCEAN", "P2D", None), ("Validation", "OCEAN", "P2D", None), ("SOP", "OCEAN", "P2D", None)],
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
                ("Ocean", "CNT_40_DRY_STD", "per_container", 3000, None, 200, 0, None),
                ("Ocean", "CNT_20_DRY_STD", "per_container", 1800, None, 200, 0, None),
                ("Truck", "TRL_53_STD", "per_load", 1800, None, 150, 0, None),
                ("Truck", "TRL_53_STD", "per_mile", 2.2, None, 100, 0, None),
                ("Air", "AIR_STD", "per_kg", 3.8, 250, 75, 0, None),
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
            (sku_id, pack_name, pack_type, is_default, units_per_pack, kg_per_unit, pack_tare_kg, dim_l_m, dim_w_m, dim_h_m, min_order_packs, increment_packs, stackable)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (sku_map["MFG-88421"], "STANDARD", "STANDARD", 1, 24, 1.2, 0.8, 0.55, 0.40, 0.35, 5, 5, 1),
                (sku_map["INT-100045"], "STANDARD", "STANDARD", 1, 12, 2.1, 1.0, 0.60, 0.45, 0.40, 2, 2, 1),
            ],
        )


def ensure_templates() -> None:
    template_dir = Path("templates")
    template_dir.mkdir(exist_ok=True)

    for table_key, fname in TEMPLATE_SPECS:
        cols = list(TABLE_SPECS[table_key].keys())
        sample_row = [TABLE_SPECS[table_key][col].example for col in cols]
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(cols)
        writer.writerow(sample_row)
        (template_dir / fname).write_text(buffer.getvalue(), encoding="utf-8")

    (template_dir / "bom_template.csv").write_text(
        "part_number,supplier_code,need_date,qty\nINT-100045,DEFAULT,2026-03-17,96\n"
    )
