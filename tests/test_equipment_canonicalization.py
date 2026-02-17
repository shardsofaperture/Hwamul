import sqlite3

import db
from planning_engine import plan_quick_run


def _apply_migrations_through(conn: sqlite3.Connection, max_version: int) -> None:
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    for version, script in db.MIGRATIONS:
        if version > max_version:
            break
        if callable(script):
            script(conn)
        else:
            conn.executescript(script)
        conn.execute("INSERT INTO schema_migrations(version) VALUES (?)", (version,))


def test_migration_15_merges_duplicate_equipment_and_remaps_refs() -> None:
    conn = sqlite3.connect(":memory:")
    _apply_migrations_through(conn, 14)

    with conn:
        conn.execute(
            "INSERT INTO equipment_presets(name, mode, internal_length_m, internal_width_m, internal_height_m, max_payload_kg) VALUES ('40rf', 'OCEAN', 11.58, 2.29, 2.26, 27500)"
        )
        old_a = conn.execute("SELECT id FROM equipment_presets WHERE name='40rf'").fetchone()[0]
        conn.execute(
            "INSERT INTO equipment_presets(name, mode, internal_length_m, internal_width_m, internal_height_m, max_payload_kg) VALUES ('40 reefer', 'Ocean', 11.58, 2.29, 2.26, 27500)"
        )
        old_b = conn.execute("SELECT id FROM equipment_presets WHERE name='40 reefer'").fetchone()[0]

        conn.execute("CREATE TABLE shipments(id INTEGER PRIMARY KEY AUTOINCREMENT, equipment_id INTEGER)")
        conn.execute("INSERT INTO shipments(equipment_id) VALUES (?)", (old_a,))
        conn.execute("INSERT INTO rate_card(mode, service_scope, equipment, dim_class, origin_type, origin_code, dest_type, dest_code, uom_pricing, base_rate, effective_from, is_active) VALUES ('OCEAN','P2P','40 reefer','STD','PORT','CNSHA','PORT','USLAX','PER_CONTAINER',1000,'2026-01-01',1)")

        db._migration_15_canonical_equipment_codes(conn)

    canonical = conn.execute(
        "SELECT id FROM equipment_presets WHERE equipment_code='CNT_40_RF' AND active=1"
    ).fetchall()
    assert len(canonical) == 1
    canonical_id = canonical[0][0]

    assert conn.execute("SELECT equipment_id FROM shipments").fetchone()[0] == canonical_id
    assert conn.execute("SELECT equipment FROM rate_card").fetchone()[0] == "CNT_40_RF"
    assert conn.execute("SELECT active FROM equipment_presets WHERE id=?", (old_a,)).fetchone()[0] == 0
    assert conn.execute("SELECT active FROM equipment_presets WHERE id=?", (old_b,)).fetchone()[0] == 0


def test_plan_quick_run_returns_only_active_equipment_codes(tmp_path) -> None:
    db.DB_PATH = tmp_path / "planner.db"
    db.run_migrations()

    conn = db.get_conn()
    with conn:
        conn.execute("INSERT OR IGNORE INTO suppliers(supplier_code, supplier_name) VALUES ('DEFAULT', 'Default Supplier')")
        supplier_id = conn.execute("SELECT supplier_id FROM suppliers WHERE supplier_code='DEFAULT'").fetchone()[0]
        conn.execute("INSERT INTO sku_master(part_number, supplier_id, description, default_coo) VALUES ('PN-1', ?, 'Part', 'CN')", (supplier_id,))
        sku_id = conn.execute("SELECT sku_id FROM sku_master WHERE part_number='PN-1' AND supplier_id=?", (supplier_id,)).fetchone()[0]
        conn.execute("""
            INSERT INTO packaging_rules(
                sku_id, pack_name, pack_type, is_default,
                units_per_pack, kg_per_unit, pack_tare_kg,
                dim_l_m, dim_w_m, dim_h_m, min_order_packs, increment_packs, stackable
            ) VALUES (?, 'STD', 'STANDARD', 1, 10, 1, 0.5, 0.5, 0.4, 0.3, 1, 1, 1)
        """, (sku_id,))
        conn.execute("INSERT OR IGNORE INTO lead_times(country_of_origin, mode, lead_days) VALUES ('CN', 'OCEAN', 30)")
        conn.execute(
            """
            INSERT INTO equipment_presets(
                equipment_code, name, mode,
                internal_length_m, internal_width_m, internal_height_m,
                max_payload_kg, active
            ) VALUES ('LEGACY_40RF', '40 reefer legacy', 'OCEAN', 11.58, 2.29, 2.26, 27500, 0)
            """
        )

    result = plan_quick_run(
        conn,
        sku_id=sku_id,
        required_units=120,
        need_date="2026-01-15",
        coo_override=None,
        pack_rule_id=None,
        lane_origin_code=None,
        lane_dest_code=None,
        service_scope="P2P",
        modes=["OCEAN", "AIR", "TRUCK"],
    )

    codes = [row["equipment_code"] for row in result["equipment"]]
    assert codes
    assert len(codes) == len(set(codes))
    assert "LEGACY_40RF" not in codes
