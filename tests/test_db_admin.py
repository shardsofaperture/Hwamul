import sqlite3

import pandas as pd

import db
from db import (
    compute_grid_diff,
    delete_rows,
    export_data_bundle,
    import_data_bundle,
    purge_demand_before,
    run_migrations,
    upsert_rows,
)


def test_compute_grid_diff_insert_update_delete():
    original = pd.DataFrame(
        [
            {"id": 1, "name": "A", "value": 10},
            {"id": 2, "name": "B", "value": 20},
        ]
    )
    edited = pd.DataFrame(
        [
            {"id": 1, "name": "A", "value": 15},
            {"id": 3, "name": "C", "value": 30},
        ]
    )

    inserts, updates, deletes = compute_grid_diff(original, edited, ["id"])

    assert inserts["id"].tolist() == [3]
    assert updates["id"].tolist() == [1]
    assert deletes["id"].tolist() == [2]


def test_upsert_and_delete_rows():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL)")

    seed = pd.DataFrame([{"id": 1, "name": "A", "value": 10}])
    upsert_rows(conn, "t", seed, ["id"])

    changed = pd.DataFrame(
        [
            {"id": 1, "name": "A2", "value": 12},
            {"id": 2, "name": "B", "value": 20},
        ]
    )
    upsert_rows(conn, "t", changed, ["id"])

    rows = conn.execute("SELECT id, name, value FROM t ORDER BY id").fetchall()
    assert rows == [(1, "A2", 12.0), (2, "B", 20.0)]

    delete_rows(conn, "t", pd.DataFrame([{"id": 1}]), ["id"])
    rows = conn.execute("SELECT id FROM t ORDER BY id").fetchall()
    assert rows == [(2,)]


def test_export_import_and_purge(tmp_path):
    db_path = tmp_path / "planner.db"
    db.DB_PATH = db_path
    run_migrations()

    conn = db.get_conn()
    with conn:
        conn.execute("INSERT OR IGNORE INTO suppliers(supplier_code, supplier_name) VALUES ('DEFAULT', 'Default Supplier')")
        supplier_id = conn.execute("SELECT supplier_id FROM suppliers WHERE supplier_code='DEFAULT'").fetchone()[0]
        conn.execute(
            "INSERT INTO sku_master(part_number, supplier_id, description, default_coo) VALUES ('PN1', ?, 'Part 1', 'US')",
            (supplier_id,),
        )
        sku_id = conn.execute(
            "SELECT sku_id FROM sku_master WHERE part_number='PN1' AND supplier_id=?",
            (supplier_id,),
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO rates(mode, pricing_model, rate_value, effective_start, effective_end)
            VALUES ('Ocean', 'per_container', 1000, '2020-01-01', '2099-12-31')
            """
        )
        conn.execute(
            """
            INSERT INTO demand_lines(sku_id, need_date, qty, notes)
            VALUES (?, '2020-01-01', 10, 'old')
            """,
            (sku_id,),
        )

    full = export_data_bundle("full")
    recent = export_data_bundle("recent")
    history = export_data_bundle("history")

    assert b'"profile": "full"' in full
    assert b'"profile": "recent"' in recent
    assert b'"profile": "history"' in history

    stats = import_data_bundle(full)
    assert stats["sku_master"] == 1
    assert stats["rates"] == 1
    assert stats["demand_lines"] == 1

    deleted = purge_demand_before("2021-01-01")
    assert deleted["demand_lines"] == 1


def test_supplier_and_sku_uniqueness_constraints(tmp_path):
    db.DB_PATH = tmp_path / "planner.db"
    run_migrations()
    conn = db.get_conn()
    with conn:
        conn.execute("INSERT INTO suppliers(supplier_code, supplier_name) VALUES ('S1', 'Supplier 1')")
        supplier_id = conn.execute("SELECT supplier_id FROM suppliers WHERE supplier_code='S1'").fetchone()[0]
        conn.execute("INSERT INTO sku_master(part_number, supplier_id, default_coo) VALUES ('PNX', ?, 'US')", (supplier_id,))

    with conn:
        try:
            conn.execute("INSERT INTO sku_master(part_number, supplier_id, default_coo) VALUES ('PNX', ?, 'US')", (supplier_id,))
            assert False, "Expected unique(part_number, supplier_id) violation"
        except sqlite3.IntegrityError:
            pass


def test_migration_v6_moves_part_number_tables_to_sku_id(tmp_path):
    db.DB_PATH = tmp_path / "planner.db"
    conn = sqlite3.connect(db.DB_PATH)
    conn.row_factory = sqlite3.Row
    with conn:
        for version, script in db.MIGRATIONS:
            if version > 5:
                break
            conn.executescript(script)
            conn.execute("INSERT INTO schema_migrations(version) VALUES (?)", (version,))

        conn.execute("INSERT INTO sku_master(part_number, description, default_coo) VALUES ('LEGACY-1', 'Legacy', 'CN')")
        conn.execute(
            """
            INSERT INTO packaging_rules(part_number, pack_type, is_default, units_per_pack, kg_per_unit, pack_tare_kg, pack_length_m, pack_width_m, pack_height_m)
            VALUES ('LEGACY-1', 'STD', 1, 10, 1.0, 0.5, 0.1, 0.1, 0.1)
            """
        )
        conn.execute("INSERT INTO demand_lines(part_number, need_date, qty) VALUES ('LEGACY-1', '2026-01-01', 50)")
        conn.execute("INSERT INTO lead_time_overrides(part_number, mode, lead_days) VALUES ('LEGACY-1', 'Ocean', 22)")

    db.run_migrations()
    migrated = db.get_conn()
    suppliers = [r[0] for r in migrated.execute("SELECT supplier_code FROM suppliers").fetchall()]
    assert "DEFAULT" in suppliers

    row = migrated.execute(
        """
        SELECT sm.sku_id, sm.part_number, s.supplier_code
        FROM sku_master sm
        JOIN suppliers s ON s.supplier_id = sm.supplier_id
        WHERE sm.part_number='LEGACY-1'
        """
    ).fetchone()
    assert row[1] == "LEGACY-1"
    assert row[2] == "DEFAULT"

    assert migrated.execute("SELECT COUNT(*) FROM packaging_rules WHERE sku_id=?", (row[0],)).fetchone()[0] == 1
    assert migrated.execute("SELECT COUNT(*) FROM demand_lines WHERE sku_id=?", (row[0],)).fetchone()[0] == 1
    assert migrated.execute("SELECT COUNT(*) FROM lead_time_overrides WHERE sku_id=?", (row[0],)).fetchone()[0] == 1


def test_migration_v7_backfills_packaging_rules_sku_id_from_part_number(tmp_path):
    db.DB_PATH = tmp_path / "planner.db"
    conn = sqlite3.connect(db.DB_PATH)
    with conn:
        conn.execute(
            """
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        for version in range(1, 7):
            conn.execute("INSERT INTO schema_migrations(version) VALUES (?)", (version,))

        conn.execute(
            """
            CREATE TABLE sku_master (
                sku_id INTEGER PRIMARY KEY AUTOINCREMENT,
                part_number TEXT NOT NULL,
                supplier_id INTEGER NOT NULL,
                description TEXT,
                default_coo TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE packaging_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                part_number TEXT NOT NULL,
                pack_type TEXT NOT NULL DEFAULT 'STANDARD',
                is_default INTEGER NOT NULL DEFAULT 1,
                units_per_pack REAL NOT NULL,
                kg_per_unit REAL NOT NULL,
                pack_tare_kg REAL NOT NULL,
                pack_length_m REAL NOT NULL,
                pack_width_m REAL NOT NULL,
                pack_height_m REAL NOT NULL,
                min_order_packs INTEGER NOT NULL DEFAULT 1,
                increment_packs INTEGER NOT NULL DEFAULT 1,
                stackable INTEGER NOT NULL DEFAULT 1,
                UNIQUE(part_number, pack_type)
            )
            """
        )
        conn.execute(
            "INSERT INTO sku_master(part_number, supplier_id, description, default_coo) VALUES ('LEGACY-2', 1, 'Legacy 2', 'KR')"
        )
        conn.execute(
            """
            INSERT INTO packaging_rules(part_number, pack_type, is_default, units_per_pack, kg_per_unit, pack_tare_kg, pack_length_m, pack_width_m, pack_height_m)
            VALUES ('LEGACY-2', 'CASE', 1, 12, 1.2, 0.6, 0.3, 0.2, 0.1)
            """
        )

    db.run_migrations()
    migrated = db.get_conn()
    cols = [r[1] for r in migrated.execute("PRAGMA table_info(packaging_rules)").fetchall()]
    assert "sku_id" in cols
    assert "part_number" not in cols
    assert migrated.execute("SELECT COUNT(*) FROM packaging_rules WHERE sku_id IS NOT NULL").fetchone()[0] == 1
    idx = [r[1] for r in migrated.execute("PRAGMA index_list('packaging_rules')").fetchall()]
    assert "idx_packaging_rules_sku_id" in idx


def test_packaging_rules_crud_with_pack_name_key(tmp_path):
    db.DB_PATH = tmp_path / "planner.db"
    run_migrations()
    conn = db.get_conn()

    with conn:
        conn.execute("INSERT INTO suppliers(supplier_code, supplier_name) VALUES ('S2', 'Supplier 2')")
        supplier_id = conn.execute("SELECT supplier_id FROM suppliers WHERE supplier_code='S2'").fetchone()[0]
        conn.execute(
            "INSERT INTO sku_master(part_number, supplier_id, description, default_coo) VALUES ('PN-C', ?, 'Composite', 'US')",
            (supplier_id,),
        )
        sku_id = conn.execute(
            "SELECT sku_id FROM sku_master WHERE part_number='PN-C' AND supplier_id=?",
            (supplier_id,),
        ).fetchone()[0]

    seed = pd.DataFrame(
        [
            {
                "sku_id": sku_id,
                "pack_name": "CASE",
                "pack_type": "CASE",
                "is_default": 1,
                "units_per_pack": 10,
                "kg_per_unit": 1.0,
                "pack_tare_kg": 0.5,
                "dim_l_m": 0.2,
                "dim_w_m": 0.2,
                "dim_h_m": 0.2,
                "min_order_packs": 1,
                "increment_packs": 1,
                "stackable": 1,
            }
        ]
    )
    upsert_rows(conn, "packaging_rules", seed, ["sku_id", "pack_name"])

    changed = seed.copy()
    changed.loc[0, "units_per_pack"] = 24
    upsert_rows(conn, "packaging_rules", changed, ["sku_id", "pack_name"])

    updated_units = conn.execute(
        "SELECT units_per_pack FROM packaging_rules WHERE sku_id=? AND pack_name='CASE'",
        (sku_id,),
    ).fetchone()[0]
    assert updated_units == 24


def test_single_default_pack_rule_per_sku_enforced(tmp_path):
    db.DB_PATH = tmp_path / "planner.db"
    run_migrations()
    conn = db.get_conn()

    with conn:
        conn.execute("INSERT INTO suppliers(supplier_code, supplier_name) VALUES ('S3', 'Supplier 3')")
        supplier_id = conn.execute("SELECT supplier_id FROM suppliers WHERE supplier_code='S3'").fetchone()[0]
        conn.execute(
            "INSERT INTO sku_master(part_number, supplier_id, description, default_coo) VALUES ('PN-D', ?, 'D', 'US')",
            (supplier_id,),
        )
        sku_id = conn.execute("SELECT sku_id FROM sku_master WHERE part_number='PN-D' AND supplier_id=?", (supplier_id,)).fetchone()[0]
        conn.execute(
            """
            INSERT INTO packaging_rules(sku_id, pack_name, pack_type, is_default, units_per_pack, kg_per_unit, pack_tare_kg, dim_l_m, dim_w_m, dim_h_m)
            VALUES (?, 'CASE', 'CASE', 1, 10, 1, 0.1, 0.2, 0.2, 0.2)
            """,
            (sku_id,),
        )

    with conn:
        try:
            conn.execute(
                """
                INSERT INTO packaging_rules(sku_id, pack_name, pack_type, is_default, units_per_pack, kg_per_unit, pack_tare_kg, dim_l_m, dim_w_m, dim_h_m)
                VALUES (?, 'PALLET', 'PALLET', 1, 100, 1, 1, 1, 1, 1)
                """,
                (sku_id,),
            )
            assert False, "Expected single-default unique index violation"
        except sqlite3.IntegrityError:
            pass


def test_map_demand_rows_to_sku_id_with_supplier_code(tmp_path):
    db.DB_PATH = tmp_path / "planner.db"
    run_migrations()
    catalog = pd.DataFrame(
        [
            {"sku_id": 1, "part_number": "PN1", "supplier_code": "S1"},
            {"sku_id": 2, "part_number": "PN1", "supplier_code": "S2"},
        ]
    )
    imported = pd.DataFrame([{"part_number": "PN1", "supplier_code": "S2", "qty": 10, "need_date": "2026-01-01"}])
    merged, errors = db.map_import_demand_rows(imported, catalog)
    assert not errors
    assert int(merged.loc[0, "sku_id"]) == 2


def test_pack_rounding_uses_default_or_override_pack_rule(tmp_path):
    db.DB_PATH = tmp_path / "planner.db"
    run_migrations()
    conn = db.get_conn()

    with conn:
        conn.execute("INSERT INTO suppliers(supplier_code, supplier_name) VALUES ('S4', 'Supplier 4')")
        supplier_id = conn.execute("SELECT supplier_id FROM suppliers WHERE supplier_code='S4'").fetchone()[0]
        conn.execute(
            "INSERT INTO sku_master(part_number, supplier_id, description, default_coo) VALUES ('PN-R', ?, 'R', 'US')",
            (supplier_id,),
        )
        sku_id = conn.execute("SELECT sku_id FROM sku_master WHERE part_number='PN-R' AND supplier_id=?", (supplier_id,)).fetchone()[0]

        conn.execute(
            """
            INSERT INTO packaging_rules(sku_id, pack_name, pack_type, is_default, units_per_pack, kg_per_unit, pack_tare_kg, dim_l_m, dim_w_m, dim_h_m)
            VALUES (?, 'SMALL', 'CASE', 1, 10, 1, 0, 0.1, 0.1, 0.1)
            """,
            (sku_id,),
        )
        conn.execute(
            """
            INSERT INTO packaging_rules(sku_id, pack_name, pack_type, is_default, units_per_pack, kg_per_unit, pack_tare_kg, dim_l_m, dim_w_m, dim_h_m)
            VALUES (?, 'LARGE', 'PALLET', 0, 25, 1, 0, 0.2, 0.2, 0.2)
            """,
            (sku_id,),
        )
        override_id = conn.execute("SELECT id FROM packaging_rules WHERE sku_id=? AND pack_name='LARGE'", (sku_id,)).fetchone()[0]
        conn.execute("INSERT INTO demand_lines(sku_id, need_date, qty, pack_rule_id) VALUES (?, '2026-01-01', 51, NULL)", (sku_id,))
        conn.execute("INSERT INTO demand_lines(sku_id, need_date, qty, pack_rule_id) VALUES (?, '2026-01-01', 51, ?)", (sku_id, override_id))

    rows = conn.execute("SELECT * FROM demand_lines ORDER BY id").fetchall()
    default_rule = db.resolve_pack_rule_for_demand(conn, rows[0])
    override_rule = db.resolve_pack_rule_for_demand(conn, rows[1])

    from models import PackagingRule, rounded_order_packs

    default_packs = rounded_order_packs(51, PackagingRule(**{k: default_rule[k] for k in PackagingRule.__dataclass_fields__.keys() if k in default_rule.keys()}))
    override_packs = rounded_order_packs(51, PackagingRule(**{k: override_rule[k] for k in PackagingRule.__dataclass_fields__.keys() if k in override_rule.keys()}))

    assert default_packs == 6
    assert override_packs == 3


def test_customs_hts_migration_and_export_profiles(tmp_path):
    db.DB_PATH = tmp_path / "planner.db"
    run_migrations()
    conn = db.get_conn()
    with conn:
        conn.execute("INSERT OR IGNORE INTO suppliers(supplier_code, supplier_name) VALUES ('SHTS', 'HTS Supplier')")
        supplier_id = conn.execute("SELECT supplier_id FROM suppliers WHERE supplier_code='SHTS'").fetchone()[0]
        conn.execute(
            "INSERT INTO sku_master(part_number, supplier_id, description, default_coo) VALUES ('HTS-1', ?, 'HTS Part', 'CN')",
            (supplier_id,),
        )
        sku_id = conn.execute(
            "SELECT sku_id FROM sku_master WHERE part_number='HTS-1' AND supplier_id=?",
            (supplier_id,),
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO customs_hts_rates(
                sku_id, hts_code, material_input, country_of_origin, tariff_program,
                base_duty_rate, tariff_rate, section_232, section_301,
                domestic_trucking_required, port_to_ramp_required,
                special_documentation_required, documentation_notes,
                effective_from, effective_to, notes
            ) VALUES (?, '7208.39.0015', 'Hot rolled steel coil', 'CN', 'MFN',
                      2.5, 25, 1, 0, 1, 1, 1, 'Mill cert required',
                      '2024-01-01', '2099-12-31', 'active row')
            """,
            (sku_id,),
        )
        conn.execute(
            """
            INSERT INTO customs_hts_rates(
                sku_id, hts_code, material_input, country_of_origin, tariff_program,
                base_duty_rate, tariff_rate, section_232, section_301,
                domestic_trucking_required, port_to_ramp_required,
                special_documentation_required, documentation_notes,
                effective_from, effective_to, notes
            ) VALUES (?, '7208.39.0015', 'Hot rolled steel coil', 'CN', 'MFN',
                      2.5, 10, 1, 0, 0, 0, 0, 'legacy',
                      '2020-01-01', '2020-12-31', 'expired row')
            """,
            (sku_id,),
        )

    recent = export_data_bundle("recent")
    history = export_data_bundle("history")

    assert b'"customs_hts_rates"' in recent
    assert b'"active row"' in recent
    assert b'"expired row"' not in recent

    assert b'"customs_hts_rates"' in history
    assert b'"expired row"' in history
