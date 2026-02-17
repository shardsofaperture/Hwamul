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
