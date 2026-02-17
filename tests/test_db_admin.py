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
        conn.execute("INSERT INTO sku_master(part_number, description, default_coo) VALUES ('PN1', 'Part 1', 'US')")
        conn.execute(
            """
            INSERT INTO rates(mode, pricing_model, rate_value, effective_start, effective_end)
            VALUES ('Ocean', 'per_container', 1000, '2020-01-01', '2099-12-31')
            """
        )
        conn.execute(
            """
            INSERT INTO demand_lines(part_number, need_date, qty, notes)
            VALUES ('PN1', '2020-01-01', 10, 'old')
            """
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
