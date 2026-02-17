import sqlite3

import pandas as pd

from db import compute_grid_diff, delete_rows, upsert_rows


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
