from __future__ import annotations

import importlib
from pathlib import Path

import pandas as pd


def _load_db_module(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PLANNER_DB_PATH", str(tmp_path / "planner.db"))
    import db  # noqa: F401

    return importlib.reload(db)


def _sample_df(ship_tos: str = "USLAX_DC01|USLGB_DC02", modes: str = "OCEAN|TRUCK", pack_kg: float = 24.5) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "part_number": "PN_10001",
                "supplier_code": "MAEU",
                "pack_kg": pack_kg,
                "length_mm": 1200,
                "width_mm": 800,
                "height_mm": 900,
                "is_stackable": 1,
                "max_stack": 3,
                "ship_from_city": "SHANGHAI",
                "ship_from_port_code": "CNSHA",
                "ship_from_duns": "123456789",
                "ship_from_location_code": "CN_SHA_PDC",
                "ship_to_locations": ship_tos,
                "allowed_modes": modes,
                "incoterm": "FOB",
                "incoterm_named_place": "SHANGHAI PORT",
                "pack_name": "STD_PN_10001",
            }
        ]
    )


def test_pack_master_import_is_idempotent_and_updates_fields(tmp_path: Path, monkeypatch) -> None:
    db = _load_db_module(tmp_path, monkeypatch)
    db.run_migrations()

    from services.master_data_import import apply_pack_master_import

    conn = db.get_conn()
    first = apply_pack_master_import(conn, _sample_df())
    second = apply_pack_master_import(conn, _sample_df(pack_kg=30.0))

    assert first.packaging_rules_upserted == 1
    assert second.packaging_rules_upserted == 1

    counts = {
        "suppliers": conn.execute("SELECT COUNT(*) AS c FROM suppliers").fetchone()["c"],
        "sku_master": conn.execute("SELECT COUNT(*) AS c FROM sku_master").fetchone()["c"],
        "packaging_rules": conn.execute("SELECT COUNT(*) AS c FROM packaging_rules").fetchone()["c"],
        "ship_from_locations": conn.execute("SELECT COUNT(*) AS c FROM ship_from_locations").fetchone()["c"],
    }
    assert counts == {
        "suppliers": 2,  # DEFAULT + MAEU
        "sku_master": 1,
        "packaging_rules": 1,
        "ship_from_locations": 1,
    }

    updated_weight = conn.execute("SELECT kg_per_unit FROM packaging_rules").fetchone()["kg_per_unit"]
    assert updated_weight == 30.0


def test_pack_master_import_replace_set_behavior(tmp_path: Path, monkeypatch) -> None:
    db = _load_db_module(tmp_path, monkeypatch)
    db.run_migrations()

    from services.master_data_import import apply_pack_master_import

    conn = db.get_conn()
    apply_pack_master_import(conn, _sample_df())
    apply_pack_master_import(conn, _sample_df(ship_tos="USLAX_DC01", modes="OCEAN"))

    sku_id = conn.execute("SELECT sku_id FROM sku_master LIMIT 1").fetchone()["sku_id"]
    ship_tos = conn.execute(
        "SELECT destination_code FROM sku_ship_to_locations WHERE sku_id = ? ORDER BY destination_code",
        (sku_id,),
    ).fetchall()
    modes = conn.execute(
        "SELECT mode_code FROM sku_allowed_modes WHERE sku_id = ? ORDER BY mode_code",
        (sku_id,),
    ).fetchall()

    assert [r["destination_code"] for r in ship_tos] == ["USLAX_DC01"]
    assert [r["mode_code"] for r in modes] == ["OCEAN"]
