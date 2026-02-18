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


def test_pack_master_validation_report_hard_fail_rules() -> None:
    from services.master_data_import import validate_pack_master_import

    df = _sample_df().copy()
    df.loc[0, "pack_kg"] = 0
    df.loc[0, "length_mm"] = -1
    df.loc[0, "allowed_modes"] = "OCEAN|RAIL"
    df.loc[0, "incoterm"] = "XYZ"
    df.loc[0, "is_stackable"] = 1
    df.loc[0, "max_stack"] = ""

    report = validate_pack_master_import(df)

    codes = {issue.code for issue in report.errors}
    assert "NON_POSITIVE_VALUE" in codes
    assert "INVALID_MODE_TOKEN" in codes
    assert "INVALID_INCOTERM" in codes
    assert "INVALID_MAX_STACK" in codes
    assert report.summary["failed"] == 1
    assert report.summary["accepted"] == 0


def test_pack_master_validation_report_warnings() -> None:
    from services.master_data_import import validate_pack_master_import

    df = pd.concat([_sample_df(), _sample_df()], ignore_index=True)
    df.loc[0, "ship_from_port_code"] = "SHA"

    report = validate_pack_master_import(df)

    warning_codes = {issue.code for issue in report.warnings}
    assert "WEAK_PORT_CODE_FORMAT" in warning_codes
    assert "DUPLICATE_SUPPLIER_PART_VARIANT" in warning_codes
    assert report.summary["warned"] >= 1


def test_apply_pack_master_upload_returns_validation_payload(tmp_path: Path, monkeypatch) -> None:
    db = _load_db_module(tmp_path, monkeypatch)
    db.run_migrations()

    import app

    report_df = _sample_df().copy()
    report_df.loc[0, "allowed_modes"] = "OCEAN|RAIL"

    ok, msg, payload = app.apply_pack_master_upload(report_df)

    assert not ok
    assert "failed validation" in msg
    assert "errors" in payload
    assert payload["summary"]["failed"] == 1


def test_normalize_delimited_tokens_dedupes_and_normalizes(tmp_path: Path, monkeypatch) -> None:
    db = _load_db_module(tmp_path, monkeypatch)

    out = db.normalize_delimited_tokens("  ocean |TRUCK|ocean|| air ")

    assert out == ["AIR", "OCEAN", "TRUCK"]


def test_replace_sku_token_set_replaces_existing_rows(tmp_path: Path, monkeypatch) -> None:
    db = _load_db_module(tmp_path, monkeypatch)
    db.run_migrations()
    conn = db.get_conn()
    conn.execute("INSERT INTO suppliers(supplier_code, supplier_name) VALUES (?, ?)", ("TST", "Test Supplier"))
    supplier_id = conn.execute("SELECT supplier_id FROM suppliers WHERE supplier_code = ?", ("TST",)).fetchone()["supplier_id"]
    conn.execute("INSERT INTO sku_master(part_number, supplier_id, description, default_coo) VALUES (?, ?, ?, ?)", ("PN_REPLACE", supplier_id, "desc", "CN"))

    sku_id = conn.execute("SELECT sku_id FROM sku_master WHERE part_number = ?", ("PN_REPLACE",)).fetchone()["sku_id"]
    conn.execute("INSERT INTO sku_ship_to_locations(sku_id, destination_code) VALUES (?, ?)", (sku_id, "OLD_DC"))

    inserted = db.replace_sku_token_set(
        conn,
        table_name="sku_ship_to_locations",
        sku_id=int(sku_id),
        column_name="destination_code",
        values=["USLAX_DC01", "USLGB_DC02"],
    )

    rows = conn.execute(
        "SELECT destination_code FROM sku_ship_to_locations WHERE sku_id = ? ORDER BY destination_code",
        (sku_id,),
    ).fetchall()

    assert inserted == 2
    assert [r["destination_code"] for r in rows] == ["USLAX_DC01", "USLGB_DC02"]
