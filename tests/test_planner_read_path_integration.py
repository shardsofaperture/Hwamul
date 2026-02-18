from __future__ import annotations

import importlib
from pathlib import Path

import pandas as pd

from bom_planner import create_bom_run, generate_pack_plan, validate_bom_frame
from planning_engine import plan_quick_run
from services.master_data_import import apply_pack_master_import


def _load_db_module(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PLANNER_DB_PATH", str(tmp_path / "planner.db"))
    import db  # noqa: F401

    return importlib.reload(db)


def test_template_v2_import_then_bom_then_quick_plan_without_manual_lane(tmp_path: Path, monkeypatch) -> None:
    db = _load_db_module(tmp_path, monkeypatch)
    db.run_migrations()
    conn = db.get_conn()

    template_df = pd.read_csv(Path("tests/fixtures/pack_mdm_template_v2.csv"))
    sample_bom = pd.read_csv(Path("tests/fixtures/sample_bom_v2.csv"))

    apply_pack_master_import(conn, template_df)

    mapped, errors, warnings = validate_bom_frame(conn, sample_bom)
    assert errors == []
    assert warnings == []

    run_id = create_bom_run(conn, "v2-integration", mapped)
    pack_plan = generate_pack_plan(conn, run_id)
    assert not pack_plan.empty

    sku_id = int(mapped.iloc[0]["sku_id"])
    result = plan_quick_run(
        conn=conn,
        sku_id=sku_id,
        required_units=10,
        need_date="2026-03-17",
        coo_override=None,
        pack_rule_id=None,
        lane_origin_code=None,
        lane_dest_code=None,
        service_scope="P2P",
        modes=None,
    )

    routing = result["routing_context"]
    assert routing["selected_origin_code"] == "CNSHA"
    assert routing["selected_dest_code"] == "USLAX_DC01"
    assert routing["allowed_modes"] == ["OCEAN", "TRUCK"]
    assert routing["incoterm"] == "FOB"
    assert routing["incoterm_named_place"] == "SHANGHAI PORT"
    assert result["equipment"]
