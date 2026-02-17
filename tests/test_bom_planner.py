from __future__ import annotations

import pandas as pd

import db
from bom_planner import (
    BomPlanningPolicy,
    create_bom_run,
    generate_container_plan,
    generate_pack_plan,
    generate_truck_plan,
    validate_bom_frame,
)
from fit_engine import packs_per_equipment


def _seed(conn):
    conn.execute("INSERT INTO suppliers(supplier_code, supplier_name) VALUES ('SUP', 'Supplier')")
    conn.execute("INSERT INTO sku_master(part_number, supplier_id, description, default_coo) VALUES ('A', 2, 'a', 'CN')")
    conn.execute("INSERT INTO sku_master(part_number, supplier_id, description, default_coo) VALUES ('B', 2, 'b', 'CN')")
    conn.execute(
        """
        INSERT INTO packaging_rules(sku_id, pack_name, is_default, units_per_pack, kg_per_unit, pack_tare_kg, dim_l_m, dim_w_m, dim_h_m, min_order_packs, increment_packs, stackable)
        VALUES (1,'STD',1,10,2,5,1.0,1.0,1.0,1,1,0)
        """
    )
    conn.execute(
        """
        INSERT INTO packaging_rules(sku_id, pack_name, is_default, units_per_pack, kg_per_unit, pack_tare_kg, dim_l_m, dim_w_m, dim_h_m, min_order_packs, increment_packs, stackable)
        VALUES (2,'STD',1,10,2,5,1.0,1.0,1.0,1,1,0)
        """
    )


def _setup(tmp_path):
    db.DB_PATH = tmp_path / "planner.db"
    db.run_migrations()
    conn = db.get_conn()
    with conn:
        _seed(conn)
    return conn


def test_bom_import_and_pack_plan_keeps_phase_date_separate(tmp_path):
    conn = _setup(tmp_path)
    frame = pd.DataFrame([
        {"phase_name": "Sample Run 1", "need_date": "2026-01-10", "part_number": "A", "required_kg": 100},
        {"phase_name": "Trial Run 2", "need_date": "2026-01-20", "part_number": "A", "required_kg": 80},
        {"phase_name": "Trial Run 2", "need_date": "2026-01-20", "part_number": "B", "required_kg": 60},
    ])
    mapped, errors, _ = validate_bom_frame(conn, frame)
    assert not errors
    run_id = create_bom_run(conn, "t1", mapped)
    plan = generate_pack_plan(conn, run_id)
    assert len(plan.groupby(["phase_name", "need_date", "sku_id"])) == 3


def test_no_mix_container_rule_separates_skus(tmp_path):
    conn = _setup(tmp_path)
    frame = pd.DataFrame([
        {"phase_name": "SOP", "need_date": "2026-01-10", "part_number": "A", "required_kg": 300},
        {"phase_name": "SOP", "need_date": "2026-01-10", "part_number": "B", "required_kg": 300},
    ])
    mapped, _, _ = validate_bom_frame(conn, frame)
    run_id = create_bom_run(conn, "t2", mapped)
    generate_pack_plan(conn, run_id)
    out = generate_container_plan(conn, run_id, BomPlanningPolicy())
    assert out["sku_id"].nunique() == 2
    assert len(out) == 2


def test_pack_rounding_and_excess(tmp_path):
    conn = _setup(tmp_path)
    frame = pd.DataFrame([
        {"phase_name": "SOP", "need_date": "2026-01-10", "part_number": "A", "required_kg": 95},
    ])
    mapped, _, _ = validate_bom_frame(conn, frame)
    run_id = create_bom_run(conn, "t3", mapped)
    out = generate_pack_plan(conn, run_id)
    assert float(out.iloc[0]["shipped_kg"]) >= 95
    assert float(out.iloc[0]["excess_kg"]) > 0


def test_geometry_non_stackable_allows_multiple_floor_grid():
    pack = {"units_per_pack": 1, "kg_per_unit": 100, "pack_tare_kg": 0, "dim_l_m": 1.2, "dim_w_m": 1.0, "dim_h_m": 1.2, "stackable": 0}
    eq = {"internal_length_m": 12.0, "internal_width_m": 2.4, "internal_height_m": 2.6, "max_payload_kg": 50000}
    fit = packs_per_equipment(pack, eq)
    assert fit["layers_allowed"] == 1
    assert fit["packs_fit"] > 1


def test_truck_mix_ok_reduces_vs_naive_sum(tmp_path):
    conn = _setup(tmp_path)
    frame = pd.DataFrame([
        {"phase_name": "SOP", "need_date": "2026-01-10", "part_number": "A", "required_kg": 300},
        {"phase_name": "SOP", "need_date": "2026-01-10", "part_number": "B", "required_kg": 300},
    ])
    mapped, _, _ = validate_bom_frame(conn, frame)
    run_id = create_bom_run(conn, "t4", mapped)
    generate_pack_plan(conn, run_id)
    result = generate_truck_plan(conn, run_id, BomPlanningPolicy())
    assert int(result.iloc[0]["truck_count"]) >= 1
    naive = 0
    for sku_id in [1, 2]:
        packs = conn.execute("SELECT SUM(packs_required) FROM pack_plan_lines WHERE bom_run_id=? AND sku_id=?", (run_id, sku_id)).fetchone()[0]
        fit = 4  # 53 trailer with 1x1 floor packs ~= 4 by area in this seeded setup
        naive += (packs + fit - 1) // fit
    assert int(result.iloc[0]["truck_count"]) <= naive
