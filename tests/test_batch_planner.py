from batch_planner import plan_containers_no_mix, plan_trucks_mix_ok
from fit_engine import packs_per_equipment, required_packs_for_kg


def test_kg_to_packs_rounding():
    pack_rule = {
        "units_per_pack": 6,
        "kg_per_unit": 200,
        "pack_tare_kg": 0,
        "dim_l_m": 1.0,
        "dim_w_m": 1.0,
        "dim_h_m": 1.0,
        "min_order_packs": 1,
        "increment_packs": 1,
        "stackable": 1,
    }
    result = required_packs_for_kg(20000, pack_rule)
    assert result["required_units"] == 100
    assert result["packs_required"] == 17
    assert result["shipped_units"] == 102
    assert result["excess_kg"] == 400


def test_non_stackable_not_forced_single_pack():
    pack_rule = {
        "units_per_pack": 1,
        "kg_per_unit": 1,
        "pack_tare_kg": 0,
        "dim_l_m": 1.0,
        "dim_w_m": 1.0,
        "dim_h_m": 1.0,
        "stackable": 0,
        "max_stack": None,
    }
    eq = {
        "internal_length_m": 6.0,
        "internal_width_m": 3.0,
        "internal_height_m": 4.0,
        "max_payload_kg": 10000,
    }
    fit = packs_per_equipment(pack_rule, eq)
    assert fit["layers_allowed"] == 1
    assert fit["packs_per_layer"] == 18
    assert fit["packs_fit"] == 18


def test_container_no_mix_independent_counts():
    reqs = [
        {"sku_id": 1, "required_kg": 1200, "pack_rule": {"units_per_pack": 10, "kg_per_unit": 10, "pack_tare_kg": 0, "dim_l_m": 1, "dim_w_m": 1, "dim_h_m": 1, "stackable": 1}},
        {"sku_id": 2, "required_kg": 1200, "pack_rule": {"units_per_pack": 10, "kg_per_unit": 10, "pack_tare_kg": 0, "dim_l_m": 1, "dim_w_m": 1, "dim_h_m": 1, "stackable": 1}},
    ]
    eq = {
        "equipment_code": "CNT_40_DRY_STD",
        "internal_length_m": 2.0,
        "internal_width_m": 2.0,
        "internal_height_m": 1.0,
        "max_payload_kg": 20000,
    }
    result = plan_containers_no_mix(reqs, eq)
    assert len(result["per_sku"]) == 2
    assert sum(r["containers_needed"] for r in result["per_sku"]) == result["total_conveyance_count"]


def test_truck_mix_ok_reduces_count_vs_no_mix_baseline():
    reqs = [
        {"sku_id": 1, "required_kg": 200, "pack_rule": {"units_per_pack": 1, "kg_per_unit": 10, "pack_tare_kg": 0, "dim_l_m": 1, "dim_w_m": 1, "dim_h_m": 1, "stackable": 1}},
        {"sku_id": 2, "required_kg": 200, "pack_rule": {"units_per_pack": 1, "kg_per_unit": 10, "pack_tare_kg": 0, "dim_l_m": 1, "dim_w_m": 1, "dim_h_m": 1, "stackable": 1}},
    ]
    truck = {
        "equipment_code": "TRL_53_STD",
        "internal_length_m": 10.0,
        "internal_width_m": 2.0,
        "internal_height_m": 3.0,
        "max_payload_kg": 10000,
    }
    result = plan_trucks_mix_ok(reqs, truck, allow_stacking_in_trucks=False)
    assert result["truck_count"] <= result["no_mix_baseline_truck_count"]
