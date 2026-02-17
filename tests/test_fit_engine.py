from fit_engine import (
    equipment_count_for_packs,
    pack_gross_kg,
    pack_volume_m3,
    packs_per_equipment,
    required_shipped_units,
)
from planning_engine import norm_mode


def test_pack_rounding_required_shipped_units():
    pack_rule = {
        "units_per_pack": 6,
        "kg_per_unit": 1,
        "pack_tare_kg": 0,
        "dim_l_m": 1,
        "dim_w_m": 1,
        "dim_h_m": 1,
        "min_order_packs": 1,
        "increment_packs": 1,
    }
    result = required_shipped_units(39, pack_rule)
    assert result["packs"] == 7
    assert result["shipped_units"] == 42
    assert result["excess_units"] == 3


def test_equipment_fit_by_cube_and_payload():
    pack_rule = {
        "units_per_pack": 1,
        "kg_per_unit": 200,
        "pack_tare_kg": 0,
        "dim_l_m": 1,
        "dim_w_m": 1,
        "dim_h_m": 1,
    }
    equipment = {
        "length_m": 10,
        "width_m": 1,
        "height_m": 1,
        "max_payload_kg": 1000,
    }
    fit = packs_per_equipment(pack_rule, equipment)
    assert pack_volume_m3(pack_rule) == 1
    assert pack_gross_kg(pack_rule) == 200
    assert fit["by_cube"] == 10
    assert fit["by_weight"] == 5
    assert fit["packs_fit"] == 5
    assert equipment_count_for_packs(7, fit["packs_fit"]) == 2


def test_mode_normalization():
    assert norm_mode("Air") == norm_mode("AIR") == "AIR"
