from datetime import date

from models import Equipment, PackagingRule
from planner import allocate_tranches, lead_days_for, norm_mode, recommend_modes


def test_percent_allocation_uses_original_demand_with_carry_rounding():
    rule = PackagingRule(6, 1, 0, 1, 1, 1, part_number="P1")
    rows = allocate_tranches(
        demand_qty=100,
        rule=rule,
        tranches=[("T1", "percent", 60), ("T2", "percent", 40)],
        need_date=date(2026, 1, 10),
        sku_id=1,
    )
    assert rows[0].requested_units == 60
    assert rows[1].requested_units == 40
    assert rows[0].shipped_units == 60
    assert rows[1].shipped_units == 42
    assert rows[1].excess_units == 2
    assert sum(r.requested_units for r in rows) == 100


def test_lead_override_uses_sku_id_and_normalized_mode():
    lead_table = {("CN", "OCEAN"): 45, ("CN", "AIR"): 7}
    override = {(1, "AIR"): 3}
    assert lead_days_for("air", "cn", 1, lead_table, override) == 3


def test_mode_normalization_equivalent_results():
    eq = {"AIR": [Equipment("AIR", "AIR", 1, 1, 1, 5000, 167)]}
    rule = PackagingRule(6, 1, 0.5, 0.2, 0.2, 0.2)
    kwargs = dict(
        sku_id=1,
        part_number="P",
        coo="CN",
        need_date=date(2026, 1, 15),
        requested_units=10,
        pack_rule=rule,
        equipment_by_mode=eq,
        rates=[{"mode": "AIR", "pricing_model": "per_kg", "rate_value": 1.0, "minimum_charge": 0, "fixed_fee": 0}],
        lead_table={("CN", "AIR"): 7},
        sku_lead_override={},
        rate_cards=[],
    )
    rec_upper = recommend_modes(mode_override="AIR", **kwargs)[0]
    rec_title = recommend_modes(mode_override="Air", **kwargs)[0]
    rec_lower = recommend_modes(mode_override="air", **kwargs)[0]
    assert norm_mode(rec_upper["mode"]) == "AIR"
    assert rec_upper["cost_total"] == rec_title["cost_total"] == rec_lower["cost_total"]


def test_recommend_uses_shipped_units_for_cube_and_weight():
    eq = {
        "OCEAN": [Equipment("40DV", "OCEAN", 12.03, 2.35, 2.39, 26000, None)],
    }
    rule = PackagingRule(6, 2, 3, 0.5, 0.4, 0.3)
    rec = recommend_modes(
        sku_id=1,
        part_number="P",
        coo="CN",
        need_date=date(2026, 1, 20),
        requested_units=39,
        pack_rule=rule,
        equipment_by_mode=eq,
        rates=[{"mode": "OCEAN", "equipment_name": "40DV", "pricing_model": "per_container", "rate_value": 1000, "fixed_fee": 0, "surcharge": 0}],
        lead_table={("CN", "OCEAN"): 30},
        sku_lead_override={},
    )[0]
    expected_weight = (42 / 6) * rule.gross_pack_weight_kg
    expected_volume = (42 / 6) * rule.pack_cube_m3
    assert rec["requested_units"] == 39
    assert rec["shipped_units"] == 42
    assert rec["excess_units"] == 3
    assert rec["weight_utilization"] == round(min(1.0, expected_weight / eq["OCEAN"][0].max_payload_kg), 4)
    assert rec["cube_utilization"] == round(min(1.0, expected_volume / eq["OCEAN"][0].volume_m3), 4)


def test_recommend_non_air_equipment_name_casing_and_spaces_are_normalized():
    eq = {
        "OCEAN": [Equipment("40dv", "OCEAN", 12.03, 2.35, 2.39, 26000, None)],
    }
    rule = PackagingRule(6, 2, 3, 0.5, 0.4, 0.3)
    kwargs = dict(
        sku_id=1,
        part_number="P",
        coo="CN",
        need_date=date(2026, 1, 20),
        requested_units=39,
        pack_rule=rule,
        equipment_by_mode=eq,
        lead_table={("CN", "OCEAN"): 30},
        sku_lead_override={},
    )

    rec_clean = recommend_modes(
        rates=[{"mode": "OCEAN", "equipment_name": "40DV", "pricing_model": "per_container", "rate_value": 1000, "fixed_fee": 5, "surcharge": 7}],
        **kwargs,
    )[0]
    rec_mixed = recommend_modes(
        rates=[{"mode": "OCEAN", "equipment_name": "  40dV  ", "pricing_model": "per_container", "rate_value": 1000, "fixed_fee": 5, "surcharge": 7}],
        **kwargs,
    )[0]

    assert rec_clean["mode"] == rec_mixed["mode"]
    assert rec_clean["equipment_count"] == rec_mixed["equipment_count"]
    assert rec_clean["cost_total"] == rec_mixed["cost_total"]
