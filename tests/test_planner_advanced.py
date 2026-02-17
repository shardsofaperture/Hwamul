from datetime import date

from planner import customs_report, phase_cost_rollup, recommend_modes
from models import Equipment, PackagingRule


def test_recommend_modes_uses_rate_cards_and_truck_legs():
    eq = {
        "AIR": [Equipment("AIR", "AIR", 1, 1, 1, 50000, 167)],
        "OCEAN": [Equipment("40DV", "OCEAN", 12.03, 2.35, 2.39, 26000, None)],
        "TRUCK": [Equipment("53FT", "TRUCK", 16, 2.5, 2.5, 20000, None)],
    }
    rule = PackagingRule(10, 5, 1, 1.0, 1.0, 1.0, part_number="P")
    cards = [
        {"id": 1, "is_active": 1, "mode": "OCEAN", "equipment": "40DV", "service_scope": "P2D", "origin_type": "PORT", "origin_code": "USLAX", "dest_type": "CITY", "dest_code": "PLANT1", "effective_from": "2025-01-01", "effective_to": None, "priority": 1, "base_rate": 1000, "uom_pricing": "PER_CONTAINER", "min_charge": None, "currency": "USD"},
        {"id": 2, "is_active": 1, "mode": "TRUCK", "equipment": "53FT", "service_scope": "D2D", "origin_type": "CITY", "origin_code": "USLAX", "dest_type": "CITY", "dest_code": "PLANT1", "effective_from": "2025-01-01", "effective_to": None, "priority": 1, "base_rate": 2, "uom_pricing": "PER_MILE", "min_charge": None, "currency": "USD"},
    ]
    recs = recommend_modes(
        sku_id=1,
        part_number="P",
        coo="CN",
        need_date=date(2026, 1, 10),
        requested_units=100,
        pack_rule=rule,
        equipment_by_mode=eq,
        rates=[],
        lead_table={("CN", "OCEAN"): 30, ("CN", "AIR"): 5, ("CN", "TRUCK"): 8},
        sku_lead_override={},
        rate_cards=cards,
        rate_charges=[],
        service_scope="P2D",
        route_info={"origin_port": "USLAX", "dest_port": "USLAX", "plant_code": "PLANT1"},
        miles=100,
        mode_override="OCEAN",
    )
    assert recs
    assert recs[0]["selected_rate_card_id"] == 1
    assert recs[0]["domestic_legs_cost"] == 200


def test_customs_report_duty_calc_and_phase_rollup():
    shipments = [{"phase": "Trial1", "sku_id": 1, "part_number": "P1", "qty": 10, "unit_price": 50, "weight_kg": 100, "mode": "AIR", "base_cost": 300, "domestic_legs_cost": 50, "arrival_date": "2026-01-02"}]
    skus = [{"sku_id": 1, "part_number": "P1", "default_coo": "CN", "hts_code": "7208.39.0015"}]
    rates = [{"hts_code": "7208.39.0015", "country_of_origin": "CN", "effective_from": "2025-01-01", "effective_to": None, "base_duty_rate": 2.5, "tariff_rate": 25, "section_232": 1, "section_301": 1}]
    customs = customs_report(shipments, skus, rates)
    assert customs[0]["duty_amount"] == 137.5
    roll = phase_cost_rollup(shipments, customs)
    assert roll[0]["phase"] == "Trial1"
    assert roll[0]["total_cost"] == 487.5
