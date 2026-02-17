import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from acceptance_pipeline import run_acceptance_pipeline


def test_acceptance_scenario_outputs_exist_and_are_populated():
    result = run_acceptance_pipeline()

    phase_summary = result["phase_cost_summary"]
    shipment_plan = result["shipment_plan"]
    itemized = result["itemized_rate_breakdown"]
    customs = result["customs_report"]

    assert phase_summary
    assert {row["phase"] for row in phase_summary} == {
        "trial1", "trial2", "sample1", "sample2", "speedup", "validation", "SOP"
    }

    assert shipment_plan
    phase_modes = {row["phase"]: row["default_mode"] for row in shipment_plan}
    assert phase_modes["trial1"] == "AIR"
    assert phase_modes["trial2"] == "AIR"
    assert phase_modes["SOP"] == "OCEAN"
    sample = shipment_plan[0]
    assert "default_mode" in sample
    assert "intl_scope" in sample
    assert "intl_equipment_count" in sample
    assert "truck_equipment_count" in sample

    assert itemized
    assert any(row["item_type"] == "BASE" for row in itemized)
    assert any(row["item_type"] == "ACCESSORIAL" for row in itemized)

    assert customs
    customs_sample = customs[0]
    for field in ["coo", "hts_code", "entered_value_usd", "qty", "uom", "origin_port", "entry_port", "seller", "consignee"]:
        assert field in customs_sample
