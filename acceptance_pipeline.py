from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import Any

from models import Equipment, PackagingRule, chargeable_air_weight_kg, estimate_equipment_count, rounded_order_packs
from rate_engine import RateTestInput, compute_rate_total, select_best_rate_card


DATA_DIR = Path(__file__).resolve().parent / "scenario_data"
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs" / "acceptance"


def _read_csv(name: str) -> list[dict[str, str]]:
    with (DATA_DIR / name).open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(name: str, rows: list[dict[str, Any]]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / name
    if not rows:
        path.write_text("", encoding="utf-8")
        return path
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def run_acceptance_pipeline() -> dict[str, Any]:
    phases = {r["phase"]: r for r in _read_csv("phases.csv")}
    suppliers = {(r["supplier_code"]): r for r in _read_csv("suppliers.csv")}
    skus = {(r["part_number"], r["supplier_code"]): r for r in _read_csv("skus.csv")}
    pack_rules = {(r["part_number"], r["supplier_code"]): r for r in _read_csv("pack_rules.csv")}
    demand_rows = _read_csv("demand.csv")
    routing = {r["phase"]: r for r in _read_csv("routing.csv")}
    rate_cards = [
        {
            "id": int(r["id"]),
            "mode": r["mode"],
            "equipment": r["equipment"],
            "service_scope": r["service_scope"],
            "origin_type": r["origin_type"],
            "origin_code": r["origin_code"],
            "dest_type": r["dest_type"],
            "dest_code": r["dest_code"],
            "currency": r["currency"],
            "uom_pricing": r["uom_pricing"],
            "base_rate": float(r["base_rate"]),
            "min_charge": float(r["min_charge"]),
            "effective_from": r["effective_from"],
            "effective_to": r["effective_to"] or None,
            "is_active": int(r["is_active"]),
            "priority": int(r["priority"]),
        }
        for r in _read_csv("rate_cards.csv")
    ]
    rate_charges = [
        {
            "rate_card_id": int(r["rate_card_id"]),
            "charge_code": r["charge_code"],
            "charge_name": r["charge_name"],
            "calc_method": r["calc_method"],
            "amount": float(r["amount"]),
            "applies_when": r["applies_when"],
            "effective_from": None,
            "effective_to": None,
            "min_amount": None,
            "max_amount": None,
        }
        for r in _read_csv("rate_charges.csv")
    ]
    customs = {(r["part_number"], r["supplier_code"]): r for r in _read_csv("customs.csv")}
    equipment_rows = _read_csv("equipment.csv")
    eq_by_mode = {
        r["mode"]: Equipment(
            name=r["equipment"],
            mode=r["mode"],
            length_m=float(r["length_m"]),
            width_m=float(r["width_m"]),
            height_m=float(r["height_m"]),
            max_payload_kg=float(r["max_payload_kg"]),
            volumetric_factor=float(r["volumetric_factor"] or 167),
        )
        for r in equipment_rows
    }

    shipment_plan: list[dict[str, Any]] = []
    itemized: list[dict[str, Any]] = []
    customs_rows: list[dict[str, Any]] = []

    for idx, demand in enumerate(demand_rows, start=1):
        phase_name = demand["phase"]
        phase = phases[phase_name]
        route = routing[phase_name]
        sku_key = (demand["part_number"], demand["supplier_code"])
        sku = skus[sku_key]
        pack = pack_rules[sku_key]
        qty = float(demand["qty"])

        rule = PackagingRule(
            units_per_pack=float(pack["units_per_pack"]),
            kg_per_unit=float(pack["kg_per_unit"]),
            pack_tare_kg=float(pack["pack_tare_kg"]),
            dim_l_m=float(pack["dim_l_m"]),
            dim_w_m=float(pack["dim_w_m"]),
            dim_h_m=float(pack["dim_h_m"]),
            min_order_packs=int(pack["min_order_packs"]),
            increment_packs=int(pack["increment_packs"]),
            part_number=demand["part_number"],
        )
        packs = rounded_order_packs(qty, rule)
        gross_weight = packs * rule.gross_pack_weight_kg
        total_volume = packs * rule.pack_cube_m3

        default_mode = phase["default_mode"]
        intl_scope = phase["service_scope"]
        intl_equipment = eq_by_mode[default_mode]

        if default_mode == "AIR":
            chargeable_weight = chargeable_air_weight_kg(gross_weight, total_volume, intl_equipment.volumetric_factor or 167)
            intl_count = 1.0
        else:
            chargeable_weight = gross_weight
            intl_count = float(estimate_equipment_count(total_volume, gross_weight, intl_equipment))

        intl_shipment = RateTestInput(
            ship_date=date.fromisoformat(phase["need_date"]),
            mode=default_mode,
            equipment=intl_equipment.name,
            service_scope=intl_scope,
            origin_type="PORT" if intl_scope.startswith("P") else "CITY",
            origin_code=route["origin_port"] if intl_scope.startswith("P") else "CN_FACTORY",
            dest_type="PORT" if intl_scope.endswith("P") else "CITY",
            dest_code=route["dest_port"] if intl_scope.endswith("P") else route["plant"],
            weight_kg=gross_weight,
            volume_m3=total_volume,
            containers_count=intl_count,
            chargeable_weight_kg=chargeable_weight,
        )
        intl_card = select_best_rate_card(rate_cards, intl_shipment)
        if intl_card is None:
            raise AssertionError(f"No international rate card for {phase_name} {default_mode} {intl_scope}")
        intl_result = compute_rate_total(intl_card, rate_charges, intl_shipment)

        domestic_equipment = eq_by_mode["TRUCK"]
        truck_count = float(estimate_equipment_count(total_volume, gross_weight, domestic_equipment))
        dom_scope = route["domestic_scope"]
        dom_shipment = RateTestInput(
            ship_date=date.fromisoformat(phase["need_date"]),
            mode="TRUCK",
            equipment=domestic_equipment.name,
            service_scope=dom_scope,
            origin_type="PORT" if dom_scope.startswith("D") else "CITY",
            origin_code=route["dest_port"],
            dest_type="CITY",
            dest_code=route["plant"],
            weight_kg=gross_weight,
            volume_m3=total_volume,
            containers_count=truck_count,
        )
        dom_card = select_best_rate_card(rate_cards, dom_shipment)
        if dom_card is None:
            raise AssertionError(f"No domestic rate card for {phase_name}")
        dom_result = compute_rate_total(dom_card, rate_charges, dom_shipment)

        total_cost = intl_result["grand_total"] + dom_result["grand_total"]
        shipment_plan.append(
            {
                "shipment_id": idx,
                "phase": phase_name,
                "need_date": phase["need_date"],
                "part_number": demand["part_number"],
                "supplier_code": demand["supplier_code"],
                "default_mode": default_mode,
                "intl_scope": intl_scope,
                "domestic_scope": dom_scope,
                "intl_equipment_count": intl_count,
                "truck_equipment_count": truck_count,
                "weight_kg": round(gross_weight, 2),
                "volume_m3": round(total_volume, 3),
                "origin": route["origin_country"],
                "destination": route["dest_country"],
                "dest_port": route["dest_port"],
                "plant": route["plant"],
                "intl_cost_usd": intl_result["grand_total"],
                "domestic_cost_usd": dom_result["grand_total"],
                "total_cost_usd": round(total_cost, 2),
            }
        )

        for leg, card, result in (("INTERNATIONAL", intl_card, intl_result), ("DOMESTIC", dom_card, dom_result)):
            for item in result["items"]:
                itemized.append(
                    {
                        "shipment_id": idx,
                        "phase": phase_name,
                        "leg": leg,
                        "rate_card_id": card["id"],
                        "item_type": item["type"],
                        "charge_code": item["code"],
                        "charge_name": item["name"],
                        "amount_usd": item["amount"],
                    }
                )

        custom = customs[sku_key]
        customs_rows.append(
            {
                "shipment_id": idx,
                "phase": phase_name,
                "supplier_code": demand["supplier_code"],
                "part_number": demand["part_number"],
                "coo": custom["country_of_origin"],
                "hts_code": custom["hts_code"],
                "tariff_program": custom["tariff_program"],
                "qty": qty,
                "uom": sku["uom"],
                "entered_value_usd": round(qty * float(sku["unit_value_usd"]), 2),
                "gross_weight_kg": round(gross_weight, 2),
                "origin_port": route["origin_port"],
                "entry_port": route["dest_port"],
                "seller": suppliers[demand["supplier_code"]]["supplier_name"],
                "consignee": route["plant"],
                "section_232": custom["section_232"],
                "section_301": custom["section_301"],
                "special_documentation_required": custom["special_documentation_required"],
                "documentation_notes": custom["documentation_notes"],
            }
        )

    phase_summary: dict[str, dict[str, float]] = {}
    for row in shipment_plan:
        slot = phase_summary.setdefault(row["phase"], {"intl": 0.0, "dom": 0.0, "total": 0.0, "count": 0})
        slot["intl"] += float(row["intl_cost_usd"])
        slot["dom"] += float(row["domestic_cost_usd"])
        slot["total"] += float(row["total_cost_usd"])
        slot["count"] += 1

    phase_cost_summary = [
        {
            "phase": phase,
            "shipment_count": values["count"],
            "intl_cost_usd": round(values["intl"], 2),
            "domestic_cost_usd": round(values["dom"], 2),
            "total_cost_usd": round(values["total"], 2),
        }
        for phase, values in phase_summary.items()
    ]
    overall_total = round(sum(r["total_cost_usd"] for r in phase_cost_summary), 2)

    out = {
        "phase_cost_summary": phase_cost_summary,
        "shipment_plan": shipment_plan,
        "itemized_rate_breakdown": itemized,
        "customs_report": customs_rows,
        "overall_total_usd": overall_total,
    }

    out_paths = {
        "phase_cost_summary": _write_csv("phase_cost_summary.csv", phase_cost_summary),
        "shipment_plan": _write_csv("shipment_plan.csv", shipment_plan),
        "itemized_rate_breakdown": _write_csv("itemized_rate_breakdown.csv", itemized),
        "customs_report": _write_csv("customs_report.csv", customs_rows),
    }
    out["output_files"] = {k: str(v) for k, v in out_paths.items()}
    return out


if __name__ == "__main__":
    result = run_acceptance_pipeline()
    print("Acceptance pipeline complete")
    print(f"Overall total logistics cost (USD): {result['overall_total_usd']}")
    for name, path in result["output_files"].items():
        print(f"- {name}: {path}")
