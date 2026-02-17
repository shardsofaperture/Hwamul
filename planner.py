"""Planning service functions for allocation, recommendations, and shipment builds."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta

from models import (
    Equipment,
    PackagingRule,
    chargeable_air_weight_kg,
    estimate_equipment_count,
    rounded_order_packs,
)


@dataclass
class TrancheResult:
    sku: str
    need_date: date
    tranche_name: str
    requested_units: float
    ordered_units: float
    excess_units: float
    packs: int



def allocate_tranches(demand_qty: float, rule: PackagingRule, tranches: list[tuple[str, str, float]]) -> list[TrancheResult]:
    """Allocate demand into tranches and carry excess to later tranches.

    Tranches format: (name, type, value), where type is 'percent' or 'absolute'.
    """
    remaining = demand_qty
    carry_excess = 0.0
    rows: list[TrancheResult] = []
    for name, alloc_type, value in tranches:
        if alloc_type == "percent":
            requested = max(0.0, remaining * value / 100)
        else:
            requested = min(remaining, max(0.0, value))
        requested = max(0.0, requested - carry_excess)
        packs = rounded_order_packs(requested, rule)
        ordered_units = packs * rule.units_per_pack
        excess = max(0.0, ordered_units - requested)
        carry_excess = excess
        remaining = max(0.0, remaining - requested)
        rows.append(
            TrancheResult(
                sku=rule.sku,
                need_date=date.today(),
                tranche_name=name,
                requested_units=requested,
                ordered_units=ordered_units,
                excess_units=excess,
                packs=packs,
            )
        )
    return rows


def lead_days_for(mode: str, coo: str, sku: str, lead_table: dict[tuple[str, str], int], sku_override: dict[tuple[str, str], int], manual_override: int | None = None) -> int:
    if manual_override is not None:
        return manual_override
    if (sku, mode) in sku_override:
        return sku_override[(sku, mode)]
    return lead_table.get((coo, mode), 999)


def recommend_modes(
    sku: str,
    coo: str,
    need_date: date,
    ordered_units: float,
    pack_rule: PackagingRule,
    equipment_by_mode: dict[str, list[Equipment]],
    rates: list[dict],
    lead_table: dict[tuple[str, str], int],
    sku_lead_override: dict[tuple[str, str], int],
    manual_lead_override: int | None = None,
):
    total_weight = (ordered_units / pack_rule.units_per_pack) * pack_rule.gross_pack_weight_kg
    total_volume = (ordered_units / pack_rule.units_per_pack) * pack_rule.pack_cube_m3

    recs = []
    for mode, equipments in equipment_by_mode.items():
        lead_days = lead_days_for(mode, coo, sku, lead_table, sku_lead_override, manual_lead_override)
        ship_by = need_date - timedelta(days=lead_days)
        feasible = lead_days < 900
        cost = 0.0
        eq_count = 0
        util = 0.0

        if mode == "Air":
            eq = equipments[0]
            chargeable = chargeable_air_weight_kg(total_weight, total_volume, eq.volumetric_factor or 167)
            rate = next((r for r in rates if r["mode"] == "Air" and r["pricing_model"] == "per_kg"), None)
            if rate:
                cost = max(rate["minimum_charge"] or 0, chargeable * rate["rate_value"]) + (rate["fixed_fee"] or 0)
            eq_count = 1
            util = min(1.0, total_weight / eq.max_payload_kg)
        else:
            eq = equipments[0]
            eq_count = estimate_equipment_count(total_volume, total_weight, eq)
            rate = next((r for r in rates if r["mode"] == mode and r["equipment_name"] == eq.name and r["pricing_model"] in {"per_container", "per_load"}), None)
            if rate:
                cost = eq_count * rate["rate_value"] + (rate["fixed_fee"] or 0) + (rate["surcharge"] or 0)
            util = min(1.0, max(total_volume / (eq_count * eq.volume_m3), total_weight / (eq_count * eq.max_payload_kg))) if eq_count else 0

        recs.append(
            {
                "mode": mode,
                "lead_days": lead_days,
                "ship_by": ship_by.isoformat(),
                "feasible": feasible,
                "estimated_cost": round(cost, 2),
                "equipment_count": eq_count,
                "utilization_pct": round(util * 100, 1),
            }
        )

    recs.sort(key=lambda r: (not r["feasible"], r["estimated_cost"]))
    return recs


def build_shipments(tranches: list[dict], equipment_map: dict[str, Equipment]):
    grouped = defaultdict(list)
    for row in tranches:
        grouped[row["mode"]].append(row)

    outputs = []
    for mode, rows in grouped.items():
        eq = equipment_map[mode]
        total_volume = sum(r["volume_m3"] for r in rows)
        total_weight = sum(r["weight_kg"] for r in rows)
        count = estimate_equipment_count(total_volume, total_weight, eq)
        utilization = max(
            total_volume / (count * eq.volume_m3) if eq.volume_m3 else 0,
            total_weight / (count * eq.max_payload_kg) if eq.max_payload_kg else 0,
        ) if count else 0
        outputs.append(
            {
                "mode": mode,
                "shipments": count,
                "utilization_pct": round(min(1.0, utilization) * 100, 1),
                "ship_by_date": min(r["ship_by"] for r in rows),
                "cost": round(sum(r["cost"] for r in rows), 2),
            }
        )
    return outputs
