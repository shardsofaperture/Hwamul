"""Batch planning for container no-mix and truck mixed loading.

Mixed-truck planning uses a First-Fit Decreasing (FFD) heuristic, a standard
approximation for NP-hard bin packing variants.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from math import ceil
from typing import Any

from fit_engine import equipment_capacity, pack_gross_kg, pack_volume_m3, packs_per_equipment, required_packs_for_kg


POLICY_NO_MIX = "NO_MIX"
POLICY_MIX_OK = "MIX_OK"


@dataclass
class TruckBin:
    truck_id: int
    max_payload_kg: float
    max_volume_m3: float
    max_floor_m2: float
    remaining_payload_kg: float
    remaining_volume_m3: float
    remaining_floor_m2: float
    used_weight_kg: float = 0.0
    used_volume_m3: float = 0.0
    contents: dict[int, int] = field(default_factory=dict)

    def can_fit(self, item: dict[str, Any], *, use_floor_area: bool) -> bool:
        if self.remaining_payload_kg + 1e-9 < float(item["weight_kg"]):
            return False
        if self.remaining_volume_m3 + 1e-9 < float(item["volume_m3"]):
            return False
        if use_floor_area and self.remaining_floor_m2 + 1e-9 < float(item.get("floor_m2", 0.0)):
            return False
        return True

    def add(self, item: dict[str, Any], *, use_floor_area: bool) -> None:
        self.remaining_payload_kg -= float(item["weight_kg"])
        self.remaining_volume_m3 -= float(item["volume_m3"])
        self.used_weight_kg += float(item["weight_kg"])
        self.used_volume_m3 += float(item["volume_m3"])
        if use_floor_area:
            self.remaining_floor_m2 -= float(item.get("floor_m2", 0.0))
        self.contents[int(item["sku_id"])] = self.contents.get(int(item["sku_id"]), 0) + 1


def plan_containers_no_mix(
    requirements: list[dict[str, Any]],
    container_equipment: dict[str, Any],
) -> dict[str, Any]:
    per_sku: list[dict[str, Any]] = []
    total_containers = 0
    for req in requirements:
        sku_id = int(req["sku_id"])
        pack_rule = req["pack_rule"]
        qty = required_packs_for_kg(float(req["required_kg"]), pack_rule)
        fit = packs_per_equipment(pack_rule, container_equipment)
        packs_fit = int(fit["packs_fit"])
        if packs_fit <= 0:
            raise ValueError(f"SKU {sku_id} cannot fit selected container/equipment")
        containers_needed = ceil(int(qty["packs_required"]) / packs_fit)
        caps = equipment_capacity(container_equipment)
        shipped_packs = int(qty["packs_required"])
        used_weight = shipped_packs * pack_gross_kg(pack_rule)
        used_volume = shipped_packs * pack_volume_m3(pack_rule)
        total_weight_cap = containers_needed * float(caps["max_payload_kg"])
        total_cube_cap = containers_needed * float(caps["eq_volume_m3"])
        per_sku.append(
            {
                "sku_id": sku_id,
                "part_number": req.get("part_number", ""),
                "required_kg": float(req["required_kg"]),
                **qty,
                "equipment_code": container_equipment.get("equipment_code"),
                "packs_fit": packs_fit,
                "containers_needed": containers_needed,
                "limiting_constraint": fit["limiting_constraint"],
                "cube_util": (used_volume / total_cube_cap) if total_cube_cap > 0 else 0.0,
                "weight_util": (used_weight / total_weight_cap) if total_weight_cap > 0 else 0.0,
            }
        )
        total_containers += containers_needed
    return {"per_sku": per_sku, "total_conveyance_count": total_containers, "policy": POLICY_NO_MIX}


def _truck_layers(pack_rule: dict[str, Any], eq_h: float, allow_stacking_in_trucks: bool) -> int:
    if not allow_stacking_in_trucks:
        return 1
    if not bool(pack_rule.get("stackable", 1)):
        return 1
    pack_h = float(pack_rule.get("dim_h_m") or 0.0)
    if pack_h <= 0:
        return 1
    layers = int(eq_h // pack_h)
    max_stack = pack_rule.get("max_stack")
    if max_stack not in (None, ""):
        layers = min(layers, int(max_stack))
    return max(1, layers)


def plan_trucks_mix_ok(
    requirements: list[dict[str, Any]],
    truck_equipment: dict[str, Any],
    *,
    allow_stacking_in_trucks: bool = False,
    use_floor_area: bool = True,
) -> dict[str, Any]:
    caps = equipment_capacity(truck_equipment)
    eq_l = float(caps["internal_length_m"])
    eq_w = float(caps["internal_width_m"])
    eq_h = float(caps["internal_height_m"])
    truck_payload = float(caps["max_payload_kg"])
    truck_volume = float(caps["eq_volume_m3"])
    truck_floor = eq_l * eq_w

    items: list[dict[str, Any]] = []
    per_sku_conversion: list[dict[str, Any]] = []
    no_mix_baseline = 0

    for req in requirements:
        sku_id = int(req["sku_id"])
        pack_rule = req["pack_rule"]
        qty = required_packs_for_kg(float(req["required_kg"]), pack_rule)
        packs_required = int(qty["packs_required"])
        per_sku_conversion.append(
            {
                "sku_id": sku_id,
                "part_number": req.get("part_number", ""),
                "required_kg": float(req["required_kg"]),
                **qty,
            }
        )

        fit_alone = packs_per_equipment(pack_rule, truck_equipment)
        fit_alone_packs = int(fit_alone["packs_fit"])
        if fit_alone_packs <= 0:
            raise ValueError(f"SKU {sku_id} cannot fit selected truck equipment")
        no_mix_baseline += ceil(packs_required / fit_alone_packs)

        layers = _truck_layers(pack_rule, eq_h, allow_stacking_in_trucks)
        volume = pack_volume_m3(pack_rule)
        weight = pack_gross_kg(pack_rule)
        floor_area = (float(pack_rule.get("dim_l_m") or 0.0) * float(pack_rule.get("dim_w_m") or 0.0)) / layers
        for _ in range(packs_required):
            items.append(
                {
                    "sku_id": sku_id,
                    "volume_m3": volume,
                    "weight_kg": weight,
                    "floor_m2": floor_area,
                }
            )

    items.sort(key=lambda x: float(x["volume_m3"]), reverse=True)

    trucks: list[TruckBin] = []
    for item in items:
        placed = False
        for truck in trucks:
            if truck.can_fit(item, use_floor_area=use_floor_area):
                truck.add(item, use_floor_area=use_floor_area)
                placed = True
                break
        if placed:
            continue
        new_bin = TruckBin(
            truck_id=len(trucks) + 1,
            max_payload_kg=truck_payload,
            max_volume_m3=truck_volume,
            max_floor_m2=truck_floor,
            remaining_payload_kg=truck_payload,
            remaining_volume_m3=truck_volume,
            remaining_floor_m2=truck_floor,
        )
        if not new_bin.can_fit(item, use_floor_area=use_floor_area):
            raise ValueError(f"Item too large to fit empty truck for sku_id={item['sku_id']}")
        new_bin.add(item, use_floor_area=use_floor_area)
        trucks.append(new_bin)

    total_weight = sum(t.used_weight_kg for t in trucks)
    total_volume = sum(t.used_volume_m3 for t in trucks)
    truck_count = len(trucks)
    weight_util = total_weight / (truck_count * truck_payload) if truck_count > 0 and truck_payload > 0 else 0.0
    volume_util = total_volume / (truck_count * truck_volume) if truck_count > 0 and truck_volume > 0 else 0.0

    per_truck_rows = []
    for t in trucks:
        per_truck_rows.append(
            {
                "truck_id": t.truck_id,
                "sku_breakdown": dict(sorted(t.contents.items())),
                "total_weight": t.used_weight_kg,
                "total_volume": t.used_volume_m3,
            }
        )

    residuals = defaultdict(int)
    return {
        "policy": POLICY_MIX_OK,
        "per_sku_conversion": per_sku_conversion,
        "truck_count": truck_count,
        "weight_util": weight_util,
        "volume_util": volume_util,
        "trucks": per_truck_rows,
        "no_mix_baseline_truck_count": no_mix_baseline,
        "residuals": dict(residuals),
    }


def plan_trucks_no_mix(requirements: list[dict[str, Any]], truck_equipment: dict[str, Any]) -> dict[str, Any]:
    truck_rows: list[dict[str, Any]] = []
    truck_id = 1
    conversions: list[dict[str, Any]] = []
    caps = equipment_capacity(truck_equipment)
    payload = float(caps["max_payload_kg"])
    volume = float(caps["eq_volume_m3"])
    total_weight = 0.0
    total_volume = 0.0
    for req in requirements:
        qty = required_packs_for_kg(float(req["required_kg"]), req["pack_rule"])
        conversions.append({
            "sku_id": int(req["sku_id"]),
            "part_number": req.get("part_number", ""),
            "required_kg": float(req["required_kg"]),
            **qty,
        })
        fit = packs_per_equipment(req["pack_rule"], truck_equipment)
        packs_fit = int(fit["packs_fit"])
        if packs_fit <= 0:
            raise ValueError(f"SKU {req['sku_id']} cannot fit selected truck equipment")
        remaining = int(qty["packs_required"])
        pweight = pack_gross_kg(req["pack_rule"])
        pvol = pack_volume_m3(req["pack_rule"])
        while remaining > 0:
            take = min(remaining, packs_fit)
            w = take * pweight
            v = take * pvol
            total_weight += w
            total_volume += v
            truck_rows.append({
                "truck_id": truck_id,
                "sku_breakdown": {int(req["sku_id"]): take},
                "total_weight": w,
                "total_volume": v,
            })
            truck_id += 1
            remaining -= take
    count = len(truck_rows)
    return {
        "policy": POLICY_NO_MIX,
        "per_sku_conversion": conversions,
        "truck_count": count,
        "weight_util": total_weight / (count * payload) if count and payload > 0 else 0.0,
        "volume_util": total_volume / (count * volume) if count and volume > 0 else 0.0,
        "trucks": truck_rows,
        "no_mix_baseline_truck_count": count,
        "residuals": {},
    }
