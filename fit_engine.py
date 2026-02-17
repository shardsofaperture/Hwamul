"""Cube/payload fit utilities for quick planning."""
from __future__ import annotations

from math import ceil, floor, inf
from typing import Any


def _value(obj: Any, key: str, default: float | int = 0) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    try:
        return obj[key]
    except Exception:
        return getattr(obj, key, default)


def pack_volume_m3(pack_rule_row_or_model: Any) -> float:
    return (
        float(_value(pack_rule_row_or_model, "dim_l_m", 0.0))
        * float(_value(pack_rule_row_or_model, "dim_w_m", 0.0))
        * float(_value(pack_rule_row_or_model, "dim_h_m", 0.0))
    )


def pack_gross_kg(pack_rule: Any) -> float:
    units_per_pack = float(_value(pack_rule, "units_per_pack", 0.0))
    kg_per_unit = float(_value(pack_rule, "kg_per_unit", 0.0))
    pack_tare_kg = float(_value(pack_rule, "pack_tare_kg", 0.0))
    return (units_per_pack * kg_per_unit) + pack_tare_kg


def rounded_order_packs(required_units: float, pack_rule: Any) -> int:
    units_per_pack = float(_value(pack_rule, "units_per_pack", 0.0))
    if units_per_pack <= 0:
        raise ValueError("units_per_pack must be greater than 0")

    packs = ceil(float(required_units) / units_per_pack)
    packs = max(packs, int(_value(pack_rule, "min_order_packs", 1) or 1))

    increment = int(_value(pack_rule, "increment_packs", 1) or 1)
    increment = max(1, increment)
    return ceil(packs / increment) * increment


def required_shipped_units(required_units: float, pack_rule: Any) -> dict:
    packs = rounded_order_packs(required_units, pack_rule)
    units_per_pack = float(_value(pack_rule, "units_per_pack", 0.0))
    shipped_units = packs * units_per_pack
    excess_units = max(0.0, shipped_units - float(required_units))
    return {
        "packs": packs,
        "shipped_units": shipped_units,
        "excess_units": excess_units,
    }


def equipment_capacity(equipment_preset: Any) -> dict:
    length_m = float(_value(equipment_preset, "length_m", 0.0) or 0.0)
    width_m = float(_value(equipment_preset, "width_m", 0.0) or 0.0)
    height_m = float(_value(equipment_preset, "height_m", 0.0) or 0.0)
    eq_volume_m3 = length_m * width_m * height_m if all(x > 0 for x in [length_m, width_m, height_m]) else 0.0
    max_payload_kg = float(_value(equipment_preset, "max_payload_kg", 0.0) or 0.0)
    return {"eq_volume_m3": eq_volume_m3, "max_payload_kg": max_payload_kg}


def packs_per_equipment(pack_rule: Any, equipment: Any) -> dict:
    caps = equipment_capacity(equipment)
    eq_volume_m3 = float(caps["eq_volume_m3"])
    max_payload_kg = float(caps["max_payload_kg"])
    pvol = pack_volume_m3(pack_rule)
    pgross = pack_gross_kg(pack_rule)

    by_cube = floor(eq_volume_m3 / pvol) if eq_volume_m3 > 0 and pvol > 0 else inf
    by_weight = floor(max_payload_kg / pgross) if max_payload_kg > 0 and pgross > 0 else inf
    packs_fit = max(0, int(min(by_cube, by_weight))) if min(by_cube, by_weight) != inf else 0
    return {"by_cube": by_cube, "by_weight": by_weight, "packs_fit": packs_fit}


def equipment_count_for_packs(packs_required: int, packs_fit: int) -> int:
    if packs_fit <= 0:
        return 0
    return ceil(int(packs_required) / int(packs_fit))


def utilization(
    packs_required: int,
    packs_fit: int,
    equipment_count: int,
    *,
    pack_volume: float = 0.0,
    pack_gross: float = 0.0,
    eq_volume: float = 0.0,
    max_payload: float = 0.0,
) -> dict:
    total_capacity_packs = int(equipment_count) * int(packs_fit)
    pack_util = (float(packs_required) / total_capacity_packs) if total_capacity_packs > 0 else 0.0

    used_volume = float(packs_required) * float(pack_volume)
    used_weight = float(packs_required) * float(pack_gross)
    cap_volume = float(equipment_count) * float(eq_volume)
    cap_weight = float(equipment_count) * float(max_payload)

    cube_util = (used_volume / cap_volume) if cap_volume > 0 else 0.0
    weight_util = (used_weight / cap_weight) if cap_weight > 0 else 0.0

    return {
        "pack_utilization": pack_util,
        "cube_util": cube_util,
        "weight_util": weight_util,
    }
