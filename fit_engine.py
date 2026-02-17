"""Cube/payload fit utilities for quick planning."""
from __future__ import annotations

from math import ceil, floor
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


def required_packs_for_kg(required_kg: float, pack_rule: Any) -> dict:
    """Convert required kilograms into rounded packs using pack rules.

    If kg_per_unit is missing/non-positive we fall back to treating required_kg as
    units so users can still run a plan with explicit labeling.
    """
    required_kg = float(required_kg or 0.0)
    units_per_pack = float(_value(pack_rule, "units_per_pack", 0.0) or 0.0)
    kg_per_unit = float(_value(pack_rule, "kg_per_unit", 0.0) or 0.0)
    if units_per_pack <= 0:
        raise ValueError("units_per_pack must be greater than 0")

    kg_as_units_mode = kg_per_unit <= 0
    required_units = required_kg if kg_as_units_mode else (required_kg / kg_per_unit)
    packs = rounded_order_packs(required_units, pack_rule)
    shipped_units = packs * units_per_pack
    shipped_kg = shipped_units if kg_as_units_mode else (shipped_units * kg_per_unit)
    excess_kg = max(0.0, shipped_kg - required_kg)

    return {
        "required_kg": required_kg,
        "required_units": required_units,
        "packs_required": packs,
        "shipped_units": shipped_units,
        "shipped_kg": shipped_kg,
        "excess_kg": excess_kg,
        "kg_as_units_mode": kg_as_units_mode,
    }


def _required_positive(value: float, field_name: str, equipment_name: str | None = None) -> float:
    if value <= 0:
        target = f" for equipment '{equipment_name}'" if equipment_name else ""
        raise ValueError(f"Missing or invalid {field_name}{target}: must be > 0")
    return value


def equipment_capacity(equipment_preset: Any) -> dict:
    equipment_name = str(_value(equipment_preset, "name", "")).strip() or None
    length_m = float(_value(equipment_preset, "internal_length_m", _value(equipment_preset, "length_m", 0.0)) or 0.0)
    width_m = float(_value(equipment_preset, "internal_width_m", _value(equipment_preset, "width_m", 0.0)) or 0.0)
    height_m = float(_value(equipment_preset, "internal_height_m", _value(equipment_preset, "height_m", 0.0)) or 0.0)
    max_payload_kg = float(_value(equipment_preset, "max_payload_kg", 0.0) or 0.0)

    length_m = _required_positive(length_m, "internal_length_m", equipment_name)
    width_m = _required_positive(width_m, "internal_width_m", equipment_name)
    height_m = _required_positive(height_m, "internal_height_m", equipment_name)
    max_payload_kg = _required_positive(max_payload_kg, "max_payload_kg", equipment_name)

    eq_volume_m3 = length_m * width_m * height_m
    return {
        "eq_volume_m3": eq_volume_m3,
        "max_payload_kg": max_payload_kg,
        "internal_length_m": length_m,
        "internal_width_m": width_m,
        "internal_height_m": height_m,
    }


def packs_per_layer(pack_dims: tuple[float, float], equipment_dims: tuple[float, float]) -> int:
    pack_l, pack_w = pack_dims
    eq_l, eq_w = equipment_dims
    if min(pack_l, pack_w, eq_l, eq_w) <= 0:
        raise ValueError("Pack and equipment footprint dimensions must be > 0")
    best = max(
        floor(eq_l / pack_l) * floor(eq_w / pack_w),
        floor(eq_l / pack_w) * floor(eq_w / pack_l),
    )
    return max(0, int(best))


def layers_allowed(pack_h: float, eq_h: float, stackable: bool, max_stack: int | None) -> int:
    if pack_h <= 0 or eq_h <= 0:
        raise ValueError("Pack and equipment heights must be > 0")
    if not stackable:
        return 1
    layers = floor(eq_h / pack_h)
    if max_stack is not None:
        layers = min(layers, int(max_stack))
    return max(1, int(layers))


def packs_per_equipment(pack_rule: Any, equipment: Any) -> dict:
    caps = equipment_capacity(equipment)
    eq_l = float(caps["internal_length_m"])
    eq_w = float(caps["internal_width_m"])
    eq_h = float(caps["internal_height_m"])
    max_payload_kg = float(caps["max_payload_kg"])

    pack_l = float(_value(pack_rule, "dim_l_m", 0.0) or 0.0)
    pack_w = float(_value(pack_rule, "dim_w_m", 0.0) or 0.0)
    pack_h = float(_value(pack_rule, "dim_h_m", 0.0) or 0.0)
    _required_positive(pack_l, "dim_l_m")
    _required_positive(pack_w, "dim_w_m")
    _required_positive(pack_h, "dim_h_m")

    pgross = pack_gross_kg(pack_rule)
    _required_positive(pgross, "pack_gross_kg")

    per_layer = packs_per_layer((pack_l, pack_w), (eq_l, eq_w))
    max_stack = _value(pack_rule, "max_stack", None)
    layers = layers_allowed(pack_h, eq_h, bool(_value(pack_rule, "stackable", 1)), max_stack if max_stack not in ("", None) else None)

    by_grid = int(per_layer * layers)
    by_weight = floor(max_payload_kg / pgross)
    packs_fit = max(0, int(min(by_grid, by_weight)))
    limiting_constraint = "FLOOR_OR_HEIGHT" if by_grid < by_weight else "PAYLOAD"
    return {
        "packs_per_layer": per_layer,
        "layers_allowed": layers,
        "by_grid": by_grid,
        "by_weight": by_weight,
        "packs_fit": packs_fit,
        "limiting_constraint": limiting_constraint,
    }



def packs_fit(pack_rule: Any, equipment: Any, stacking_policy: bool = True) -> dict:
    """Compatibility wrapper for BOM planner fit semantics."""
    fit = packs_per_equipment(pack_rule, equipment)
    if stacking_policy:
        return fit
    fit = dict(fit)
    per_layer = int(fit["packs_per_layer"])
    by_weight = int(fit["by_weight"])
    fit["layers_allowed"] = 1
    fit["by_grid"] = per_layer
    fit["packs_fit"] = max(0, min(per_layer, by_weight))
    fit["limiting_constraint"] = "FLOOR_OR_HEIGHT" if per_layer < by_weight else "PAYLOAD"
    return fit

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
