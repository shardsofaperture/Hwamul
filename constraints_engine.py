"""Constraint engine for realistic conveyance capacity planning."""
from __future__ import annotations

from math import floor
from typing import Any

from fit_engine import _value, equipment_capacity, pack_gross_kg, pack_volume_m3, packs_per_layer, layers_allowed


FLOOR_GRID = "FLOOR_GRID"
CONTAINER_PAYLOAD = "CONTAINER_PAYLOAD"
CONTAINER_MGW = "CONTAINER_MGW"
DRAY_LEGAL_PAYLOAD = "DRAY_LEGAL_PAYLOAD"
AXLE_GROUP_LIMIT = "AXLE_GROUP_LIMIT"
BRIDGE_FORMULA = "BRIDGE_FORMULA"
GVW_LIMIT = "GVW_LIMIT"
ULD_MAX_GROSS = "ULD_MAX_GROSS"
AIR_CHARGEABLE_WEIGHT = "AIR_CHARGEABLE_WEIGHT"
RAIL_GROSS_LIMIT = "RAIL_GROSS_LIMIT"


LB_PER_KG = 2.2046226218


def _require_positive(value: float, field_name: str) -> float:
    if value <= 0:
        raise ValueError(f"Missing or invalid {field_name}: must be > 0")
    return value


def bridge_formula_max_gvw(axle_count: int, axle_group_span_ft: float) -> float:
    """Federal bridge formula cap in lb.

    Per 23 CFR 658.17: W = 500 * ((L * N / (N - 1)) + 12N + 36),
    where W is allowable weight in pounds, L is distance in feet between outer axles,
    and N is number of axles in the group. See FHWA bridge formula summary:
    https://ops.fhwa.dot.gov/freight/policy/rpt_congress/truck_sw_laws/app_a.htm
    """
    if axle_count < 2:
        return 0.0
    if axle_group_span_ft <= 0:
        return 0.0
    n = float(axle_count)
    l = float(axle_group_span_ft)
    return float(500.0 * (((l * n) / (n - 1.0)) + (12.0 * n) + 36.0))


def _group_limit_lb(axles: int, single_lb: float, tandem_lb: float) -> float:
    if axles <= 1:
        return single_lb
    if axles == 2:
        return tandem_lb
    # Estimated extension for tridem+ groups until explicit charts are modeled.
    return tandem_lb * (axles / 2.0)


def compute_truck_legal_payload_lb(
    truck_config: dict[str, Any],
    jurisdiction_rule: dict[str, Any],
    cargo_weight_distribution_model: dict[str, float] | None = None,
) -> dict[str, Any]:
    steer_axles = int(_value(truck_config, "steer_axles", 1) or 1)
    drive_axles = int(_value(truck_config, "drive_axles", 2) or 2)
    trailer_axles = int(_value(truck_config, "trailer_axles", 2) or 2)
    axle_count = steer_axles + drive_axles + trailer_axles

    span_ft = float(_value(truck_config, "axle_span_ft", 51.0) or 51.0)
    bridge_limit_lb = bridge_formula_max_gvw(axle_count, span_ft)

    truck_max_gvw_lb = float(_value(truck_config, "max_gvw_lb", 80000.0) or 80000.0)
    rule_max_gvw_lb = float(_value(jurisdiction_rule, "max_gvw_lb", 80000.0) or 80000.0)
    max_single_axle_lb = float(_value(jurisdiction_rule, "max_single_axle_lb", 20000.0) or 20000.0)
    max_tandem_lb = float(_value(jurisdiction_rule, "max_tandem_lb", 34000.0) or 34000.0)

    tractor_tare = float(_value(truck_config, "tractor_tare_lb", 18000.0) or 18000.0)
    trailer_tare = float(_value(truck_config, "trailer_tare_lb", 8000.0) or 8000.0)
    container_tare = float(_value(truck_config, "container_tare_lb", 0.0) or 0.0)
    tare_lb = tractor_tare + trailer_tare + container_tare

    model = cargo_weight_distribution_model or {}
    steer_share = float(model.get("steer_pct", _value(truck_config, "steer_weight_share_pct", 0.12)) or 0.12)
    drive_share = float(model.get("drive_pct", _value(truck_config, "drive_weight_share_pct", 0.44)) or 0.44)
    trailer_share = float(model.get("trailer_pct", _value(truck_config, "trailer_weight_share_pct", 0.44)) or 0.44)
    total_share = max(1e-9, steer_share + drive_share + trailer_share)
    steer_share, drive_share, trailer_share = (
        steer_share / total_share,
        drive_share / total_share,
        trailer_share / total_share,
    )

    gross_cap_by_axle = min(
        _group_limit_lb(steer_axles, max_single_axle_lb, max_tandem_lb) / max(1e-9, steer_share),
        _group_limit_lb(drive_axles, max_single_axle_lb, max_tandem_lb) / max(1e-9, drive_share),
        _group_limit_lb(trailer_axles, max_single_axle_lb, max_tandem_lb) / max(1e-9, trailer_share),
    )

    gross_cap_gvw = min(truck_max_gvw_lb, rule_max_gvw_lb)
    gross_cap_bridge = min(gross_cap_gvw, bridge_limit_lb) if bridge_limit_lb > 0 else gross_cap_gvw
    legal_gross_lb = min(gross_cap_gvw, gross_cap_by_axle, gross_cap_bridge)
    legal_payload_lb = max(0.0, legal_gross_lb - tare_lb)

    payload_caps = {
        GVW_LIMIT: max(0.0, gross_cap_gvw - tare_lb),
        AXLE_GROUP_LIMIT: max(0.0, gross_cap_by_axle - tare_lb),
        BRIDGE_FORMULA: max(0.0, gross_cap_bridge - tare_lb),
    }
    limiting_constraint = min(payload_caps, key=payload_caps.get)

    return {
        "legal_payload_lb": legal_payload_lb,
        "limiting_constraint": limiting_constraint,
        "breakdown": [
            {"constraint": GVW_LIMIT, "max_payload_lb": payload_caps[GVW_LIMIT]},
            {"constraint": AXLE_GROUP_LIMIT, "max_payload_lb": payload_caps[AXLE_GROUP_LIMIT]},
            {"constraint": BRIDGE_FORMULA, "max_payload_lb": payload_caps[BRIDGE_FORMULA]},
        ],
        "assumptions": {
            "axle_count": axle_count,
            "axle_span_ft": span_ft,
            "weight_distribution": {
                "steer_pct": steer_share,
                "drive_pct": drive_share,
                "trailer_pct": trailer_share,
            },
            "tare_lb": tare_lb,
        },
    }


def max_units_per_conveyance(
    sku_id: int,
    pack_rule: Any,
    equipment: Any,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    del sku_id
    ctx = context or {}
    mode = str(_value(equipment, "mode", "")).upper().strip()
    caps = equipment_capacity(equipment)

    pack_l = _require_positive(float(_value(pack_rule, "dim_l_m", 0.0) or 0.0), "dim_l_m")
    pack_w = _require_positive(float(_value(pack_rule, "dim_w_m", 0.0) or 0.0), "dim_w_m")
    pack_h = _require_positive(float(_value(pack_rule, "dim_h_m", 0.0) or 0.0), "dim_h_m")
    pack_gross = _require_positive(float(pack_gross_kg(pack_rule)), "pack_gross_kg")

    eq_l = float(caps["internal_length_m"])
    eq_w = float(caps["internal_width_m"])
    eq_h = float(caps["internal_height_m"])
    max_payload_kg = float(caps["max_payload_kg"])

    per_layer = packs_per_layer((pack_l, pack_w), (eq_l, eq_w))
    max_stack = _value(pack_rule, "max_stack", None)
    stackable = bool(_value(pack_rule, "stackable", 1))
    layers = layers_allowed(pack_h, eq_h, stackable, max_stack if max_stack not in ("", None) else None)
    geometric_max_units = int(per_layer * layers)

    constraints: list[dict[str, Any]] = [
        {"constraint": FLOOR_GRID, "max_units": geometric_max_units, "details": {"packs_per_layer": per_layer, "layers_allowed": layers}},
        {"constraint": CONTAINER_PAYLOAD, "max_units": int(floor(max_payload_kg / pack_gross)), "details": {"max_payload_kg": max_payload_kg, "pack_gross_kg": pack_gross}},
    ]

    max_gross_kg = float(_value(equipment, "max_gross_kg", 0.0) or 0.0)
    tare_kg = float(_value(equipment, "tare_kg", 0.0) or 0.0)
    if max_gross_kg > 0:
        constraints.append(
            {
                "constraint": CONTAINER_MGW,
                "max_units": int(floor(max(0.0, max_gross_kg - tare_kg) / pack_gross)),
                "details": {"max_gross_kg": max_gross_kg, "tare_kg": tare_kg},
            }
        )

    if mode in {"TRUCK", "DRAY"} or bool(ctx.get("container_on_chassis")):
        truck_result = compute_truck_legal_payload_lb(
            dict(ctx.get("truck_config") or {}),
            dict(ctx.get("jurisdiction_rule") or {}),
            dict(ctx.get("cargo_weight_distribution_model") or {}),
        )
        legal_payload_kg = truck_result["legal_payload_lb"] / LB_PER_KG
        constraints.append(
            {
                "constraint": DRAY_LEGAL_PAYLOAD,
                "max_units": int(floor(legal_payload_kg / pack_gross)),
                "details": truck_result,
            }
        )

    if mode == "AIR":
        uld_max_gross_kg = float(ctx.get("air_uld_max_gross_kg") or _value(equipment, "max_gross_kg", 0.0) or 0.0)
        if uld_max_gross_kg > 0:
            constraints.append(
                {
                    "constraint": ULD_MAX_GROSS,
                    "max_units": int(floor(uld_max_gross_kg / pack_gross)),
                    "details": {"uld_max_gross_kg": uld_max_gross_kg},
                }
            )
        chargeable_limit_kg = float(ctx.get("air_chargeable_limit_kg") or 0.0)
        volumetric_factor = float(_value(equipment, "volumetric_factor", 167.0) or 167.0)
        chargeable_pack_weight = max(pack_gross, pack_volume_m3(pack_rule) * volumetric_factor)
        if chargeable_limit_kg > 0:
            constraints.append(
                {
                    "constraint": AIR_CHARGEABLE_WEIGHT,
                    "max_units": int(floor(chargeable_limit_kg / chargeable_pack_weight)),
                    "details": {
                        "chargeable_limit_kg": chargeable_limit_kg,
                        "chargeable_pack_weight_kg": chargeable_pack_weight,
                    },
                }
            )

    if mode == "RAIL":
        rail_max_gross_kg = float(ctx.get("rail_max_gross_kg") or 0.0)
        if rail_max_gross_kg > 0:
            constraints.append(
                {
                    "constraint": RAIL_GROSS_LIMIT,
                    "max_units": int(floor(rail_max_gross_kg / pack_gross)),
                    "details": {"rail_max_gross_kg": rail_max_gross_kg},
                }
            )

    positive_constraints = [{**c, "max_units": max(0, int(c["max_units"]))} for c in constraints]
    limiting = min(positive_constraints, key=lambda c: c["max_units"]) if positive_constraints else {"constraint": FLOOR_GRID, "max_units": 0}

    return {
        "max_units": int(limiting["max_units"]),
        "limiting_constraint": str(limiting["constraint"]),
        "breakdown": positive_constraints,
        "packs_per_layer": per_layer,
        "layers_allowed": layers,
        "notes": ["Estimated legal payload based on assumed axle distribution.", "SOLAS/VGM compliance remains operationally required for ocean exports."],
    }
