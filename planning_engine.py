"""End-to-end quick planning workflow."""
from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from typing import Any

from constraints_engine import max_units_per_conveyance
from fit_engine import (
    equipment_capacity,
    equipment_count_for_packs,
    pack_gross_kg,
    pack_volume_m3,
    required_shipped_units,
    utilization,
)
from rate_engine import RateTestInput, compute_rate_total, select_best_rate_card


def norm_mode(mode: str | None) -> str:
    return (mode or "").strip().upper()


def norm_equipment_code(code: str | None) -> str:
    return (code or "").strip().upper()



def _legacy_rate_total(rate_row: dict[str, Any], *, equipment_count: int, shipped_units: float, shipped_weight_kg: float, shipped_volume_m3: float) -> float:
    model = str(rate_row.get("pricing_model") or "").lower().strip()
    rate_value = float(rate_row.get("rate_value") or 0.0)
    minimum_charge = rate_row.get("minimum_charge")
    fixed_fee = float(rate_row.get("fixed_fee") or 0.0)
    surcharge = float(rate_row.get("surcharge") or 0.0)

    if model == "per_container":
        total = rate_value * equipment_count
    elif model == "per_kg":
        total = rate_value * shipped_weight_kg
    elif model == "per_cbm":
        total = rate_value * shipped_volume_m3
    elif model == "per_unit":
        total = rate_value * shipped_units
    elif model == "per_mile":
        miles = float(rate_row.get("miles") or 0.0)
        total = rate_value * miles
    else:
        total = rate_value

    if minimum_charge is not None:
        total = max(total, float(minimum_charge))
    return round(total + fixed_fee + surcharge, 2)


def _carrier_name(conn: sqlite3.Connection, carrier_id: int | None) -> str | None:
    if not carrier_id:
        return None
    row = conn.execute("SELECT code, name FROM carrier WHERE id = ?", (carrier_id,)).fetchone()
    if not row:
        return None
    return row["code"] or row["name"]


def plan_quick_run(
    conn: sqlite3.Connection,
    sku_id: int,
    required_units: float,
    need_date: str,
    coo_override: str | None,
    pack_rule_id: int | None,
    lane_origin_code: str | None,
    lane_dest_code: str | None,
    service_scope: str | None,
    modes: list[str] | None,
    jurisdiction_code: str | None = None,
    truck_config_code: str | None = None,
) -> dict:
    sku_row = conn.execute(
        """
        SELECT sm.sku_id, sm.part_number, sm.description, sm.default_coo
        FROM sku_master sm
        WHERE sm.sku_id = ?
        """,
        (sku_id,),
    ).fetchone()
    if not sku_row:
        raise ValueError(f"SKU not found: {sku_id}")

    coo = (coo_override or sku_row["default_coo"] or "").strip().upper()

    if pack_rule_id:
        pack_rule_row = conn.execute(
            "SELECT * FROM packaging_rules WHERE id = ? AND sku_id = ?",
            (pack_rule_id, sku_id),
        ).fetchone()
    else:
        pack_rule_row = conn.execute(
            """
            SELECT * FROM packaging_rules
            WHERE sku_id = ?
            ORDER BY is_default DESC, id ASC
            LIMIT 1
            """,
            (sku_id,),
        ).fetchone()
    if not pack_rule_row:
        raise ValueError("No packaging rule found for selected SKU")

    pack_rule = dict(pack_rule_row)
    qty = required_shipped_units(float(required_units), pack_rule)
    packs_required = int(qty["packs"])
    shipped_units = float(qty["shipped_units"])
    excess_units = float(qty["excess_units"])

    pack_volume = pack_volume_m3(pack_rule)
    pack_gross = pack_gross_kg(pack_rule)
    shipped_weight = packs_required * pack_gross
    shipped_volume = packs_required * pack_volume

    requested_modes = {norm_mode(m) for m in (modes or []) if norm_mode(m)}
    eq_rows = conn.execute("SELECT * FROM equipment_presets WHERE active = 1 ORDER BY mode, equipment_code").fetchall()
    try:
        restriction_rows = conn.execute(
            "SELECT equipment_id, allowed FROM sku_equipment_rules WHERE sku_id = ?",
            (sku_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        restriction_rows = []
    restrictions = {int(r["equipment_id"]): int(r["allowed"]) for r in restriction_rows}

    equipment_results: list[dict[str, Any]] = []
    excluded_equipment: list[dict[str, Any]] = []
    mode_rollup: dict[str, dict[str, Any]] = {}
    rate_breakdown: dict[str, list[dict[str, Any]]] = {}
    need_dt = date.fromisoformat(need_date)

    cards = [dict(r) for r in conn.execute("SELECT * FROM rate_card WHERE is_active = 1").fetchall()]
    charges = [dict(r) for r in conn.execute("SELECT * FROM rate_charge").fetchall()]
    rates = [dict(r) for r in conn.execute("SELECT * FROM rates").fetchall()]

    selected_jurisdiction = (jurisdiction_code or "US_FED_INTERSTATE").strip().upper()
    selected_truck_config = (truck_config_code or "5AXLE_TL").strip().upper()

    jurisdiction_rule = conn.execute(
        "SELECT * FROM jurisdiction_weight_rules WHERE jurisdiction_code = ? AND active = 1",
        (selected_jurisdiction,),
    ).fetchone()
    if not jurisdiction_rule:
        jurisdiction_rule = conn.execute(
            "SELECT * FROM jurisdiction_weight_rules WHERE jurisdiction_code = 'US_FED_INTERSTATE' AND active = 1"
        ).fetchone()

    truck_config = conn.execute(
        "SELECT * FROM truck_configs WHERE truck_config_code = ? AND active = 1",
        (selected_truck_config,),
    ).fetchone()
    truck_warning = None
    if not truck_config:
        truck_warning = "Truck config missing; using conservative default 5AXLE_TL assumptions."
        truck_config = conn.execute(
            "SELECT * FROM truck_configs WHERE truck_config_code = '5AXLE_TL' AND active = 1"
        ).fetchone()

    for eq_row in eq_rows:
        eq = dict(eq_row)
        mode = norm_mode(eq.get("mode"))
        if requested_modes and mode not in requested_modes:
            continue

        eq_allowed = bool(restrictions.get(int(eq.get("id")), 1)) if restrictions else True
        if not eq_allowed:
            excluded_equipment.append(
                {
                    "mode": mode,
                    "equipment_name": eq.get("name"),
                    "equipment_code": eq.get("equipment_code"),
                    "reason": "Disallowed by SKU conveyance restrictions",
                }
            )
            continue

        try:
            fit = max_units_per_conveyance(
                sku_id=sku_id,
                pack_rule=pack_rule,
                equipment=eq,
                context={
                    "jurisdiction_code": selected_jurisdiction,
                    "truck_config_code": selected_truck_config,
                    "jurisdiction_rule": dict(jurisdiction_rule) if jurisdiction_rule else {},
                    "truck_config": dict(truck_config) if truck_config else {},
                    "container_on_chassis": mode in {"TRUCK", "DRAY"},
                },
            )
            packs_fit = int(fit["max_units"])
            caps = equipment_capacity(eq)
        except ValueError as exc:
            excluded_equipment.append(
                {
                    "mode": mode,
                    "equipment_name": eq.get("name"),
                    "equipment_code": eq.get("equipment_code"),
                    "reason": str(exc),
                }
            )
            continue

        equipment_count = equipment_count_for_packs(packs_required, packs_fit)

        util = utilization(
            packs_required,
            packs_fit,
            equipment_count,
            pack_volume=pack_volume,
            pack_gross=pack_gross,
            eq_volume=float(caps["eq_volume_m3"]),
            max_payload=float(caps["max_payload_kg"]),
        )

        est_cost = None
        carrier_best = None

        if lane_origin_code and lane_dest_code and cards:
            shipment = RateTestInput(
                ship_date=need_dt,
                mode=mode,
                equipment=norm_equipment_code(eq.get("equipment_code") or eq.get("name") or ""),
                service_scope=(service_scope or "P2P").upper(),
                origin_type="CITY",
                origin_code=lane_origin_code.upper(),
                dest_type="CITY",
                dest_code=lane_dest_code.upper(),
                containers_count=float(equipment_count),
                weight_kg=shipped_weight,
                volume_m3=shipped_volume,
                chargeable_weight_kg=shipped_weight,
            )
            candidate_cards = [
                c for c in cards
                if norm_mode(c.get("mode")) == mode
                and norm_equipment_code(c.get("equipment")) == norm_equipment_code(eq.get("equipment_code") or eq.get("name"))
                and str(c.get("service_scope") or "").strip().upper() == shipment.service_scope
            ]
            # try exact CITY/CITY first, then PORT/PORT fallback.
            best_result = None
            best_card = None
            for origin_type, dest_type in [("CITY", "CITY"), ("PORT", "PORT")]:
                shipment.origin_type = origin_type
                shipment.dest_type = dest_type
                card = select_best_rate_card(candidate_cards, shipment)
                if not card:
                    continue
                result = compute_rate_total(card, charges, shipment)
                if best_result is None or float(result["grand_total"]) < float(best_result["grand_total"]):
                    best_result = result
                    best_card = card
            if best_card and best_result:
                est_cost = float(best_result["grand_total"])
                carrier_best = _carrier_name(conn, best_card.get("carrier_id"))
                rate_breakdown.setdefault(mode, []).append(
                    {
                        "equipment_name": eq.get("name"),
                        "equipment_code": eq.get("equipment_code"),
                        "carrier": carrier_best,
                        "rate_card_id": best_card.get("id"),
                        "cost": est_cost,
                        "items": best_result.get("items", []),
                    }
                )

        if est_cost is None:
            matching_rates = [
                r for r in rates
                if norm_mode(r.get("mode")) == mode
                and (
                    (not r.get("equipment_name"))
                    or norm_equipment_code(r.get("equipment_name")) == norm_equipment_code(eq.get("equipment_code") or eq.get("name"))
                )
            ]
            if matching_rates:
                est_cost = min(
                    _legacy_rate_total(
                        r,
                        equipment_count=equipment_count,
                        shipped_units=shipped_units,
                        shipped_weight_kg=shipped_weight,
                        shipped_volume_m3=shipped_volume,
                    )
                    for r in matching_rates
                )

        equipment_results.append(
            {
                "mode": mode,
                "equipment_name": eq.get("name"),
                "equipment_code": eq.get("equipment_code"),
                "packs_per_layer": fit["packs_per_layer"],
                "layers_allowed": fit["layers_allowed"],
                "packs_fit": packs_fit,
                "limiting_constraint": fit.get("limiting_constraint"),
                "constraint_breakdown": fit.get("breakdown", []),
                "fit_diagnostics": {"engine": "constraints_engine", "api": "max_units_per_conveyance", "version": "1.0.0"},
                "equipment_count": equipment_count,
                "cube_util": util["cube_util"],
                "weight_util": util["weight_util"],
                "pack_utilization": util["pack_utilization"],
                "est_cost": est_cost,
                "carrier_best": carrier_best,
            }
        )

        lead_override = conn.execute(
            "SELECT lead_days FROM lead_time_overrides WHERE sku_id = ? AND UPPER(mode) = ?",
            (sku_id, mode),
        ).fetchone()
        lead_base = conn.execute(
            "SELECT lead_days FROM lead_times WHERE UPPER(country_of_origin) = ? AND UPPER(mode) = ?",
            (coo, mode),
        ).fetchone()
        lead_days = int(lead_override["lead_days"]) if lead_override else (int(lead_base["lead_days"]) if lead_base else None)
        ship_by_date = (need_dt - timedelta(days=lead_days)).isoformat() if lead_days is not None else None

        roll = mode_rollup.setdefault(
            mode,
            {
                "mode": mode,
                "lead_days": lead_days,
                "ship_by_date": ship_by_date,
                "cost_best": None,
                "carrier_best": None,
            },
        )
        if roll["lead_days"] is None and lead_days is not None:
            roll["lead_days"] = lead_days
            roll["ship_by_date"] = ship_by_date

        if est_cost is not None and (roll["cost_best"] is None or float(est_cost) < float(roll["cost_best"])):
            roll["cost_best"] = est_cost
            roll["carrier_best"] = carrier_best

    equipment_results.sort(key=lambda r: (r["mode"], r["equipment_count"], r.get("equipment_code") or "", r["equipment_name"] or ""))
    mode_summary = sorted(mode_rollup.values(), key=lambda r: r["mode"])

    return {
        "sku": {
            "sku_id": int(sku_row["sku_id"]),
            "part_number": sku_row["part_number"],
            "description": sku_row["description"],
            "coo": coo,
        },
        "pack_rule": pack_rule,
        "required_units": float(required_units),
        "packs_required": packs_required,
        "shipped_units": shipped_units,
        "excess_units": excess_units,
        "equipment": equipment_results,
        "mode_summary": mode_summary,
        "rate_breakdown": rate_breakdown,
        "excluded_equipment": sorted(excluded_equipment, key=lambda r: (r["mode"], r.get("equipment_code") or "", r["equipment_name"] or "")),
        "warnings": [w for w in [truck_warning] if w],
    }
