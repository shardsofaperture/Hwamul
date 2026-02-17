"""Planning service functions for allocation, recommendations, and shipment builds."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from rate_engine import RateTestInput, compute_rate_total, select_best_rate_card

from models import (
    Equipment,
    PackagingRule,
    chargeable_air_weight_kg,
    estimate_equipment_count,
    rounded_order_packs,
)


def norm_mode(mode: str | None) -> str:
    return (mode or "").strip().upper()


def safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass
class TrancheResult:
    sku_id: int
    part_number: str
    need_date: date
    tranche_name: str
    requested_units: float
    shipped_units: float
    excess_units: float
    packs: int



def allocate_tranches(
    demand_qty: float,
    rule: PackagingRule,
    tranches: list[tuple[str, str, float]],
    *,
    need_date: date | None = None,
    sku_id: int = 0,
) -> list[TrancheResult]:
    """Allocate demand into tranches and carry excess to later tranches.

    Tranches format: (name, type, value), where type is 'percent' or 'absolute'.
    """
    demand_qty = max(0.0, safe_float(demand_qty))
    tranche_targets: list[tuple[str, float]] = []
    for name, alloc_type, value in tranches:
        alloc = (alloc_type or "").strip().lower()
        if alloc == "percent":
            target = demand_qty * safe_float(value) / 100.0
        else:
            target = max(0.0, safe_float(value))
        tranche_targets.append((name, target))

    carry_excess = 0.0
    rows: list[TrancheResult] = []
    row_need_date = need_date or date.today()
    for name, target in tranche_targets:
        requested = max(0.0, target - carry_excess)
        packs = rounded_order_packs(requested, rule)
        shipped_units = packs * rule.units_per_pack
        excess = max(0.0, shipped_units - requested)
        carry_excess = excess
        rows.append(
            TrancheResult(
                sku_id=sku_id,
                part_number=rule.part_number,
                need_date=row_need_date,
                tranche_name=name,
                requested_units=requested,
                shipped_units=shipped_units,
                excess_units=excess,
                packs=packs,
            )
        )
    return rows


def lead_days_for(mode: str, coo: str, sku_id: int, lead_table: dict[tuple[str, str], int], sku_override: dict[tuple[int, str], int], manual_override: int | None = None) -> int:
    mode_key = norm_mode(mode)
    coo_key = (coo or "").strip().upper()
    if manual_override is not None:
        return manual_override
    if (sku_id, mode_key) in sku_override:
        return sku_override[(sku_id, mode_key)]
    return lead_table.get((coo_key, mode_key), 999)


def recommend_modes(
    sku_id: int,
    part_number: str,
    coo: str,
    need_date: date,
    requested_units: float,
    pack_rule: PackagingRule,
    equipment_by_mode: dict[str, list[Equipment]],
    rates: list[dict],
    lead_table: dict[tuple[str, str], int],
    sku_lead_override: dict[tuple[int, str], int],
    manual_lead_override: int | None = None,
    *,
    phase: str = "",
    phase_defaults: dict[str, dict] | None = None,
    rate_cards: list[dict] | None = None,
    rate_charges: list[dict] | None = None,
    service_scope: str = "P2P",
    mode_override: str | None = None,
    route_info: dict | None = None,
    miles: float | None = None,
):
    requested_units = max(0.0, safe_float(requested_units))
    shipped_packs = rounded_order_packs(requested_units, pack_rule)
    shipped_units = shipped_packs * pack_rule.units_per_pack
    excess_units = max(0.0, shipped_units - requested_units)
    total_weight = (shipped_units / pack_rule.units_per_pack) * pack_rule.gross_pack_weight_kg
    total_volume = (shipped_units / pack_rule.units_per_pack) * pack_rule.pack_cube_m3

    recs = []
    phase_defaults = phase_defaults or {}
    phase_cfg = phase_defaults.get((phase or "").strip(), {})
    if phase_cfg.get("service_scope"):
        service_scope = phase_cfg["service_scope"]

    normalized_eq: dict[str, list[Equipment]] = {}
    for mode, items in equipment_by_mode.items():
        normalized_eq.setdefault(norm_mode(mode), []).extend(items)
    equipment_by_mode = normalized_eq
    if mode_override:
        equipment_by_mode = {k: v for k, v in equipment_by_mode.items() if k == norm_mode(mode_override)}

    route = route_info or {}
    has_route = bool(route.get("origin_port") or route.get("dest_port") or route.get("supplier_city") or route.get("supplier_code") or route.get("plant_code") or route.get("plant"))
    origin_port = route.get("origin_port", "")
    dest_port = route.get("dest_port", "")
    supplier_loc = route.get("supplier_city", route.get("supplier_code", ""))
    plant_loc = route.get("plant_code", route.get("plant", ""))

    def _flags(eq_name: str):
        eqn = (eq_name or "").upper()
        l_ok = True
        w_ok = True
        h_ok = True
        if not eqn.endswith("FR"):
            l_ok = pack_rule.dim_l_norm_m <= 12.03
            w_ok = pack_rule.dim_w_norm_m <= 2.35
            h_ok = pack_rule.dim_h_norm_m <= 2.39
        over_h = not h_ok
        over_w = not w_ok
        return {
            "flatrack": eqn.endswith("FR"),
            "over_height": over_h,
            "over_width": over_w,
            "over_height_width": over_h and over_w,
        }

    def _leg(mode: str, equipment: str, scope: str, o_type: str, o_code: str, d_type: str, d_code: str, leg_miles: float | None, containers_count: float, chargeable: float, flags: dict):
        shipment = RateTestInput(
            ship_date=ship_by,
            mode=mode.upper(),
            equipment=equipment.upper(),
            service_scope=scope.upper(),
            origin_type=o_type.upper(),
            origin_code=(o_code or "").upper(),
            dest_type=d_type.upper(),
            dest_code=(d_code or "").upper(),
            weight_kg=total_weight,
            volume_m3=total_volume,
            miles=leg_miles,
            containers_count=containers_count,
            chargeable_weight_kg=chargeable,
            **flags,
        )
        if not rate_cards:
            return None
        card = select_best_rate_card(rate_cards, shipment)
        if not card:
            return None
        result = compute_rate_total(card, rate_charges or [], shipment)
        return card, result
    for mode, equipments in equipment_by_mode.items():
        mode = norm_mode(mode)
        lead_days = lead_days_for(mode, coo, sku_id, lead_table, sku_lead_override, manual_lead_override)
        ship_by = need_date - timedelta(days=lead_days)
        feasible = lead_days < 900
        cost = 0.0
        eq_count = 0
        util = 0.0

        if mode == "AIR":
            eq = equipments[0]
            chargeable = chargeable_air_weight_kg(total_weight, total_volume, eq.volumetric_factor or 167)
            rate = next((r for r in rates if norm_mode(r.get("mode")) == "AIR" and str(r.get("pricing_model", "")).lower() == "per_kg"), None)
            if rate:
                cost = max(rate["minimum_charge"] or 0, chargeable * rate["rate_value"]) + (rate["fixed_fee"] or 0)
            eq_count = 1
            util = min(1.0, total_weight / eq.max_payload_kg)
        else:
            eq = equipments[0]
            eq_count = estimate_equipment_count(total_volume, total_weight, eq)
            rate = next((
                r for r in rates
                if norm_mode(r.get("mode")) == mode and r.get("equipment_name") == eq.name and str(r.get("pricing_model", "")).lower() in {"per_container", "per_load"}
            ), None)
            if rate:
                cost = eq_count * rate["rate_value"] + (rate["fixed_fee"] or 0) + (rate["surcharge"] or 0)
            util = min(1.0, max(total_volume / (eq_count * eq.volume_m3), total_weight / (eq_count * eq.max_payload_kg))) if eq_count else 0

        details = []
        card_id = None
        main_cost = cost
        domestic_cost = 0.0
        if rate_cards and has_route:
            flags = _flags(eq.name)
            main_leg = _leg(
                mode,
                eq.name,
                service_scope,
                "PORT" if service_scope.startswith("P") else "CITY",
                origin_port if service_scope.startswith("P") else supplier_loc,
                "PORT" if service_scope.endswith("P") else "CITY",
                dest_port if service_scope.endswith("P") else plant_loc,
                miles if mode == "TRUCK" else None,
                float(eq_count or 1),
                chargeable_air_weight_kg(total_weight, total_volume, eq.volumetric_factor or 167) if mode == "AIR" else total_weight,
                flags,
            )
            if main_leg:
                card, result = main_leg
                card_id = card.get("id")
                main_cost = float(result["grand_total"])
                details.extend(result["items"])
            if service_scope in {"P2D", "D2D"}:
                truck_leg = _leg("TRUCK", "53FT", "D2D", "CITY", dest_port, "CITY", plant_loc, miles, 1.0, total_weight, {"flatrack": False, "over_height": False, "over_width": False, "over_height_width": False})
                if truck_leg:
                    _, result = truck_leg
                    domestic_cost += float(result["grand_total"])
                    details.extend(result["items"])
            if service_scope in {"D2P", "D2D"}:
                truck_leg = _leg("TRUCK", "53FT", "D2D", "CITY", supplier_loc, "CITY", origin_port, miles, 1.0, total_weight, {"flatrack": False, "over_height": False, "over_width": False, "over_height_width": False})
                if truck_leg:
                    _, result = truck_leg
                    domestic_cost += float(result["grand_total"])
                    details.extend(result["items"])

        recs.append(
            {
                "mode": mode,
                "lead_days": lead_days,
                "ship_by": ship_by.isoformat(),
                "feasible": feasible,
                "meets_date": feasible,
                "ship_by_date": ship_by.isoformat(),
                "requested_units": requested_units,
                "shipped_units": shipped_units,
                "excess_units": excess_units,
                "estimated_cost": round(main_cost + domestic_cost, 2),
                "cost_total": round(main_cost + domestic_cost, 2),
                "base_cost": round(main_cost, 2),
                "domestic_legs_cost": round(domestic_cost, 2),
                "equipment_count": eq_count,
                "required_units_count": eq_count,
                "selected_rate_card_id": card_id,
                "service_scope": service_scope,
                "cost_items": details,
                "utilization_pct": round(util * 100, 1),
                "cube_utilization": round(min(1.0, (total_volume / (eq_count * eq.volume_m3)) if eq_count and eq.volume_m3 else 0.0), 4),
                "weight_utilization": round(min(1.0, (total_weight / (eq_count * eq.max_payload_kg)) if eq_count and eq.max_payload_kg else 0.0), 4),
            }
        )

    recs.sort(key=lambda r: (not r["feasible"], r["estimated_cost"]))
    return recs


def build_shipments(tranches: list[dict], equipment_map: dict[str, Equipment]):
    grouped = defaultdict(list)
    normalized_eq = {norm_mode(k): v for k, v in equipment_map.items()}
    for row in tranches:
        grouped[norm_mode(row["mode"])].append(row)

    outputs = []
    for mode, rows in grouped.items():
        eq = normalized_eq[mode]
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


def customs_report(shipments: list[dict], sku_rows: list[dict], customs_rates: list[dict], as_of: date | None = None) -> list[dict]:
    as_of = as_of or date.today()
    sku_ix = {(r.get("sku_id"), r.get("part_number")): r for r in sku_rows}
    out: list[dict] = []
    for s in shipments:
        sku = sku_ix.get((s.get("sku_id"), s.get("part_number")), {})
        hts = sku.get("hts_code") or s.get("hts_code")
        coo = s.get("coo") or sku.get("default_coo")
        matches = [r for r in customs_rates if r.get("hts_code") == hts and (r.get("country_of_origin") in {None, "", coo})]
        chosen = None
        for r in matches:
            start = date.fromisoformat(r["effective_from"])
            end = date.fromisoformat(r["effective_to"]) if r.get("effective_to") else None
            if as_of >= start and (end is None or as_of <= end):
                chosen = r
                break
        declared_value = float(s.get("declared_value") or (s.get("qty", 0) * s.get("unit_price", 0)))
        base = float(chosen.get("base_duty_rate") or 0) if chosen else 0.0
        tariff = float(chosen.get("tariff_rate") or 0) if chosen else 0.0
        duty = declared_value * (base + tariff) / 100
        out.append({
            "phase": s.get("phase", ""),
            "part_number": s.get("part_number", ""),
            "supplier": s.get("supplier_code", ""),
            "hts_code": hts,
            "coo": coo,
            "quantity": s.get("qty", 0),
            "gross_weight": s.get("gross_weight_kg", s.get("weight_kg", 0)),
            "net_weight": s.get("net_weight_kg", s.get("weight_kg", 0)),
            "declared_value": round(declared_value, 2),
            "base_duty_rate": base,
            "tariff_rate": tariff,
            "section_232": int(chosen.get("section_232") or 0) if chosen else 0,
            "section_301": int(chosen.get("section_301") or 0) if chosen else 0,
            "duty_amount": round(duty, 2),
            "port": s.get("port", s.get("dest_port", "")),
            "importer": s.get("importer", ""),
            "exporter": s.get("exporter", ""),
            "incoterms": s.get("incoterms", ""),
            "plant": s.get("plant", ""),
        })
    return out


def phase_cost_rollup(shipments: list[dict], customs_rows: list[dict]) -> list[dict]:
    duty_by_phase: dict[str, float] = defaultdict(float)
    for r in customs_rows:
        duty_by_phase[str(r.get("phase", ""))] += float(r.get("duty_amount", 0))
    bucket: dict[str, dict] = {}
    for s in shipments:
        ph = str(s.get("phase", ""))
        rec = bucket.setdefault(ph, {"phase": ph, "total_cost": 0.0, "weight": 0.0, "volume": 0.0, "modes": set(), "arrivals": []})
        rec["total_cost"] += float(s.get("base_cost", s.get("estimated_cost", 0))) + float(s.get("domestic_legs_cost", 0))
        rec["weight"] += float(s.get("weight_kg", 0))
        rec["volume"] += float(s.get("volume_m3", 0))
        rec["modes"].add(str(s.get("mode", "")))
        if s.get("arrival_date"):
            rec["arrivals"].append(s["arrival_date"])
    rows = []
    for ph, rec in bucket.items():
        total = rec["total_cost"] + duty_by_phase.get(ph, 0.0)
        rows.append({
            "phase": ph,
            "mode_mix": ",".join(sorted(m for m in rec["modes"] if m)),
            "total_cost": round(total, 2),
            "cost_per_kg": round(total / rec["weight"], 4) if rec["weight"] else 0,
            "cost_per_m3": round(total / rec["volume"], 4) if rec["volume"] else 0,
            "eta_min": min(rec["arrivals"]) if rec["arrivals"] else "",
            "eta_max": max(rec["arrivals"]) if rec["arrivals"] else "",
        })
    return sorted(rows, key=lambda r: r["phase"])
