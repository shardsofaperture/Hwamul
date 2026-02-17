"""Master rate engine with effective dating and accessorial calculations."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date


SPECIFICITY_SCORES = {
    "PORT": 4,
    "CITY": 3,
    "REGION": 2,
    "COUNTRY": 1,
}


@dataclass
class RateTestInput:
    ship_date: date
    mode: str
    equipment: str
    service_scope: str
    origin_type: str
    origin_code: str
    dest_type: str
    dest_code: str
    carrier_id: int | None = None
    reefer: bool = False
    flatrack: bool = False
    over_height: bool = False
    over_width: bool = False
    over_height_width: bool = False
    dg: bool = False
    weight_kg: float = 0.0
    volume_m3: float = 0.0
    miles: float | None = None
    containers_count: float | None = None
    chargeable_weight_kg: float | None = None


def _is_date_valid(ship_date: date, effective_from: str, effective_to: str | None) -> bool:
    start = date.fromisoformat(effective_from)
    if ship_date < start:
        return False
    if effective_to:
        end = date.fromisoformat(effective_to)
        return ship_date <= end
    return True


def _specificity_score(row: dict) -> int:
    return SPECIFICITY_SCORES.get((row.get("origin_type") or "").upper(), 0) + SPECIFICITY_SCORES.get((row.get("dest_type") or "").upper(), 0)


def _charge_flag_applies(flag: str, shipment: RateTestInput) -> bool:
    flag = (flag or "ALWAYS").strip().upper()
    if flag == "ALWAYS":
        return True
    if flag in {"FR_ONLY", "FLATRACK_ONLY"}:
        return shipment.flatrack
    if flag == "REEFER_ONLY":
        return shipment.reefer
    if flag == "OH_ONLY":
        return shipment.over_height
    if flag == "OW_ONLY":
        return shipment.over_width
    if flag == "OHW_ONLY":
        return shipment.over_height_width
    if flag == "DG_ONLY":
        return shipment.dg
    return False


def _calc_charge(calc_method: str, amount: float, shipment: RateTestInput, base_total: float) -> float:
    method = (calc_method or "FLAT").upper()
    if method == "FLAT":
        return amount
    if method == "PER_CONTAINER":
        return amount * (shipment.containers_count or 0)
    if method == "PER_KG":
        weight = shipment.chargeable_weight_kg if shipment.chargeable_weight_kg is not None else shipment.weight_kg
        return amount * weight
    if method == "PER_CBM":
        return amount * shipment.volume_m3
    if method == "PER_MILE":
        return amount * (shipment.miles or 0)
    if method == "PERCENT_OF_BASE":
        return base_total * amount / 100
    return 0.0


def _apply_min_max(value: float, min_amount: float | None, max_amount: float | None) -> float:
    if min_amount is not None:
        value = max(value, min_amount)
    if max_amount is not None:
        value = min(value, max_amount)
    return value


def _base_total(rate_card: dict, shipment: RateTestInput) -> float:
    base_rate = float(rate_card.get("base_rate") or 0)
    uom = (rate_card.get("uom_pricing") or "FLAT").upper()
    if uom == "PER_CONTAINER":
        total = base_rate * (shipment.containers_count or 0)
    elif uom in {"PER_KG", "PER_CHARGEABLE_KG"}:
        weight = shipment.chargeable_weight_kg if shipment.chargeable_weight_kg is not None else shipment.weight_kg
        total = base_rate * weight
    elif uom == "PER_CBM":
        total = base_rate * shipment.volume_m3
    elif uom == "PER_MILE":
        total = base_rate * (shipment.miles or 0)
    else:
        total = base_rate
    min_charge = rate_card.get("min_charge")
    if min_charge is not None:
        total = max(total, float(min_charge))
    return total


def select_best_rate_card(rate_cards: list[dict], shipment: RateTestInput) -> dict | None:
    candidates = []
    for row in rate_cards:
        if not row.get("is_active"):
            continue
        if row.get("carrier_id") and shipment.carrier_id and int(row["carrier_id"]) != int(shipment.carrier_id):
            continue
        if (row.get("mode") or "").upper() != shipment.mode.upper():
            continue
        if (row.get("equipment") or "").upper() != shipment.equipment.upper():
            continue
        if (row.get("service_scope") or "").upper() != shipment.service_scope.upper():
            continue
        if (row.get("origin_type") or "").upper() != shipment.origin_type.upper():
            continue
        if (row.get("origin_code") or "").upper() != shipment.origin_code.upper():
            continue
        if (row.get("dest_type") or "").upper() != shipment.dest_type.upper():
            continue
        if (row.get("dest_code") or "").upper() != shipment.dest_code.upper():
            continue
        if not _is_date_valid(shipment.ship_date, row["effective_from"], row.get("effective_to")):
            continue
        contract_start = row.get("contract_start")
        contract_end = row.get("contract_end")
        if contract_start and shipment.ship_date < date.fromisoformat(contract_start):
            continue
        if contract_end and shipment.ship_date > date.fromisoformat(contract_end):
            continue
        candidates.append(row)

    if not candidates:
        return None

    return max(
        candidates,
        key=lambda r: (
            _specificity_score(r),
            int(r.get("priority") or 0),
            date.fromisoformat(r["effective_from"]),
        ),
    )


def compute_rate_total(rate_card: dict, charges: list[dict], shipment: RateTestInput) -> dict:
    base_total = _base_total(rate_card, shipment)
    items = [
        {
            "type": "BASE",
            "code": "BASE",
            "name": "Base freight",
            "amount": round(base_total, 2),
        }
    ]

    charges_total = 0.0
    for charge in charges:
        if int(charge.get("rate_card_id") or 0) != int(rate_card["id"]):
            continue
        eff_from = charge.get("effective_from")
        eff_to = charge.get("effective_to")
        if eff_from and shipment.ship_date < date.fromisoformat(eff_from):
            continue
        if eff_to and shipment.ship_date > date.fromisoformat(eff_to):
            continue
        if not _charge_flag_applies(charge.get("applies_when") or "ALWAYS", shipment):
            continue

        raw = _calc_charge(charge.get("calc_method") or "FLAT", float(charge.get("amount") or 0), shipment, base_total)
        bounded = _apply_min_max(raw, charge.get("min_amount"), charge.get("max_amount"))
        charges_total += bounded
        items.append(
            {
                "type": "ACCESSORIAL",
                "code": charge.get("charge_code"),
                "name": charge.get("charge_name"),
                "amount": round(bounded, 2),
            }
        )

    return {
        "currency": rate_card.get("currency"),
        "base_total": round(base_total, 2),
        "charges_total": round(charges_total, 2),
        "grand_total": round(base_total + charges_total, 2),
        "items": items,
    }
