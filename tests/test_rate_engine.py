from datetime import date

from rate_engine import RateTestInput, compute_rate_total, select_best_rate_card


def test_select_best_rate_card_prefers_priority_then_latest_effective():
    shipment = RateTestInput(
        ship_date=date(2026, 1, 15),
        mode="OCEAN",
        equipment="40DV",
        service_scope="P2P",
        origin_type="PORT",
        origin_code="USLAX",
        dest_type="PORT",
        dest_code="CNSHA",
    )

    cards = [
        {
            "id": 1,
            "is_active": 1,
            "mode": "OCEAN",
            "equipment": "40DV",
            "service_scope": "P2P",
            "origin_type": "PORT",
            "origin_code": "USLAX",
            "dest_type": "PORT",
            "dest_code": "CNSHA",
            "effective_from": "2025-01-01",
            "effective_to": None,
            "priority": 5,
            "carrier_id": None,
        },
        {
            "id": 2,
            "is_active": 1,
            "mode": "OCEAN",
            "equipment": "40DV",
            "service_scope": "P2P",
            "origin_type": "PORT",
            "origin_code": "USLAX",
            "dest_type": "PORT",
            "dest_code": "CNSHA",
            "effective_from": "2025-06-01",
            "effective_to": None,
            "priority": 5,
            "carrier_id": None,
        },
    ]

    best = select_best_rate_card(cards, shipment)
    assert best is not None
    assert best["id"] == 2


def test_compute_rate_total_with_accessorials_and_bounds():
    shipment = RateTestInput(
        ship_date=date(2026, 1, 15),
        mode="OCEAN",
        equipment="40DV",
        service_scope="P2P",
        origin_type="PORT",
        origin_code="USLAX",
        dest_type="PORT",
        dest_code="CNSHA",
        reefer=True,
        weight_kg=500,
        volume_m3=10,
        containers_count=2,
    )
    card = {
        "id": 10,
        "currency": "USD",
        "base_rate": 100,
        "uom_pricing": "PER_CONTAINER",
        "min_charge": 300,
    }
    charges = [
        {
            "rate_card_id": 10,
            "charge_code": "DOC",
            "charge_name": "Docs",
            "calc_method": "FLAT",
            "amount": 50,
            "applies_when": "ALWAYS",
            "min_amount": None,
            "max_amount": None,
            "effective_from": None,
            "effective_to": None,
        },
        {
            "rate_card_id": 10,
            "charge_code": "REEFER",
            "charge_name": "Reefer power",
            "calc_method": "PER_CONTAINER",
            "amount": 40,
            "applies_when": "REEFER_ONLY",
            "min_amount": 100,
            "max_amount": None,
            "effective_from": None,
            "effective_to": None,
        },
    ]

    result = compute_rate_total(card, charges, shipment)

    assert result["base_total"] == 300
    assert result["charges_total"] == 150
    assert result["grand_total"] == 450
    assert len(result["items"]) == 3
