from models import (
    Equipment,
    PackagingRule,
    chargeable_air_weight_kg,
    estimate_equipment_count,
    rounded_order_packs,
)


def test_pack_rounding_with_moq_and_increment():
    rule = PackagingRule(
        part_number="S1",
        units_per_pack=24,
        kg_per_unit=1,
        pack_tare_kg=0.5,
        pack_length_m=0.5,
        pack_width_m=0.4,
        pack_height_m=0.3,
        min_order_packs=5,
        increment_packs=5,
    )
    assert rounded_order_packs(50, rule) == 5
    assert rounded_order_packs(240, rule) == 10


def test_chargeable_air_weight():
    assert chargeable_air_weight_kg(120, 1.0, 167) == 167
    assert chargeable_air_weight_kg(200, 1.0, 167) == 200


def test_estimate_container_count():
    eq = Equipment("40 Dry", "Ocean", 12, 2.3, 2.3, 26000)
    assert estimate_equipment_count(50, 10000, eq) == 1
    assert estimate_equipment_count(100, 10000, eq) == 2
    assert estimate_equipment_count(10, 40000, eq) == 2
