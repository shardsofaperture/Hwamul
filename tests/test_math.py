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
        dim_l_m=0.5,
        dim_w_m=0.4,
        dim_h_m=0.3,
        min_order_packs=5,
        increment_packs=5,
    )
    assert rounded_order_packs(50, rule) == 5
    assert rounded_order_packs(240, rule) == 10


def test_pack_cube_supports_cm_input_for_pallets_and_crates():
    cm_rule = PackagingRule(
        part_number="PALLET",
        units_per_pack=1,
        kg_per_unit=10,
        pack_tare_kg=1,
        dim_l_m=120,
        dim_w_m=80,
        dim_h_m=90,
    )
    m_rule = PackagingRule(
        part_number="PALLET",
        units_per_pack=1,
        kg_per_unit=10,
        pack_tare_kg=1,
        dim_l_m=1.2,
        dim_w_m=0.8,
        dim_h_m=0.9,
    )
    assert cm_rule.pack_cube_m3 == m_rule.pack_cube_m3


def test_pack_dimension_normalization_edge_inputs_and_cube():
    # meters input
    rule_meters = PackagingRule(
        part_number="EDGE-M",
        units_per_pack=1,
        kg_per_unit=1,
        pack_tare_kg=0,
        dim_l_m=2.5,
        dim_w_m=0.3,
        dim_h_m=0.3,
    )
    assert rule_meters.dim_l_norm_m == 2.5
    assert rule_meters.dim_w_norm_m == 0.3
    assert rule_meters.dim_h_norm_m == 0.3
    assert rule_meters.pack_cube_m3 == 2.5 * 0.3 * 0.3

    # centimeters input
    rule_cm = PackagingRule(
        part_number="EDGE-CM",
        units_per_pack=1,
        kg_per_unit=1,
        pack_tare_kg=0,
        dim_l_m=30,
        dim_w_m=30,
        dim_h_m=300,
    )
    assert rule_cm.dim_l_norm_m == 0.3
    assert rule_cm.dim_w_norm_m == 0.3
    assert rule_cm.dim_h_norm_m == 3.0
    assert rule_cm.pack_cube_m3 == 0.3 * 0.3 * 3.0


def test_chargeable_air_weight():
    assert chargeable_air_weight_kg(120, 1.0, 167) == 167
    assert chargeable_air_weight_kg(200, 1.0, 167) == 200


def test_estimate_container_count():
    eq = Equipment("40 Dry", "Ocean", 12, 2.3, 2.3, 26000)
    assert estimate_equipment_count(50, 10000, eq) == 1
    assert estimate_equipment_count(100, 10000, eq) == 2
    assert estimate_equipment_count(10, 40000, eq) == 2


def test_pack_rounding_rejects_non_positive_units_per_pack():
    rule = PackagingRule(
        part_number="S1",
        units_per_pack=0,
        kg_per_unit=1,
        pack_tare_kg=0.5,
        dim_l_m=0.5,
        dim_w_m=0.4,
        dim_h_m=0.3,
    )

    try:
        rounded_order_packs(50, rule)
        assert False, "Expected ValueError for units_per_pack <= 0"
    except ValueError:
        pass
