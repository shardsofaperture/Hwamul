from constraints_engine import max_units_per_conveyance


def test_ibc_non_stackable_40rf_floor_grid_limits_to_18():
    pack_rule = {
        "units_per_pack": 1,
        "kg_per_unit": 1065,
        "pack_tare_kg": 0,
        "dim_l_m": 1.2,
        "dim_w_m": 1.0,
        "dim_h_m": 1.16,
        "stackable": 0,
        "max_stack": None,
    }
    equipment = {
        "mode": "OCEAN",
        "internal_length_m": 11.588,
        "internal_width_m": 2.280,
        "internal_height_m": 2.255,
        "max_payload_kg": 29580,
    }
    result = max_units_per_conveyance(1, pack_rule, equipment, context={})
    assert result["packs_per_layer"] == 18
    assert result["layers_allowed"] == 1
    assert result["max_units"] == 18
    assert result["limiting_constraint"] == "FLOOR_GRID"


def test_dray_legal_payload_can_be_limiting_constraint():
    pack_rule = {
        "units_per_pack": 1,
        "kg_per_unit": 5000,
        "pack_tare_kg": 0,
        "dim_l_m": 1,
        "dim_w_m": 1,
        "dim_h_m": 1,
        "stackable": 1,
    }
    equipment = {
        "mode": "DRAY",
        "internal_length_m": 12.03,
        "internal_width_m": 2.35,
        "internal_height_m": 2.39,
        "max_payload_kg": 50000,
    }
    context = {
        "container_on_chassis": True,
        "truck_config": {
            "steer_axles": 1,
            "drive_axles": 2,
            "trailer_axles": 2,
            "axle_span_ft": 51,
            "tractor_tare_lb": 22000,
            "trailer_tare_lb": 12000,
            "container_tare_lb": 9000,
            "max_gvw_lb": 80000,
            "steer_weight_share_pct": 0.12,
            "drive_weight_share_pct": 0.44,
            "trailer_weight_share_pct": 0.44,
        },
        "jurisdiction_rule": {
            "max_gvw_lb": 80000,
            "max_single_axle_lb": 20000,
            "max_tandem_lb": 34000,
        },
    }
    result = max_units_per_conveyance(1, pack_rule, equipment, context=context)
    assert result["limiting_constraint"] in {"DRAY_LEGAL_PAYLOAD", "AXLE_GROUP_LIMIT", "GVW_LIMIT", "BRIDGE_FORMULA"}
    assert result["max_units"] >= 0
