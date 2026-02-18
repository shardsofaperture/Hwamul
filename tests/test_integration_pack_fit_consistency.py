import sqlite3

from batch_planner import plan_containers_no_mix
from constraints_engine import max_units_per_conveyance
from planning_engine import plan_quick_run


def _setup_integration_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE sku_master (sku_id INTEGER PRIMARY KEY, part_number TEXT, description TEXT, default_coo TEXT);
        CREATE TABLE packaging_rules (
            id INTEGER PRIMARY KEY,
            sku_id INTEGER,
            pack_name TEXT,
            is_default INTEGER,
            units_per_pack REAL,
            kg_per_unit REAL,
            pack_tare_kg REAL,
            dim_l_m REAL,
            dim_w_m REAL,
            dim_h_m REAL,
            min_order_packs INTEGER,
            increment_packs INTEGER,
            stackable INTEGER,
            max_stack INTEGER
        );
        CREATE TABLE equipment_presets (
            id INTEGER PRIMARY KEY,
            equipment_code TEXT,
            name TEXT,
            mode TEXT,
            internal_length_m REAL,
            internal_width_m REAL,
            internal_height_m REAL,
            max_payload_kg REAL,
            active INTEGER DEFAULT 1,
            volumetric_factor REAL,
            max_gross_kg REAL,
            tare_kg REAL
        );
        CREATE TABLE truck_configs (
            id INTEGER PRIMARY KEY,
            truck_config_code TEXT,
            description TEXT,
            steer_axles INTEGER,
            drive_axles INTEGER,
            trailer_axles INTEGER,
            axle_span_ft REAL,
            tractor_tare_lb REAL,
            trailer_tare_lb REAL,
            container_tare_lb REAL,
            max_gvw_lb REAL,
            steer_weight_share_pct REAL,
            drive_weight_share_pct REAL,
            trailer_weight_share_pct REAL,
            active INTEGER
        );
        CREATE TABLE jurisdiction_weight_rules (
            id INTEGER PRIMARY KEY,
            jurisdiction_code TEXT,
            max_gvw_lb REAL,
            max_single_axle_lb REAL,
            max_tandem_lb REAL,
            notes TEXT,
            active INTEGER
        );
        CREATE TABLE sku_equipment_rules (
            sku_id INTEGER,
            equipment_id INTEGER,
            allowed INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY(sku_id, equipment_id)
        );
        CREATE TABLE lead_time_overrides (id INTEGER PRIMARY KEY, sku_id INTEGER, mode TEXT, lead_days INTEGER);
        CREATE TABLE lead_times (id INTEGER PRIMARY KEY, country_of_origin TEXT, mode TEXT, lead_days INTEGER);
        CREATE TABLE rates (
            id INTEGER PRIMARY KEY,
            mode TEXT,
            equipment_name TEXT,
            pricing_model TEXT,
            rate_value REAL,
            minimum_charge REAL,
            fixed_fee REAL,
            surcharge REAL
        );
        CREATE TABLE rate_card (
            id INTEGER PRIMARY KEY,
            carrier_id INTEGER,
            mode TEXT,
            service_scope TEXT,
            equipment TEXT,
            origin_type TEXT,
            origin_code TEXT,
            dest_type TEXT,
            dest_code TEXT,
            uom_pricing TEXT,
            base_rate REAL,
            min_charge REAL,
            effective_from TEXT,
            effective_to TEXT,
            is_active INTEGER,
            priority INTEGER
        );
        CREATE TABLE rate_charge (
            id INTEGER PRIMARY KEY,
            rate_card_id INTEGER,
            charge_code TEXT,
            charge_name TEXT,
            calc_method TEXT,
            amount REAL,
            applies_when TEXT
        );
        CREATE TABLE carrier (id INTEGER PRIMARY KEY, code TEXT, name TEXT);
        """
    )

    conn.execute("INSERT INTO sku_master VALUES (1, 'P1065', '1065kg pack profile', 'CN')")
    conn.execute(
        """
        INSERT INTO packaging_rules
            (id, sku_id, pack_name, is_default, units_per_pack, kg_per_unit, pack_tare_kg, dim_l_m, dim_w_m, dim_h_m,
             min_order_packs, increment_packs, stackable, max_stack)
        VALUES
            (1, 1, '1065KG_120x100x116', 1, 1, 1065.0, 0.0, 1.2, 1.0, 1.16, 1, 1, 1, NULL)
        """
    )

    conn.execute(
        """
        INSERT INTO equipment_presets
            (id, equipment_code, name, mode, internal_length_m, internal_width_m, internal_height_m, max_payload_kg, active, volumetric_factor, max_gross_kg, tare_kg)
        VALUES
            (1, 'CNT_40_DRY_STD', '40ft Dry Standard', 'OCEAN', 12.03, 2.35, 2.39, 26500, 1, NULL, 0, 0),
            (2, 'DRAY_40_DRY_STD', '40ft Dry on Chassis', 'DRAY', 12.03, 2.35, 2.39, 26500, 1, NULL, 0, 0)
        """
    )

    conn.execute("INSERT INTO lead_times VALUES (1, 'CN', 'OCEAN', 21)")
    conn.execute("INSERT INTO lead_times VALUES (2, 'CN', 'DRAY', 10)")
    conn.execute(
        "INSERT INTO truck_configs VALUES (1, '5AXLE_TL', 'baseline', 1, 2, 2, 51.0, 18000, 8000, 8500, 80000, 0.12, 0.44, 0.44, 1)"
    )
    conn.execute("INSERT INTO jurisdiction_weight_rules VALUES (1, 'US_FED_INTERSTATE', 80000, 20000, 34000, 'baseline', 1)")
    conn.commit()
    return conn


def _shared_pack_profile() -> dict:
    return {
        "sku_id": 1,
        "pack_rule": {
            "units_per_pack": 1,
            "kg_per_unit": 1065.0,
            "pack_tare_kg": 0.0,
            "dim_l_m": 1.2,
            "dim_w_m": 1.0,
            "dim_h_m": 1.16,
            "stackable": 1,
            "max_stack": None,
        },
        "expected_ocean_range": (23, 24),
        "expected_dray_range": (18, 22),
    }


def _app_cube_out_fit(pack_rule: dict, equipment: dict) -> dict:
    fit = max_units_per_conveyance(
        sku_id=1,
        pack_rule=pack_rule,
        equipment=equipment,
        context={"container_on_chassis": str(equipment.get("mode") or "").upper() in {"TRUCK", "DRAY"}},
    )
    return {"packs_fit": int(fit["max_units"]), "limiting_constraint": fit["limiting_constraint"]}


def _pick_equipment(result: dict, equipment_code: str) -> dict:
    for row in result["equipment"]:
        if row["equipment_code"] == equipment_code:
            return row
    raise AssertionError(f"equipment {equipment_code} not returned: {result['equipment']}")


def test_ocean_packs_fit_consistent_across_quick_plan_app_cube_and_batch_outputs():
    conn = _setup_integration_db()
    shared = _shared_pack_profile()

    quick_plan = plan_quick_run(
        conn=conn,
        sku_id=shared["sku_id"],
        required_units=10,
        need_date="2026-01-10",
        coo_override=None,
        pack_rule_id=1,
        lane_origin_code=None,
        lane_dest_code=None,
        service_scope=None,
        modes=["OCEAN"],
        jurisdiction_code="US_FED_INTERSTATE",
        truck_config_code="5AXLE_TL",
    )
    quick_ocean = _pick_equipment(quick_plan, "CNT_40_DRY_STD")

    app_ocean = _app_cube_out_fit(
        shared["pack_rule"],
        {
            "equipment_code": "CNT_40_DRY_STD",
            "name": "40ft Dry Standard",
            "mode": "OCEAN",
            "internal_length_m": 12.03,
            "internal_width_m": 2.35,
            "internal_height_m": 2.39,
            "max_payload_kg": 26500,
        },
    )

    batch_ocean = plan_containers_no_mix(
        [{"sku_id": 1, "required_kg": 10650, "pack_rule": shared["pack_rule"], "part_number": "P1065"}],
        {
            "equipment_code": "CNT_40_DRY_STD",
            "mode": "OCEAN",
            "internal_length_m": 12.03,
            "internal_width_m": 2.35,
            "internal_height_m": 2.39,
            "max_payload_kg": 26500,
        },
    )["per_sku"][0]

    lo, hi = shared["expected_ocean_range"]
    assert lo <= quick_ocean["packs_fit"] <= hi
    assert lo <= app_ocean["packs_fit"] <= hi
    assert lo <= batch_ocean["packs_fit"] <= hi

    assert quick_ocean["packs_fit"] == app_ocean["packs_fit"] == batch_ocean["packs_fit"]
    assert quick_ocean["limiting_constraint"] == app_ocean["limiting_constraint"] == batch_ocean["limiting_constraint"] == "CONTAINER_PAYLOAD"


def test_dray_contextual_difference_vs_container_batch_is_explicitly_justified():
    conn = _setup_integration_db()
    shared = _shared_pack_profile()

    quick_plan = plan_quick_run(
        conn=conn,
        sku_id=shared["sku_id"],
        required_units=10,
        need_date="2026-01-10",
        coo_override=None,
        pack_rule_id=1,
        lane_origin_code=None,
        lane_dest_code=None,
        service_scope=None,
        modes=["DRAY"],
        jurisdiction_code="US_FED_INTERSTATE",
        truck_config_code="5AXLE_TL",
    )
    quick_dray = _pick_equipment(quick_plan, "DRAY_40_DRY_STD")

    app_dray = _app_cube_out_fit(
        shared["pack_rule"],
        {
            "equipment_code": "DRAY_40_DRY_STD",
            "name": "40ft Dry on Chassis",
            "mode": "DRAY",
            "internal_length_m": 12.03,
            "internal_width_m": 2.35,
            "internal_height_m": 2.39,
            "max_payload_kg": 26500,
        },
    )

    batch_dray = plan_containers_no_mix(
        [{"sku_id": 1, "required_kg": 10650, "pack_rule": shared["pack_rule"], "part_number": "P1065"}],
        {
            "equipment_code": "DRAY_40_DRY_STD",
            "mode": "DRAY",
            "internal_length_m": 12.03,
            "internal_width_m": 2.35,
            "internal_height_m": 2.39,
            "max_payload_kg": 26500,
        },
    )["per_sku"][0]

    lo, hi = shared["expected_dray_range"]
    assert lo <= quick_dray["packs_fit"] <= hi
    assert lo <= app_dray["packs_fit"] <= hi

    # Explicitly justified mismatch: Quick Plan supplies jurisdiction + truck tare assumptions,
    # while app.py cube-out currently only toggles `container_on_chassis`.
    assert app_dray["packs_fit"] >= quick_dray["packs_fit"]
    assert quick_dray["limiting_constraint"] == app_dray["limiting_constraint"] == "DRAY_LEGAL_PAYLOAD"

    # Batch no-mix uses mode=DRAY legal payload defaults and should stay aligned with app.py cube-out
    # (both omit explicit jurisdiction/truck-config records).
    assert batch_dray["packs_fit"] == app_dray["packs_fit"]
    assert batch_dray["limiting_constraint"] == "DRAY_LEGAL_PAYLOAD"
