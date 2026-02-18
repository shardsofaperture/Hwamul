import sqlite3

from planning_engine import plan_quick_run


def _setup_min_db() -> sqlite3.Connection:
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

        CREATE TABLE ship_from_locations (
            ship_from_location_id INTEGER PRIMARY KEY,
            canonical_location_key TEXT,
            city TEXT,
            port_code TEXT,
            supplier_duns TEXT,
            internal_location_code TEXT
        );
        CREATE TABLE sku_ship_to_locations (
            sku_id INTEGER,
            destination_code TEXT,
            PRIMARY KEY(sku_id, destination_code)
        );
        CREATE TABLE sku_allowed_modes (
            sku_id INTEGER,
            mode_code TEXT,
            PRIMARY KEY(sku_id, mode_code)
        );
        """
    )
    conn.execute("INSERT INTO sku_master VALUES (1, 'P1', 'Part 1', 'CN')")
    conn.execute(
        "INSERT INTO packaging_rules VALUES (1, 1, 'STD', 1, 6, 1.0, 0.0, 1.0, 1.0, 1.0, 1, 1, 1, NULL)"
    )
    conn.execute(
        "INSERT INTO equipment_presets VALUES (1, 'AIR_STD', 'AIR', 'Air', 10.0, 1.0, 1.0, 1000.0, 1, 167.0, 0, 0)"
    )
    conn.execute("INSERT INTO lead_times VALUES (1, 'CN', 'AIR', 7)")
    conn.execute("INSERT INTO lead_time_overrides VALUES (1, 1, 'AIR', 3)")
    conn.execute(
        "INSERT INTO rates VALUES (1, 'AIR', 'AIR_STD', 'per_container', 1000, NULL, 0, 0)"
    )

    conn.execute("INSERT INTO truck_configs VALUES (1, '5AXLE_TL', 'baseline', 1, 2, 2, 51.0, 18000, 8000, 8500, 80000, 0.12, 0.44, 0.44, 1)")
    conn.execute("INSERT INTO jurisdiction_weight_rules VALUES (1, 'US_FED_INTERSTATE', 80000, 20000, 34000, 'baseline', 1)")
    conn.commit()
    return conn


def test_plan_quick_run_uses_lead_override_for_sku_id():
    conn = _setup_min_db()
    result = plan_quick_run(
        conn=conn,
        sku_id=1,
        required_units=39,
        need_date="2026-01-10",
        coo_override=None,
        pack_rule_id=None,
        lane_origin_code=None,
        lane_dest_code=None,
        service_scope=None,
        modes=["AIR"],
    )
    assert result["packs_required"] == 7
    assert result["shipped_units"] == 42
    assert result["excess_units"] == 3
    assert result["mode_summary"][0]["mode"] == "AIR"
    assert result["mode_summary"][0]["lead_days"] == 3
    assert result["mode_summary"][0]["ship_by_date"] == "2026-01-07"


def test_disallowed_equipment_filtering():
    conn = _setup_min_db()
    conn.execute("INSERT INTO equipment_presets VALUES (2, 'CNT_40_DRY_STD', 'DRY_STD', 'OCEAN', 12, 2.3, 2.3, 26000, 1, NULL, 0, 0)")
    conn.execute("INSERT INTO equipment_presets VALUES (3, 'CNT_40_DRY_HC', '40HC_DRY', 'OCEAN', 12.03, 2.352, 2.698, 26540, 1, NULL, 0, 0)")
    conn.execute("INSERT INTO sku_equipment_rules VALUES (1, 1, 0)")  # AIR denied
    conn.execute("INSERT INTO sku_equipment_rules VALUES (1, 2, 0)")  # DRY_STD denied
    conn.execute("INSERT INTO sku_equipment_rules VALUES (1, 3, 1)")  # 40HC allowed
    conn.commit()

    result = plan_quick_run(
        conn=conn,
        sku_id=1,
        required_units=10,
        need_date="2026-01-10",
        coo_override=None,
        pack_rule_id=None,
        lane_origin_code=None,
        lane_dest_code=None,
        service_scope=None,
        modes=["AIR", "OCEAN"],
    )
    names = [row["equipment_name"] for row in result["equipment"]]
    assert names == ["40HC_DRY"]
    excluded_names = {row["equipment_name"] for row in result["excluded_equipment"]}
    assert {"AIR", "DRY_STD"}.issubset(excluded_names)


def test_missing_equipment_payload_is_reported_as_excluded_reason():
    conn = _setup_min_db()
    conn.execute("DELETE FROM equipment_presets")
    conn.execute("INSERT INTO equipment_presets VALUES (1, 'BROKEN', 'BROKEN', 'AIR', 10.0, 1.0, 1.0, 0.0, 1, NULL, 0, 0)")
    conn.commit()

    result = plan_quick_run(
        conn=conn,
        sku_id=1,
        required_units=10,
        need_date="2026-01-10",
        coo_override=None,
        pack_rule_id=None,
        lane_origin_code=None,
        lane_dest_code=None,
        service_scope=None,
        modes=["AIR"],
    )
    assert result["equipment"] == []
    assert "max_payload_kg" in result["excluded_equipment"][0]["reason"]


def test_routing_context_auto_selects_ship_from_dest_and_mode_filters():
    conn = _setup_min_db()
    conn.execute("ALTER TABLE sku_master ADD COLUMN ship_from_location_id INTEGER")
    conn.execute("ALTER TABLE sku_master ADD COLUMN incoterm TEXT")
    conn.execute("ALTER TABLE sku_master ADD COLUMN incoterm_named_place TEXT")
    conn.execute("UPDATE sku_master SET ship_from_location_id = 1, incoterm = 'FOB', incoterm_named_place = 'SHANGHAI PORT' WHERE sku_id = 1")
    conn.execute("INSERT INTO ship_from_locations VALUES (1, 'K1', 'SHANGHAI', 'CNSHA', '123', 'CN_SHA_PDC')")
    conn.execute("INSERT INTO sku_ship_to_locations VALUES (1, 'USLAX_DC01')")
    conn.execute("INSERT INTO sku_allowed_modes VALUES (1, 'OCEAN')")
    conn.execute("INSERT INTO equipment_presets VALUES (2, 'CNT_40_DRY_STD', 'DRY_STD', 'OCEAN', 12, 2.3, 2.3, 26000, 1, NULL, 0, 0)")
    conn.commit()

    result = plan_quick_run(
        conn=conn,
        sku_id=1,
        required_units=10,
        need_date="2026-01-10",
        coo_override=None,
        pack_rule_id=None,
        lane_origin_code=None,
        lane_dest_code=None,
        service_scope="P2P",
        modes=None,
    )
    assert {row["mode"] for row in result["equipment"]} == {"OCEAN"}
    assert any(row["mode"] == "AIR" and "allowed_modes" in row["reason"] for row in result["excluded_equipment"])
    routing = result["routing_context"]
    assert routing["selected_origin_code"] == "CNSHA"
    assert routing["selected_dest_code"] == "USLAX_DC01"
    assert routing["incoterm"] == "FOB"
