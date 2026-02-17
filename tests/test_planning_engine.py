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
            name TEXT,
            mode TEXT,
            internal_length_m REAL,
            internal_width_m REAL,
            internal_height_m REAL,
            max_payload_kg REAL
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
    conn.execute("INSERT INTO sku_master VALUES (1, 'P1', 'Part 1', 'CN')")
    conn.execute(
        "INSERT INTO packaging_rules VALUES (1, 1, 'STD', 1, 6, 1.0, 0.0, 1.0, 1.0, 1.0, 1, 1, 1, NULL)"
    )
    conn.execute(
        "INSERT INTO equipment_presets VALUES (1, 'AIR', 'Air', 10.0, 1.0, 1.0, 1000.0)"
    )
    conn.execute("INSERT INTO lead_times VALUES (1, 'CN', 'AIR', 7)")
    conn.execute("INSERT INTO lead_time_overrides VALUES (1, 1, 'AIR', 3)")
    conn.execute(
        "INSERT INTO rates VALUES (1, 'AIR', 'AIR', 'per_container', 1000, NULL, 0, 0)"
    )
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
    conn.execute("INSERT INTO equipment_presets VALUES (2, 'DRY_STD', 'OCEAN', 12, 2.3, 2.3, 26000)")
    conn.execute("INSERT INTO equipment_presets VALUES (3, '40HC_DRY', 'OCEAN', 12.03, 2.352, 2.698, 26540)")
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
    conn.execute("INSERT INTO equipment_presets VALUES (1, 'BROKEN', 'AIR', 10.0, 1.0, 1.0, 0.0)")
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
