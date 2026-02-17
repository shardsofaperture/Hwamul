"""Database layer for the logistics planner app.

Uses SQLite with lightweight startup migrations.
"""
from __future__ import annotations

import os
import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd

_env_db_path = os.getenv("PLANNER_DB_PATH")
if _env_db_path:
    DB_PATH = Path(_env_db_path).expanduser().resolve()
else:
    DB_PATH = (Path(__file__).resolve().parent / "planner.db").resolve()


MIGRATIONS: list[tuple[int, str | Callable[[sqlite3.Connection], None]]] = [
    (
        1,
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS equipment_presets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            mode TEXT NOT NULL,
            length_m REAL,
            width_m REAL,
            height_m REAL,
            max_payload_kg REAL,
            volumetric_factor REAL,
            optional_constraints TEXT
        );

        CREATE TABLE IF NOT EXISTS lead_times (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_of_origin TEXT NOT NULL,
            mode TEXT NOT NULL,
            lead_days INTEGER NOT NULL,
            UNIQUE(country_of_origin, mode)
        );

        CREATE TABLE IF NOT EXISTS lead_time_overrides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT NOT NULL,
            mode TEXT NOT NULL,
            lead_days INTEGER NOT NULL,
            UNIQUE(sku, mode)
        );

        CREATE TABLE IF NOT EXISTS rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mode TEXT NOT NULL,
            equipment_name TEXT,
            pricing_model TEXT NOT NULL,
            rate_value REAL NOT NULL,
            minimum_charge REAL,
            fixed_fee REAL,
            surcharge REAL,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS sku_master (
            sku TEXT PRIMARY KEY,
            description TEXT,
            default_coo TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS packaging_rules (
            sku TEXT PRIMARY KEY,
            units_per_pack REAL NOT NULL,
            kg_per_unit REAL NOT NULL,
            pack_tare_kg REAL NOT NULL,
            pack_length_m REAL NOT NULL,
            pack_width_m REAL NOT NULL,
            pack_height_m REAL NOT NULL,
            min_order_packs INTEGER NOT NULL DEFAULT 1,
            increment_packs INTEGER NOT NULL DEFAULT 1,
            stackable INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (sku) REFERENCES sku_master(sku)
        );

        CREATE TABLE IF NOT EXISTS demand_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT NOT NULL,
            need_date TEXT NOT NULL,
            qty REAL NOT NULL,
            coo_override TEXT,
            priority TEXT,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS tranche_allocations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            demand_line_id INTEGER NOT NULL,
            tranche_name TEXT NOT NULL,
            allocation_type TEXT NOT NULL,
            allocation_value REAL NOT NULL,
            manual_lead_override INTEGER,
            manual_mode_override TEXT,
            FOREIGN KEY (demand_line_id) REFERENCES demand_lines(id)
        );
        """,
    ),
    (
        2,
        """
        ALTER TABLE rates RENAME TO rates_old;
        CREATE TABLE rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin TEXT,
            destination TEXT,
            mode TEXT NOT NULL,
            equipment_name TEXT,
            pricing_model TEXT NOT NULL,
            rate_value REAL NOT NULL,
            minimum_charge REAL,
            fixed_fee REAL,
            surcharge REAL,
            effective_start TEXT,
            effective_end TEXT,
            notes TEXT
        );
        INSERT INTO rates (id, mode, equipment_name, pricing_model, rate_value, minimum_charge, fixed_fee, surcharge, notes)
        SELECT id, mode, equipment_name, pricing_model, rate_value, minimum_charge, fixed_fee, surcharge, notes FROM rates_old;
        DROP TABLE rates_old;

        ALTER TABLE packaging_rules RENAME TO packaging_rules_old;
        CREATE TABLE packaging_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT NOT NULL,
            pack_type TEXT NOT NULL DEFAULT 'STANDARD',
            is_default INTEGER NOT NULL DEFAULT 1,
            units_per_pack REAL NOT NULL,
            kg_per_unit REAL NOT NULL,
            pack_tare_kg REAL NOT NULL,
            pack_length_m REAL NOT NULL,
            pack_width_m REAL NOT NULL,
            pack_height_m REAL NOT NULL,
            min_order_packs INTEGER NOT NULL DEFAULT 1,
            increment_packs INTEGER NOT NULL DEFAULT 1,
            stackable INTEGER NOT NULL DEFAULT 1,
            UNIQUE(sku, pack_type),
            FOREIGN KEY (sku) REFERENCES sku_master(sku)
        );
        INSERT INTO packaging_rules (
            sku, pack_type, is_default, units_per_pack, kg_per_unit, pack_tare_kg,
            pack_length_m, pack_width_m, pack_height_m, min_order_packs, increment_packs, stackable
        )
        SELECT sku, 'STANDARD', 1, units_per_pack, kg_per_unit, pack_tare_kg,
               pack_length_m, pack_width_m, pack_height_m, min_order_packs, increment_packs, stackable
        FROM packaging_rules_old;
        DROP TABLE packaging_rules_old;
        """,
    ),
    (
        3,
        """
        CREATE TABLE IF NOT EXISTS carrier (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS rate_card (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            carrier_id INTEGER,
            mode TEXT NOT NULL,
            service_scope TEXT NOT NULL,
            equipment TEXT NOT NULL,
            dim_class TEXT NOT NULL,
            origin_type TEXT NOT NULL,
            origin_code TEXT NOT NULL,
            dest_type TEXT NOT NULL,
            dest_code TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            uom_pricing TEXT NOT NULL,
            base_rate REAL NOT NULL,
            min_charge REAL,
            effective_from TEXT NOT NULL,
            effective_to TEXT,
            contract_start TEXT,
            contract_end TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 0,
            notes TEXT,
            FOREIGN KEY (carrier_id) REFERENCES carrier(id)
        );

        CREATE TABLE IF NOT EXISTS rate_charge (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rate_card_id INTEGER NOT NULL,
            charge_code TEXT NOT NULL,
            charge_name TEXT NOT NULL,
            calc_method TEXT NOT NULL,
            amount REAL NOT NULL,
            min_amount REAL,
            max_amount REAL,
            applies_when TEXT NOT NULL DEFAULT 'ALWAYS',
            effective_from TEXT,
            effective_to TEXT,
            FOREIGN KEY (rate_card_id) REFERENCES rate_card(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_rate_card_lookup
            ON rate_card(mode, equipment, service_scope, origin_type, origin_code, dest_type, dest_code, effective_from, effective_to, is_active, priority);
        CREATE INDEX IF NOT EXISTS idx_rate_charge_rate_card ON rate_charge(rate_card_id);
        """,
    ),
    (
        4,
        """
        PRAGMA foreign_keys=OFF;

        ALTER TABLE sku_master RENAME TO sku_master_old;
        CREATE TABLE sku_master (
            part_number TEXT PRIMARY KEY,
            description TEXT,
            default_coo TEXT NOT NULL
        );
        INSERT INTO sku_master (part_number, description, default_coo)
        SELECT sku, description, default_coo
        FROM sku_master_old;
        DROP TABLE sku_master_old;

        ALTER TABLE packaging_rules RENAME TO packaging_rules_old;
        CREATE TABLE packaging_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_number TEXT NOT NULL,
            pack_type TEXT NOT NULL DEFAULT 'STANDARD',
            is_default INTEGER NOT NULL DEFAULT 1,
            units_per_pack REAL NOT NULL,
            kg_per_unit REAL NOT NULL,
            pack_tare_kg REAL NOT NULL,
            pack_length_m REAL NOT NULL,
            pack_width_m REAL NOT NULL,
            pack_height_m REAL NOT NULL,
            min_order_packs INTEGER NOT NULL DEFAULT 1,
            increment_packs INTEGER NOT NULL DEFAULT 1,
            stackable INTEGER NOT NULL DEFAULT 1,
            UNIQUE(part_number, pack_type),
            FOREIGN KEY (part_number) REFERENCES sku_master(part_number)
        );
        INSERT INTO packaging_rules (
            id,
            part_number,
            pack_type,
            is_default,
            units_per_pack,
            kg_per_unit,
            pack_tare_kg,
            pack_length_m,
            pack_width_m,
            pack_height_m,
            min_order_packs,
            increment_packs,
            stackable
        )
        SELECT
            id,
            sku,
            pack_type,
            is_default,
            units_per_pack,
            kg_per_unit,
            pack_tare_kg,
            pack_length_m,
            pack_width_m,
            pack_height_m,
            min_order_packs,
            increment_packs,
            stackable
        FROM packaging_rules_old;
        DROP TABLE packaging_rules_old;

        ALTER TABLE demand_lines RENAME TO demand_lines_old;
        CREATE TABLE demand_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_number TEXT NOT NULL,
            need_date TEXT NOT NULL,
            qty REAL NOT NULL,
            coo_override TEXT,
            priority TEXT,
            notes TEXT
        );
        INSERT INTO demand_lines (id, part_number, need_date, qty, coo_override, priority, notes)
        SELECT id, sku, need_date, qty, coo_override, priority, notes
        FROM demand_lines_old;
        DROP TABLE demand_lines_old;

        ALTER TABLE lead_time_overrides RENAME TO lead_time_overrides_old;
        CREATE TABLE lead_time_overrides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_number TEXT NOT NULL,
            mode TEXT NOT NULL,
            lead_days INTEGER NOT NULL,
            UNIQUE(part_number, mode)
        );
        INSERT INTO lead_time_overrides (id, part_number, mode, lead_days)
        SELECT id, sku, mode, lead_days
        FROM lead_time_overrides_old;
        DROP TABLE lead_time_overrides_old;

        PRAGMA foreign_keys=ON;
        """,
    ),
    (
        5,
        """
        PRAGMA foreign_keys=OFF;

        ALTER TABLE tranche_allocations RENAME TO tranche_allocations_old;
        CREATE TABLE tranche_allocations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            demand_line_id INTEGER NOT NULL,
            tranche_name TEXT NOT NULL,
            allocation_type TEXT NOT NULL,
            allocation_value REAL NOT NULL,
            manual_lead_override INTEGER,
            manual_mode_override TEXT,
            FOREIGN KEY (demand_line_id) REFERENCES demand_lines(id)
        );
        INSERT INTO tranche_allocations (
            id,
            demand_line_id,
            tranche_name,
            allocation_type,
            allocation_value,
            manual_lead_override,
            manual_mode_override
        )
        SELECT
            id,
            demand_line_id,
            tranche_name,
            allocation_type,
            allocation_value,
            manual_lead_override,
            manual_mode_override
        FROM tranche_allocations_old;
        DROP TABLE tranche_allocations_old;

        PRAGMA foreign_keys=ON;
        """,
    ),
    (
        6,
        """
        PRAGMA foreign_keys=OFF;

        CREATE TABLE IF NOT EXISTS suppliers (
            supplier_id INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_code TEXT NOT NULL UNIQUE,
            supplier_name TEXT NOT NULL
        );
        INSERT OR IGNORE INTO suppliers (supplier_code, supplier_name)
        VALUES ('DEFAULT', 'Default Supplier');

        ALTER TABLE sku_master RENAME TO sku_master_old;
        CREATE TABLE sku_master (
            sku_id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_number TEXT NOT NULL,
            supplier_id INTEGER NOT NULL,
            description TEXT,
            default_coo TEXT NOT NULL,
            UNIQUE(part_number, supplier_id),
            FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
        );
        INSERT INTO sku_master (part_number, supplier_id, description, default_coo)
        SELECT old.part_number, s.supplier_id, old.description, old.default_coo
        FROM sku_master_old old
        CROSS JOIN suppliers s
        WHERE s.supplier_code = 'DEFAULT';
        DROP TABLE sku_master_old;

        ALTER TABLE packaging_rules RENAME TO packaging_rules_old;
        CREATE TABLE packaging_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku_id INTEGER NOT NULL,
            pack_type TEXT NOT NULL DEFAULT 'STANDARD',
            is_default INTEGER NOT NULL DEFAULT 1,
            units_per_pack REAL NOT NULL,
            kg_per_unit REAL NOT NULL,
            pack_tare_kg REAL NOT NULL,
            pack_length_m REAL NOT NULL,
            pack_width_m REAL NOT NULL,
            pack_height_m REAL NOT NULL,
            min_order_packs INTEGER NOT NULL DEFAULT 1,
            increment_packs INTEGER NOT NULL DEFAULT 1,
            stackable INTEGER NOT NULL DEFAULT 1,
            UNIQUE(sku_id, pack_type),
            FOREIGN KEY (sku_id) REFERENCES sku_master(sku_id)
        );
        INSERT INTO packaging_rules (
            id, sku_id, pack_type, is_default, units_per_pack, kg_per_unit, pack_tare_kg,
            pack_length_m, pack_width_m, pack_height_m, min_order_packs, increment_packs, stackable
        )
        SELECT
            p.id,
            s.sku_id,
            p.pack_type,
            p.is_default,
            p.units_per_pack,
            p.kg_per_unit,
            p.pack_tare_kg,
            p.pack_length_m,
            p.pack_width_m,
            p.pack_height_m,
            p.min_order_packs,
            p.increment_packs,
            p.stackable
        FROM packaging_rules_old p
        JOIN sku_master s ON s.part_number = p.part_number
        JOIN suppliers sup ON sup.supplier_id = s.supplier_id
        WHERE sup.supplier_code = 'DEFAULT';
        DROP TABLE packaging_rules_old;

        ALTER TABLE demand_lines RENAME TO demand_lines_old;
        CREATE TABLE demand_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku_id INTEGER NOT NULL,
            need_date TEXT NOT NULL,
            qty REAL NOT NULL,
            coo_override TEXT,
            priority TEXT,
            notes TEXT,
            FOREIGN KEY (sku_id) REFERENCES sku_master(sku_id)
        );
        INSERT INTO demand_lines (id, sku_id, need_date, qty, coo_override, priority, notes)
        SELECT
            d.id,
            s.sku_id,
            d.need_date,
            d.qty,
            d.coo_override,
            d.priority,
            d.notes
        FROM demand_lines_old d
        JOIN sku_master s ON s.part_number = d.part_number
        JOIN suppliers sup ON sup.supplier_id = s.supplier_id
        WHERE sup.supplier_code = 'DEFAULT';
        DROP TABLE demand_lines_old;

        ALTER TABLE lead_time_overrides RENAME TO lead_time_overrides_old;
        CREATE TABLE lead_time_overrides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku_id INTEGER NOT NULL,
            mode TEXT NOT NULL,
            lead_days INTEGER NOT NULL,
            UNIQUE(sku_id, mode),
            FOREIGN KEY (sku_id) REFERENCES sku_master(sku_id)
        );
        INSERT INTO lead_time_overrides (id, sku_id, mode, lead_days)
        SELECT
            o.id,
            s.sku_id,
            o.mode,
            o.lead_days
        FROM lead_time_overrides_old o
        JOIN sku_master s ON s.part_number = o.part_number
        JOIN suppliers sup ON sup.supplier_id = s.supplier_id
        WHERE sup.supplier_code = 'DEFAULT';
        DROP TABLE lead_time_overrides_old;

        ALTER TABLE tranche_allocations RENAME TO tranche_allocations_old;
        CREATE TABLE tranche_allocations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            demand_line_id INTEGER NOT NULL,
            tranche_name TEXT NOT NULL,
            allocation_type TEXT NOT NULL,
            allocation_value REAL NOT NULL,
            manual_lead_override INTEGER,
            manual_mode_override TEXT,
            FOREIGN KEY (demand_line_id) REFERENCES demand_lines(id)
        );
        INSERT INTO tranche_allocations (
            id, demand_line_id, tranche_name, allocation_type, allocation_value, manual_lead_override, manual_mode_override
        )
        SELECT
            id, demand_line_id, tranche_name, allocation_type, allocation_value, manual_lead_override, manual_mode_override
        FROM tranche_allocations_old;
        DROP TABLE tranche_allocations_old;

        PRAGMA foreign_keys=ON;
        """,
    ),
]


def _migration_7_packaging_rules_to_sku_fk(conn: sqlite3.Connection) -> None:
    """Normalize packaging_rules to mandatory sku_id and add lookup index.

    Handles legacy databases that still have a part_number column by backfilling
    sku_id from sku_master(part_number).
    """

    table_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='packaging_rules'"
    ).fetchone()
    if not table_exists:
        return

    cols = {
        row[1]: row
        for row in conn.execute("PRAGMA table_info(packaging_rules)").fetchall()
    }
    has_part_number = "part_number" in cols
    has_sku_id = "sku_id" in cols

    conn.execute("PRAGMA foreign_keys=OFF;")
    try:
        if has_part_number and not has_sku_id:
            conn.execute("ALTER TABLE packaging_rules ADD COLUMN sku_id INTEGER")
            has_sku_id = True

        if has_part_number and has_sku_id:
            conn.execute(
                """
                UPDATE packaging_rules
                SET sku_id = (
                    SELECT sm.sku_id
                    FROM sku_master sm
                    WHERE sm.part_number = packaging_rules.part_number
                    ORDER BY sm.sku_id
                    LIMIT 1
                )
                WHERE sku_id IS NULL
                """
            )

        if has_sku_id:
            missing = conn.execute(
                "SELECT COUNT(*) FROM packaging_rules WHERE sku_id IS NULL"
            ).fetchone()[0]
            if missing:
                raise sqlite3.IntegrityError("packaging_rules contains rows without resolvable sku_id")

            conn.executescript(
                """
                ALTER TABLE packaging_rules RENAME TO packaging_rules_old;
                CREATE TABLE packaging_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sku_id INTEGER NOT NULL,
                    pack_type TEXT NOT NULL DEFAULT 'STANDARD',
                    is_default INTEGER NOT NULL DEFAULT 1,
                    units_per_pack REAL NOT NULL,
                    kg_per_unit REAL NOT NULL,
                    pack_tare_kg REAL NOT NULL,
                    pack_length_m REAL NOT NULL,
                    pack_width_m REAL NOT NULL,
                    pack_height_m REAL NOT NULL,
                    min_order_packs INTEGER NOT NULL DEFAULT 1,
                    increment_packs INTEGER NOT NULL DEFAULT 1,
                    stackable INTEGER NOT NULL DEFAULT 1,
                    UNIQUE(sku_id, pack_type),
                    FOREIGN KEY (sku_id) REFERENCES sku_master(sku_id)
                );
                """
            )
            conn.execute(
                """
                INSERT INTO packaging_rules (
                    id, sku_id, pack_type, is_default, units_per_pack, kg_per_unit, pack_tare_kg,
                    pack_length_m, pack_width_m, pack_height_m, min_order_packs, increment_packs, stackable
                )
                SELECT
                    id, sku_id, pack_type, is_default, units_per_pack, kg_per_unit, pack_tare_kg,
                    pack_length_m, pack_width_m, pack_height_m, min_order_packs, increment_packs, stackable
                FROM packaging_rules_old
                """
            )
            conn.execute("DROP TABLE packaging_rules_old")

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_packaging_rules_sku_id ON packaging_rules(sku_id)"
        )
    finally:
        conn.execute("PRAGMA foreign_keys=ON;")


MIGRATIONS.append((7, _migration_7_packaging_rules_to_sku_fk))


def _migration_8_supplier_specific_pack_rules(conn: sqlite3.Connection) -> None:
    """Evolve pack rules for multi-variant supplier-specific workflow."""

    conn.execute("PRAGMA foreign_keys=OFF;")
    try:
        conn.executescript(
            """
            ALTER TABLE packaging_rules RENAME TO packaging_rules_old;
            CREATE TABLE packaging_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku_id INTEGER NOT NULL,
                pack_name TEXT NOT NULL,
                pack_type TEXT NOT NULL DEFAULT 'STANDARD',
                is_default INTEGER NOT NULL DEFAULT 0,
                units_per_pack REAL NOT NULL,
                kg_per_unit REAL NOT NULL,
                pack_tare_kg REAL NOT NULL,
                dim_l_m REAL NOT NULL,
                dim_w_m REAL NOT NULL,
                dim_h_m REAL NOT NULL,
                min_order_packs INTEGER NOT NULL DEFAULT 1,
                increment_packs INTEGER NOT NULL DEFAULT 1,
                stackable INTEGER NOT NULL DEFAULT 1,
                max_stack INTEGER,
                UNIQUE(sku_id, pack_name),
                FOREIGN KEY (sku_id) REFERENCES sku_master(sku_id)
            );
            """
        )
        conn.execute(
            """
            INSERT INTO packaging_rules (
                id, sku_id, pack_name, pack_type, is_default,
                units_per_pack, kg_per_unit, pack_tare_kg,
                dim_l_m, dim_w_m, dim_h_m,
                min_order_packs, increment_packs, stackable
            )
            SELECT
                id,
                sku_id,
                COALESCE(NULLIF(pack_type, ''), 'STANDARD') AS pack_name,
                COALESCE(NULLIF(pack_type, ''), 'STANDARD') AS pack_type,
                0,
                units_per_pack, kg_per_unit, pack_tare_kg,
                pack_length_m, pack_width_m, pack_height_m,
                min_order_packs, increment_packs, stackable
            FROM packaging_rules_old
            """
        )
        conn.execute("DROP TABLE packaging_rules_old")

        conn.execute(
            """
            UPDATE packaging_rules
            SET is_default = 1
            WHERE id IN (
                SELECT MIN(id)
                FROM packaging_rules
                GROUP BY sku_id
            )
            """
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_packaging_rules_single_default ON packaging_rules(sku_id) WHERE is_default = 1"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_packaging_rules_sku_id ON packaging_rules(sku_id)")

        demand_exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='demand_lines'").fetchone()
        if demand_exists:
            dl_cols = {row[1] for row in conn.execute("PRAGMA table_info(demand_lines)").fetchall()}
            if "pack_rule_id" not in dl_cols:
                conn.execute("ALTER TABLE demand_lines ADD COLUMN pack_rule_id INTEGER")
    finally:
        conn.execute("PRAGMA foreign_keys=ON;")


MIGRATIONS.append((8, _migration_8_supplier_specific_pack_rules))


def _migration_9_customs_tracking(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS customs_hts_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku_id INTEGER,
            hts_code TEXT NOT NULL,
            material_input TEXT,
            country_of_origin TEXT,
            tariff_program TEXT,
            base_duty_rate REAL NOT NULL DEFAULT 0,
            tariff_rate REAL NOT NULL DEFAULT 0,
            section_232 INTEGER NOT NULL DEFAULT 0,
            section_301 INTEGER NOT NULL DEFAULT 0,
            domestic_trucking_required INTEGER NOT NULL DEFAULT 0,
            port_to_ramp_required INTEGER NOT NULL DEFAULT 0,
            special_documentation_required INTEGER NOT NULL DEFAULT 0,
            documentation_notes TEXT,
            effective_from TEXT NOT NULL,
            effective_to TEXT,
            notes TEXT,
            UNIQUE(hts_code, country_of_origin, tariff_program, effective_from),
            FOREIGN KEY (sku_id) REFERENCES sku_master(sku_id)
        );

        CREATE INDEX IF NOT EXISTS idx_customs_hts_lookup
            ON customs_hts_rates(hts_code, country_of_origin, effective_from, effective_to);
        """
    )


MIGRATIONS.append((9, _migration_9_customs_tracking))


def _migration_10_customs_notes_and_references(conn: sqlite3.Connection) -> None:
    table_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='customs_hts_rates'"
    ).fetchone()
    if not table_exists:
        return

    cols = {row[1] for row in conn.execute("PRAGMA table_info(customs_hts_rates)").fetchall()}
    if "tariff_rate_notes" not in cols:
        conn.execute("ALTER TABLE customs_hts_rates ADD COLUMN tariff_rate_notes TEXT")
    if "documentation_url" not in cols:
        conn.execute("ALTER TABLE customs_hts_rates ADD COLUMN documentation_url TEXT")
    if "tips" not in cols:
        conn.execute("ALTER TABLE customs_hts_rates ADD COLUMN tips TEXT")


MIGRATIONS.append((10, _migration_10_customs_notes_and_references))


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _exec_script(conn: sqlite3.Connection, sql_script: str) -> None:
    conn.executescript(sql_script)


def run_migrations() -> None:
    conn = get_conn()
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        applied = {
            row["version"]
            for row in conn.execute("SELECT version FROM schema_migrations").fetchall()
        }
        for version, script in MIGRATIONS:
            if version in applied:
                continue
            if callable(script):
                script(conn)
            else:
                _exec_script(conn, script)
            conn.execute("INSERT INTO schema_migrations(version) VALUES (?)", (version,))


def insert_many(
    conn: sqlite3.Connection, query: str, rows: Iterable[tuple[object, ...]]
) -> None:
    conn.executemany(query, rows)


def compute_grid_diff(original: pd.DataFrame, edited: pd.DataFrame, key_cols: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return inserted, updated, deleted rows comparing original and edited data grids."""
    original = original.copy().fillna("")
    edited = edited.copy().fillna("")

    orig_indexed = original.set_index(key_cols, drop=False) if not original.empty else pd.DataFrame(columns=edited.columns).set_index(key_cols)
    edit_indexed = edited.set_index(key_cols, drop=False) if not edited.empty else pd.DataFrame(columns=original.columns).set_index(key_cols)

    inserts_idx = edit_indexed.index.difference(orig_indexed.index)
    deletes_idx = orig_indexed.index.difference(edit_indexed.index)
    common_idx = edit_indexed.index.intersection(orig_indexed.index)

    inserts = edit_indexed.loc[inserts_idx].reset_index(drop=True) if len(inserts_idx) else pd.DataFrame(columns=edited.columns)
    deletes = orig_indexed.loc[deletes_idx].reset_index(drop=True) if len(deletes_idx) else pd.DataFrame(columns=original.columns)

    updates = []
    for idx in common_idx:
        o = orig_indexed.loc[idx]
        e = edit_indexed.loc[idx]
        if isinstance(o, pd.DataFrame):
            o = o.iloc[0]
        if isinstance(e, pd.DataFrame):
            e = e.iloc[0]
        if not o.equals(e):
            updates.append(e.to_dict())
    updates_df = pd.DataFrame(updates, columns=edited.columns)
    return inserts, updates_df, deletes


def upsert_rows(conn: sqlite3.Connection, table: str, rows: pd.DataFrame, key_cols: list[str]) -> None:
    if rows.empty:
        return
    columns = list(rows.columns)
    placeholders = ", ".join(["?"] * len(columns))
    quoted_cols = ", ".join(columns)
    update_cols = [c for c in columns if c not in key_cols]
    update_stmt = ", ".join([f"{col}=excluded.{col}" for col in update_cols])
    sql = f"INSERT INTO {table} ({quoted_cols}) VALUES ({placeholders}) ON CONFLICT({', '.join(key_cols)}) DO UPDATE SET {update_stmt}"
    values = [tuple(None if pd.isna(v) or v == "" else v for v in row) for row in rows[columns].itertuples(index=False, name=None)]
    conn.executemany(sql, values)


def _to_native(value: object) -> object:
    return value.item() if hasattr(value, "item") else value


def delete_rows(conn: sqlite3.Connection, table: str, rows: pd.DataFrame, key_cols: list[str]) -> None:
    if rows.empty:
        return
    where = " AND ".join([f"{col} = ?" for col in key_cols])
    sql = f"DELETE FROM {table} WHERE {where}"
    params = [tuple(_to_native(row[col]) for col in key_cols) for _, row in rows.iterrows()]
    conn.executemany(sql, params)


EXPORT_TABLE_ORDER = [
    "equipment_presets",
    "suppliers",
    "sku_master",
    "packaging_rules",
    "lead_times",
    "lead_time_overrides",
    "rates",
    "carrier",
    "rate_card",
    "rate_charge",
    "customs_hts_rates",
    "demand_lines",
    "tranche_allocations",
]

TABLE_KEY_COLS: dict[str, list[str]] = {
    "equipment_presets": ["name"],
    "suppliers": ["supplier_code"],
    "sku_master": ["part_number", "supplier_id"],
    "packaging_rules": ["sku_id", "pack_name"],
    "lead_times": ["country_of_origin", "mode"],
    "lead_time_overrides": ["sku_id", "mode"],
    "rates": ["mode", "pricing_model", "origin", "destination", "equipment_name", "effective_start", "effective_end"],
    "carrier": ["code"],
    "rate_card": ["mode", "service_scope", "equipment", "origin_type", "origin_code", "dest_type", "dest_code", "effective_from", "effective_to", "carrier_id"],
    "rate_charge": ["rate_card_id", "charge_code", "charge_name", "calc_method", "effective_from", "effective_to"],
    "customs_hts_rates": ["hts_code", "country_of_origin", "tariff_program", "effective_from"],
    "demand_lines": ["sku_id", "need_date", "qty", "notes"],
    "tranche_allocations": ["demand_line_id", "tranche_name", "allocation_type"],
}


def _query_map_for_profile(profile: str) -> dict[str, str]:
    today = date.today().isoformat()
    if profile == "full":
        return {table: f"SELECT * FROM {table}" for table in EXPORT_TABLE_ORDER}
    if profile == "recent":
        return {
            "equipment_presets": "SELECT * FROM equipment_presets",
            "suppliers": "SELECT * FROM suppliers",
            "sku_master": "SELECT * FROM sku_master",
            "packaging_rules": "SELECT * FROM packaging_rules",
            "lead_times": "SELECT * FROM lead_times",
            "lead_time_overrides": "SELECT * FROM lead_time_overrides",
            "rates": (
                "SELECT * FROM rates "
                "WHERE (effective_start IS NULL OR effective_start = '' OR effective_start <= :today) "
                "AND (effective_end IS NULL OR effective_end = '' OR effective_end >= :today)"
            ),
            "customs_hts_rates": (
                "SELECT * FROM customs_hts_rates "
                "WHERE effective_from <= :today "
                "AND (effective_to IS NULL OR effective_to = '' OR effective_to >= :today)"
            ),
        }
    if profile == "history":
        return {
            "demand_lines": "SELECT * FROM demand_lines",
            "tranche_allocations": "SELECT * FROM tranche_allocations",
            "rates": (
                "SELECT * FROM rates "
                "WHERE effective_end IS NOT NULL AND effective_end <> '' AND effective_end < :today"
            ),
            "customs_hts_rates": (
                "SELECT * FROM customs_hts_rates "
                "WHERE effective_to IS NOT NULL AND effective_to <> '' AND effective_to < :today"
            ),
        }
    raise ValueError(f"Unknown export profile: {profile}")


def export_data_bundle(profile: str = "full") -> bytes:
    conn = get_conn()
    payload: dict[str, object] = {
        "format": "hwamul.export.v1",
        "profile": profile,
        "exported_at": pd.Timestamp.utcnow().isoformat(),
        "tables": {},
    }
    queries = _query_map_for_profile(profile)
    for table, query in queries.items():
        df = pd.read_sql_query(query, conn, params={"today": date.today().isoformat()})
        payload["tables"][table] = df.where(pd.notna(df), None).to_dict("records")
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def import_data_bundle(blob: bytes) -> dict[str, int]:
    package = json.loads(blob.decode("utf-8"))
    if package.get("format") != "hwamul.export.v1":
        raise ValueError("Unsupported bundle format")
    tables = package.get("tables", {})
    if not isinstance(tables, dict):
        raise ValueError("Bundle tables payload must be a dict")

    conn = get_conn()
    stats: dict[str, int] = {}
    with conn:
        for table in EXPORT_TABLE_ORDER:
            rows = tables.get(table)
            if not rows:
                continue
            frame = pd.DataFrame(rows)
            if frame.empty:
                continue
            key_cols = ["id"] if "id" in frame.columns else TABLE_KEY_COLS.get(table)
            if not key_cols:
                continue
            upsert_rows(conn, table, frame, key_cols)
            stats[table] = len(frame)
    return stats


def purge_demand_before(cutoff_date: str) -> dict[str, int]:
    conn = get_conn()
    with conn:
        demand_ids = [r[0] for r in conn.execute("SELECT id FROM demand_lines WHERE need_date < ?", (cutoff_date,)).fetchall()]
        if demand_ids:
            placeholders = ",".join(["?"] * len(demand_ids))
            deleted_allocs = conn.execute(
                f"DELETE FROM tranche_allocations WHERE demand_line_id IN ({placeholders})",
                demand_ids,
            ).rowcount
            deleted_demand = conn.execute(
                "DELETE FROM demand_lines WHERE need_date < ?",
                (cutoff_date,),
            ).rowcount
        else:
            deleted_allocs = 0
            deleted_demand = 0
    return {"demand_lines": deleted_demand, "tranche_allocations": deleted_allocs}


def vacuum_db() -> None:
    conn = get_conn()
    conn.execute("VACUUM")


def select_default_pack_rule(conn: sqlite3.Connection, sku_id: int) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT * FROM packaging_rules
        WHERE sku_id = ?
        ORDER BY is_default DESC, id ASC
        LIMIT 1
        """,
        (sku_id,),
    ).fetchone()


def resolve_pack_rule_for_demand(conn: sqlite3.Connection, demand_line: sqlite3.Row) -> sqlite3.Row | None:
    if demand_line["pack_rule_id"] is not None:
        row = conn.execute(
            "SELECT * FROM packaging_rules WHERE id = ? AND sku_id = ?",
            (demand_line["pack_rule_id"], demand_line["sku_id"]),
        ).fetchone()
        if row is not None:
            return row
    return select_default_pack_rule(conn, int(demand_line["sku_id"]))


def map_import_demand_rows(
    import_frame: pd.DataFrame,
    sku_catalog: pd.DataFrame,
    supplier_choice_by_part: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    supplier_choice_by_part = supplier_choice_by_part or {}
    frame = import_frame.copy()
    errors: list[str] = []

    if "supplier_code" in frame.columns:
        merged = frame.merge(
            sku_catalog[["sku_id", "part_number", "supplier_code"]],
            on=["part_number", "supplier_code"],
            how="left",
        )
        missing = merged[merged["sku_id"].isna()]
        if not missing.empty:
            errors.append("Some rows did not map to sku_id from part_number + supplier_code")
        return merged, errors

    merged = frame.merge(
        sku_catalog[["sku_id", "part_number", "supplier_code"]],
        on=["part_number"],
        how="left",
    )
    candidate_counts = merged.groupby("part_number")["sku_id"].nunique(dropna=True)
    ambiguous_parts = candidate_counts[candidate_counts > 1].index.tolist()

    for part_number in ambiguous_parts:
        selected_supplier = supplier_choice_by_part.get(part_number)
        if not selected_supplier:
            errors.append(f"Supplier selection required for part_number={part_number}")
            continue
        mask = (merged["part_number"] == part_number) & (merged["supplier_code"] == selected_supplier)
        sku_values = merged.loc[mask, "sku_id"].dropna().unique().tolist()
        if not sku_values:
            errors.append(f"Invalid supplier mapping for part_number={part_number}")
            continue
        merged.loc[merged["part_number"] == part_number, "sku_id"] = sku_values[0]

    if merged["sku_id"].isna().any():
        errors.append("Some rows are still unmapped to sku_id")

    return merged, errors
