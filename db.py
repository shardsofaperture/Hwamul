"""Database layer for the logistics planner app.

Uses SQLite with lightweight startup migrations.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd

_env_db_path = os.getenv("PLANNER_DB_PATH")
if _env_db_path:
    DB_PATH = Path(_env_db_path).expanduser().resolve()
else:
    DB_PATH = (Path(__file__).resolve().parent / "planner.db").resolve()


MIGRATIONS: list[tuple[int, str]] = [
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
]


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


def delete_rows(conn: sqlite3.Connection, table: str, rows: pd.DataFrame, key_cols: list[str]) -> None:
    if rows.empty:
        return
    where = " AND ".join([f"{col} = ?" for col in key_cols])
    sql = f"DELETE FROM {table} WHERE {where}"
    params = [tuple(row[col] for col in key_cols) for _, row in rows.iterrows()]
    conn.executemany(sql, params)
