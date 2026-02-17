"""Database layer for the logistics planner app.

Uses SQLite with lightweight startup migrations.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Iterable

DB_PATH = Path(os.getenv("PLANNER_DB_PATH", "planner.db"))


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
    )
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
