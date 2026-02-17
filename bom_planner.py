from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from math import ceil
import io
import sqlite3
from typing import Any

import pandas as pd

from batch_planner import plan_trucks_mix_ok
from fit_engine import equipment_capacity, pack_gross_kg, pack_volume_m3, packs_per_equipment, required_packs_for_kg
from planner import norm_mode, norm_equipment_code

REQUIRED_COLUMNS = ["phase_name", "need_date", "part_number", "required_kg"]
OPTIONAL_COLUMNS = [
    "coo_override",
    "priority",
    "notes",
    "allocation_mode",
    "allocation_value",
    "allocation_target_mode",
    "equipment_preference",
]


@dataclass
class BomPlanningPolicy:
    container_policy: str = "NO_MIX"
    truck_policy: str = "MIX_OK"
    allow_stacking_in_trucks: bool = False
    default_ocean_equipment: str = "CNT_40_DRY_HC"
    default_truck_equipment: str = "TRL_53_STD"


def read_bom_upload(file_name: str, blob: bytes) -> pd.DataFrame:
    lower = file_name.lower()
    if lower.endswith(".csv"):
        return pd.read_csv(io.BytesIO(blob))
    if lower.endswith(".xlsx"):
        return pd.read_excel(io.BytesIO(blob))
    raise ValueError("Unsupported file format. Upload CSV or XLSX.")


def _normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    rename = {c: c.strip().lower() for c in frame.columns}
    df = frame.rename(columns=rename).copy()
    if "sku" in df.columns and "part_number" not in df.columns:
        df = df.rename(columns={"sku": "part_number"})
    return df


def validate_bom_frame(conn: sqlite3.Connection, frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str], list[str]]:
    df = _normalize_columns(frame)
    errors: list[str] = []
    warnings: list[str] = []

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        return df, [f"Missing required columns: {', '.join(missing)}"], warnings

    for col in OPTIONAL_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df["required_kg"] = pd.to_numeric(df["required_kg"], errors="coerce")
    bad_kg = df[df["required_kg"].isna() | (df["required_kg"] <= 0)]
    for idx in bad_kg.index:
        errors.append(f"Row {idx + 1}: required_kg must be > 0")

    parsed = pd.to_datetime(df["need_date"], errors="coerce")
    bad_date = df[parsed.isna()]
    for idx in bad_date.index:
        errors.append(f"Row {idx + 1}: invalid need_date")
    df["need_date"] = parsed.dt.date.astype("string")

    sku_map = pd.read_sql_query("SELECT sku_id, part_number, default_coo FROM sku_master", conn)
    merged = df.merge(sku_map, on="part_number", how="left")
    missing_sku = merged[merged["sku_id"].isna()]
    for idx, row in missing_sku.iterrows():
        errors.append(f"Row {idx + 1}: unknown part_number {row['part_number']}")

    pack_rows = pd.read_sql_query(
        "SELECT sku_id FROM packaging_rules WHERE is_default = 1 GROUP BY sku_id",
        conn,
    )
    has_pack = set(pack_rows["sku_id"].tolist())
    for idx, row in merged.iterrows():
        if pd.notna(row.get("sku_id")) and int(row["sku_id"]) not in has_pack:
            warnings.append(f"Row {idx + 1}: missing default pack rule for sku_id={int(row['sku_id'])}")

    return merged, errors, warnings


def create_bom_run(conn: sqlite3.Connection, name: str, validated_rows: pd.DataFrame) -> int:
    with conn:
        cur = conn.execute(
            "INSERT INTO bom_runs(name, created_at) VALUES (?, ?)",
            (name, datetime.utcnow().isoformat(timespec="seconds")),
        )
        bom_run_id = int(cur.lastrowid)
        rows = []
        for _, r in validated_rows.iterrows():
            rows.append(
                (
                    bom_run_id,
                    str(r["phase_name"]),
                    str(r["need_date"]),
                    int(r["sku_id"]),
                    float(r["required_kg"]),
                    r.get("coo_override"),
                    int(r["priority"]) if pd.notna(r.get("priority")) and str(r.get("priority")).strip() != "" else None,
                    r.get("notes"),
                    (str(r.get("allocation_mode") or "NONE").upper()),
                    float(r.get("allocation_value")) if pd.notna(r.get("allocation_value")) else None,
                    norm_mode(r.get("allocation_target_mode")),
                    norm_equipment_code(r.get("equipment_preference")),
                )
            )
        conn.executemany(
            """
            INSERT INTO bom_lines(
                bom_run_id, phase_name, need_date, sku_id, required_kg,
                coo_override, priority, notes,
                allocation_mode, allocation_value, allocation_target_mode,
                equipment_preference
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return bom_run_id


def _default_pack_rule(conn: sqlite3.Connection, sku_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM packaging_rules WHERE sku_id = ? ORDER BY is_default DESC, id ASC LIMIT 1",
        (sku_id,),
    ).fetchone()


def generate_pack_plan(conn: sqlite3.Connection, bom_run_id: int) -> pd.DataFrame:
    lines = conn.execute("SELECT * FROM bom_lines WHERE bom_run_id = ?", (bom_run_id,)).fetchall()
    out = []
    with conn:
        conn.execute("DELETE FROM pack_plan_lines WHERE bom_run_id = ?", (bom_run_id,))
        for line in lines:
            pack_rule = _default_pack_rule(conn, int(line["sku_id"]))
            if not pack_rule:
                continue
            qty = required_packs_for_kg(float(line["required_kg"]), pack_rule)
            out.append(
                {
                    "bom_run_id": bom_run_id,
                    "phase_name": line["phase_name"],
                    "need_date": line["need_date"],
                    "sku_id": int(line["sku_id"]),
                    "required_kg": float(line["required_kg"]),
                    "shipped_kg": float(qty["shipped_kg"]),
                    "excess_kg": float(qty["excess_kg"]),
                    "packs_required": int(qty["packs_required"]),
                    "pack_rule_id": int(pack_rule["id"]),
                }
            )
        conn.executemany(
            """
            INSERT INTO pack_plan_lines(
              bom_run_id, phase_name, need_date, sku_id, required_kg,
              shipped_kg, excess_kg, packs_required, pack_rule_id
            ) VALUES (:bom_run_id, :phase_name, :need_date, :sku_id, :required_kg,
                      :shipped_kg, :excess_kg, :packs_required, :pack_rule_id)
            """,
            out,
        )
    return pd.DataFrame(out)


def _allowed_equipment(conn: sqlite3.Connection, sku_id: int, mode: str) -> list[sqlite3.Row]:
    mode = norm_mode(mode)
    all_rows = conn.execute("SELECT * FROM equipment_presets WHERE active = 1 AND UPPER(mode)=?", (mode,)).fetchall()
    restrict = conn.execute(
        """
        SELECT ser.equipment_id, ser.allowed
        FROM sku_equipment_rules ser
        JOIN equipment_presets ep ON ep.id = ser.equipment_id
        WHERE ser.sku_id = ? AND ep.active = 1 AND UPPER(ep.mode)=?
        """,
        (sku_id, mode),
    ).fetchall()
    if not restrict:
        return all_rows
    allowed_ids = {int(r["equipment_id"]) for r in restrict if int(r["allowed"]) == 1}
    return [r for r in all_rows if int(r["id"]) in allowed_ids]


def generate_container_plan(conn: sqlite3.Connection, bom_run_id: int, policy: BomPlanningPolicy) -> pd.DataFrame:
    plan = pd.read_sql_query("SELECT * FROM pack_plan_lines WHERE bom_run_id = ?", conn, params=(bom_run_id,))
    if plan.empty:
        return plan
    out: list[dict[str, Any]] = []
    with conn:
        conn.execute("DELETE FROM container_plan_lines WHERE bom_run_id = ?", (bom_run_id,))
        grouped = plan.groupby(["phase_name", "need_date", "sku_id"], as_index=False)["packs_required"].sum()
        for _, row in grouped.iterrows():
            sku_id = int(row["sku_id"])
            bom_pref = conn.execute(
                "SELECT equipment_preference FROM bom_lines WHERE bom_run_id=? AND phase_name=? AND need_date=? AND sku_id=? AND equipment_preference IS NOT NULL AND TRIM(equipment_preference)<>'' LIMIT 1",
                (bom_run_id, row["phase_name"], row["need_date"], sku_id),
            ).fetchone()
            candidates = _allowed_equipment(conn, sku_id, "OCEAN")
            if not candidates:
                continue
            preferred = norm_equipment_code(bom_pref["equipment_preference"]) if bom_pref else ""
            selected = None
            if preferred:
                selected = next((c for c in candidates if norm_equipment_code(c["equipment_code"]) == preferred), None)
            if not selected:
                selected = next((c for c in candidates if norm_equipment_code(c["equipment_code"]) == norm_equipment_code(policy.default_ocean_equipment)), candidates[0])
            pack_rule = _default_pack_rule(conn, sku_id)
            if not pack_rule:
                continue
            try:
                fit = packs_per_equipment(pack_rule, selected)
                caps = equipment_capacity(selected)
            except ValueError:
                continue
            packs_fit = int(fit["packs_fit"])
            if packs_fit <= 0:
                continue
            packs_req = int(row["packs_required"])
            containers = int(ceil(packs_req / packs_fit))
            pweight = pack_gross_kg(pack_rule)
            pvol = pack_volume_m3(pack_rule)
            out.append(
                {
                    "bom_run_id": bom_run_id,
                    "phase_name": row["phase_name"],
                    "need_date": row["need_date"],
                    "sku_id": sku_id,
                    "equipment_code": selected["equipment_code"],
                    "packs_fit": packs_fit,
                    "containers_needed": containers,
                    "cube_util": (packs_req * pvol) / (containers * caps["eq_volume_m3"]),
                    "weight_util": (packs_req * pweight) / (containers * caps["max_payload_kg"]),
                    "limiting_constraint": fit["limiting_constraint"],
                }
            )
        conn.executemany(
            """
            INSERT INTO container_plan_lines(
              bom_run_id, phase_name, need_date, sku_id, equipment_code,
              packs_fit, containers_needed, cube_util, weight_util, limiting_constraint
            ) VALUES (:bom_run_id, :phase_name, :need_date, :sku_id, :equipment_code,
              :packs_fit, :containers_needed, :cube_util, :weight_util, :limiting_constraint)
            """,
            out,
        )
    return pd.DataFrame(out)


def generate_truck_plan(conn: sqlite3.Connection, bom_run_id: int, policy: BomPlanningPolicy) -> pd.DataFrame:
    rows = pd.read_sql_query(
        """
        SELECT p.phase_name, p.need_date, p.sku_id, SUM(p.packs_required) packs_required,
               pr.*
        FROM pack_plan_lines p
        JOIN packaging_rules pr ON pr.id = p.pack_rule_id
        WHERE p.bom_run_id = ?
        GROUP BY p.phase_name, p.need_date, p.sku_id
        """,
        conn,
        params=(bom_run_id,),
    )
    if rows.empty:
        return rows
    truck_eq = conn.execute(
        "SELECT * FROM equipment_presets WHERE active = 1 AND UPPER(TRIM(equipment_code)) = UPPER(TRIM(?))",
        (policy.default_truck_equipment,),
    ).fetchone()
    if not truck_eq:
        raise ValueError("Default truck equipment not found")

    run_rows: list[dict[str, Any]] = []
    truck_rows: list[dict[str, Any]] = []
    truck_item_rows: list[dict[str, Any]] = []

    with conn:
        conn.execute("DELETE FROM truck_plan_runs WHERE bom_run_id = ?", (bom_run_id,))
        conn.execute("DELETE FROM truck_plan_trucks WHERE bom_run_id = ?", (bom_run_id,))
        conn.execute("DELETE FROM truck_plan_truck_items WHERE bom_run_id = ?", (bom_run_id,))

        for (phase_name, need_date), g in rows.groupby(["phase_name", "need_date"]):
            reqs = []
            for _, r in g.iterrows():
                reqs.append(
                    {
                        "sku_id": int(r["sku_id"]),
                        "required_kg": float(r["packs_required"]) * float(r["units_per_pack"]) * float(r["kg_per_unit"]),
                        "pack_rule": r.to_dict(),
                    }
                )
            result = plan_trucks_mix_ok(
                reqs,
                dict(truck_eq),
                allow_stacking_in_trucks=policy.allow_stacking_in_trucks,
                use_floor_area=True,
            )
            run_rows.append(
                {
                    "bom_run_id": bom_run_id,
                    "phase_name": phase_name,
                    "need_date": need_date,
                    "equipment_code": truck_eq["equipment_code"],
                    "truck_count": int(result["truck_count"]),
                    "weight_util": float(result["weight_util"]),
                    "volume_util": float(result["volume_util"]),
                }
            )
            for t in result["trucks"]:
                truck_rows.append(
                    {
                        "bom_run_id": bom_run_id,
                        "phase_name": phase_name,
                        "need_date": need_date,
                        "truck_index": int(t["truck_id"]),
                        "total_weight": float(t["total_weight"]),
                        "total_volume": float(t["total_volume"]),
                    }
                )
                for sku_id, packs in t["sku_breakdown"].items():
                    truck_item_rows.append(
                        {
                            "bom_run_id": bom_run_id,
                            "phase_name": phase_name,
                            "need_date": need_date,
                            "truck_index": int(t["truck_id"]),
                            "sku_id": int(sku_id),
                            "packs_loaded": int(packs),
                        }
                    )
        conn.executemany(
            """
            INSERT INTO truck_plan_runs(bom_run_id, phase_name, need_date, equipment_code, truck_count, weight_util, volume_util)
            VALUES (:bom_run_id, :phase_name, :need_date, :equipment_code, :truck_count, :weight_util, :volume_util)
            """,
            run_rows,
        )
        conn.executemany(
            """
            INSERT INTO truck_plan_trucks(bom_run_id, phase_name, need_date, truck_index, total_weight, total_volume)
            VALUES (:bom_run_id, :phase_name, :need_date, :truck_index, :total_weight, :total_volume)
            """,
            truck_rows,
        )
        conn.executemany(
            """
            INSERT INTO truck_plan_truck_items(bom_run_id, phase_name, need_date, truck_index, sku_id, packs_loaded)
            VALUES (:bom_run_id, :phase_name, :need_date, :truck_index, :sku_id, :packs_loaded)
            """,
            truck_item_rows,
        )
    return pd.DataFrame(run_rows)


def generate_schedule_summary(conn: sqlite3.Connection, bom_run_id: int) -> pd.DataFrame:
    lines = conn.execute(
        """
        SELECT b.*, sm.default_coo
        FROM bom_lines b
        JOIN sku_master sm ON sm.sku_id = b.sku_id
        WHERE b.bom_run_id = ?
        """,
        (bom_run_id,),
    ).fetchall()
    out = []
    with conn:
        conn.execute("DELETE FROM schedule_summary WHERE bom_run_id = ?", (bom_run_id,))
        for line in lines:
            mode_targets = {"OCEAN", "AIR", "TRUCK"}
            mode = norm_mode(line["allocation_target_mode"])
            if mode:
                mode_targets.add(mode)
            coo = (line["coo_override"] or line["default_coo"] or "").upper()
            need_date = datetime.fromisoformat(line["need_date"]).date()
            for m in sorted(mode_targets):
                over = conn.execute(
                    "SELECT lead_days FROM lead_time_overrides WHERE sku_id = ? AND UPPER(mode)=?",
                    (int(line["sku_id"]), m),
                ).fetchone()
                if over:
                    lead = int(over[0])
                else:
                    base = conn.execute(
                        "SELECT lead_days FROM lead_times WHERE UPPER(country_of_origin)=? AND UPPER(mode)=?",
                        (coo, m),
                    ).fetchone()
                    lead = int(base[0]) if base else 999
                out.append(
                    {
                        "bom_run_id": bom_run_id,
                        "phase_name": line["phase_name"],
                        "need_date": line["need_date"],
                        "sku_id": int(line["sku_id"]),
                        "mode": m,
                        "lead_days": lead,
                        "ship_by_date": (need_date - timedelta(days=lead)).isoformat(),
                    }
                )
        conn.executemany(
            """
            INSERT INTO schedule_summary(bom_run_id, phase_name, need_date, sku_id, mode, lead_days, ship_by_date)
            VALUES (:bom_run_id, :phase_name, :need_date, :sku_id, :mode, :lead_days, :ship_by_date)
            """,
            out,
        )
    return pd.DataFrame(out)
