from __future__ import annotations

import io

import pandas as pd
import streamlit as st

from batch_planner import plan_containers_no_mix, plan_trucks_mix_ok, plan_trucks_no_mix
from db import get_conn, get_equipment_by_code, get_pack_rules_for_sku, map_import_demand_rows, run_migrations
from seed import seed_if_empty
from field_specs import TABLE_SPECS
from validators import validate_dates, validate_with_specs
from pathlib import Path

st.set_page_config(page_title="Batch Plan", layout="wide")
run_migrations()
seed_if_empty()
conn = get_conn()



def render_demand_upload_box(conn) -> None:
    with st.expander("Upload data to fill this page", expanded=False):
        demand_template = Path("templates") / "demand_template.csv"
        if demand_template.exists():
            st.download_button("Download demand_template.csv", data=demand_template.read_bytes(), file_name="demand_template.csv", mime="text/csv", key="batch_demand_template")
        upload = st.file_uploader("Upload demand csv", type=["csv"], key="batch_demand_upload")
        if upload is None:
            return
        frame = pd.read_csv(upload)
        edited = st.data_editor(frame, num_rows="dynamic", width="stretch", key="batch_demand_editor")
        if st.button("Append uploaded demand", key="batch_append_demand"):
            required = [name for name, spec in TABLE_SPECS["demand_import"].items() if spec.required]
            missing = [col for col in required if col not in edited.columns]
            if missing:
                st.error("Missing required columns: " + ", ".join(missing))
                st.stop()
            errors = validate_with_specs("demand_import", edited) + validate_dates(edited, ["need_date"])
            if errors:
                st.error("; ".join(errors))
                st.stop()
            sku_catalog = pd.read_sql_query(
                """SELECT sm.sku_id, sm.part_number, s.supplier_code FROM sku_master sm JOIN suppliers s ON s.supplier_id = sm.supplier_id""",
                conn,
            )
            merged, map_errors = map_import_demand_rows(edited, sku_catalog, {})
            if map_errors:
                st.error("; ".join(map_errors))
                st.stop()
            to_insert = merged[["sku_id", "need_date", "qty", "coo_override", "priority", "notes", "phase", "mode_override", "service_scope", "miles"]]
            with conn:
                to_insert.to_sql("demand_lines", conn, if_exists="append", index=False)
            st.success(f"Appended {len(to_insert)} demand rows")

st.title("Batch Plan")

if hasattr(st, "page_link"):
    nav = st.container(border=True)
    nav.caption("Navigation")
    c1, c2, c3, c4 = nav.columns(4)
    c1.page_link("app.py", label="Main Planner", icon="🏠")
    c2.page_link("pages/quick_plan.py", label="Quick Plan", icon="📦")
    c3.page_link("pages/batch_plan.py", label="Batch Plan", icon="🚚")
    c4.page_link("pages/bom_planner.py", label="BOM Planner", icon="🧾")

render_demand_upload_box(conn)

sku_catalog = pd.read_sql_query(
    """
    SELECT sm.sku_id, sm.part_number, s.supplier_code
    FROM sku_master sm
    JOIN suppliers s ON s.supplier_id = sm.supplier_id
    """,
    conn,
)

paste_text = st.text_area(
    "Paste CSV rows (sku or part_number, required_kg)",
    height=180,
    placeholder="sku,required_kg\nABC123,20000\nXYZ9,15000",
)

container_codes = ["CNT_20_DRY_STD", "CNT_40_DRY_STD", "CNT_40_DRY_HC", "CNT_20_RF", "CNT_40_RF"]
truck_codes = ["TRL_53_STD"]
container_code = st.selectbox("Container equipment", container_codes, index=2)
truck_code = st.selectbox("Truck equipment", truck_codes, index=0)

allow_mixing_containers = st.checkbox("Allow mixing in containers", value=False)
allow_mixing_trucks = st.checkbox("Allow mixing in trucks", value=True)
allow_stacking_trucks = st.checkbox("Allow stacking in trucks", value=False)

if st.button("Run Batch Plan", type="primary"):
    if not paste_text.strip():
        st.error("Please paste csv rows first.")
        st.stop()
    try:
        demand_df = pd.read_csv(io.StringIO(paste_text))
    except Exception as exc:
        st.error(f"Unable to parse CSV: {exc}")
        st.stop()

    cols = {c.lower().strip(): c for c in demand_df.columns}
    sku_col = cols.get("sku") or cols.get("part_number")
    kg_col = cols.get("required_kg")
    if not sku_col or not kg_col:
        st.error("CSV must include sku (or part_number) and required_kg columns.")
        st.stop()

    demand_df = demand_df.rename(columns={sku_col: "sku", kg_col: "required_kg"})
    merged = demand_df.merge(
        sku_catalog,
        left_on="sku",
        right_on="part_number",
        how="left",
    )
    if merged["sku_id"].isna().any():
        missing = merged[merged["sku_id"].isna()]["sku"].astype(str).tolist()
        st.error(f"Unknown sku/part_number values: {', '.join(missing)}")
        st.stop()

    requirements = []
    errors = []
    for _, row in merged.iterrows():
        sku_id = int(row["sku_id"])
        pack_rows = get_pack_rules_for_sku(conn, sku_id)
        if not pack_rows:
            errors.append(f"No pack rule for sku_id={sku_id}")
            continue
        requirements.append(
            {
                "sku_id": sku_id,
                "part_number": row["part_number"],
                "required_kg": float(row["required_kg"]),
                "pack_rule": dict(pack_rows[0]),
            }
        )

    if errors:
        for msg in errors:
            st.error(msg)
        st.stop()

    container_eq = get_equipment_by_code(conn, container_code)
    truck_eq = get_equipment_by_code(conn, truck_code)
    if not container_eq or not truck_eq:
        st.error("Selected equipment not found or inactive.")
        st.stop()

    try:
        if allow_mixing_containers:
            st.info("Container MIX_OK override enabled: showing aggregate mixed plan for containers.")
            mixed_container = plan_trucks_mix_ok(requirements, dict(container_eq), allow_stacking_in_trucks=True, use_floor_area=True)
            container_result = {
                "per_sku": [],
                "total_conveyance_count": mixed_container["truck_count"],
                "policy": "MIX_OK",
            }
        else:
            container_result = plan_containers_no_mix(requirements, dict(container_eq))

        if allow_mixing_trucks:
            truck_result = plan_trucks_mix_ok(
                requirements,
                dict(truck_eq),
                allow_stacking_in_trucks=allow_stacking_trucks,
                use_floor_area=True,
            )
        else:
            truck_result = plan_trucks_no_mix(requirements, dict(truck_eq))
    except ValueError as exc:
        st.error(str(exc))
        st.stop()

    table1 = pd.DataFrame(truck_result["per_sku_conversion"])
    if not table1.empty:
        table1 = table1[["part_number", "required_kg", "kg_as_units_mode", "packs_required", "shipped_kg", "excess_kg"]]
        st.subheader("Per SKU conversion")
        st.dataframe(table1, width="stretch", hide_index=True)

    container_df = pd.DataFrame(container_result["per_sku"])
    if not container_df.empty:
        container_df["cube_util"] = (container_df["cube_util"] * 100).round(1)
        container_df["weight_util"] = (container_df["weight_util"] * 100).round(1)
        st.subheader("Container results")
        st.dataframe(
            container_df[["part_number", "equipment_code", "packs_fit", "containers_needed", "limiting_constraint", "cube_util", "weight_util"]],
            width="stretch",
            hide_index=True,
        )

    st.subheader("Truck summary")
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "truck_count": truck_result["truck_count"],
                    "weight_util%": round(truck_result["weight_util"] * 100, 1),
                    "volume_util%": round(truck_result["volume_util"] * 100, 1),
                    "no_mix_baseline": truck_result["no_mix_baseline_truck_count"],
                    "mix_policy": "MIX_OK" if allow_mixing_trucks else "NO_MIX",
                    "container_policy": "MIX_OK" if allow_mixing_containers else "NO_MIX",
                }
            ]
        ),
        width="stretch",
        hide_index=True,
    )

    truck_plan_df = pd.DataFrame(truck_result["trucks"])
    if not truck_plan_df.empty:
        st.subheader("Truck load plan")
        st.dataframe(truck_plan_df, width="stretch", hide_index=True)
