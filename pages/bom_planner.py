from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from bom_planner import (
    BomPlanningPolicy,
    create_bom_run,
    generate_container_plan,
    generate_pack_plan,
    generate_schedule_summary,
    generate_truck_plan,
    read_bom_upload,
    validate_bom_frame,
)
from db import get_conn, map_import_demand_rows, run_migrations
from seed import seed_if_empty
from field_specs import TABLE_SPECS
from validators import validate_dates, validate_with_specs
from pathlib import Path

st.set_page_config(page_title="BOM Planner", layout="wide")
run_migrations()
seed_if_empty()
conn = get_conn()



def render_demand_upload_box(conn) -> None:
    with st.expander("Upload data to fill this page", expanded=False):
        st.caption("Optional demand upload to populate planning data while using BOM workflows.")
        demand_template = Path("templates") / "demand_template.csv"
        if demand_template.exists():
            st.download_button("Download demand_template.csv", data=demand_template.read_bytes(), file_name="demand_template.csv", mime="text/csv", key="bom_demand_template")
        upload = st.file_uploader("Upload demand csv", type=["csv"], key="bom_demand_upload")
        if upload is None:
            return
        frame = pd.read_csv(upload)
        edited = st.data_editor(frame, num_rows="dynamic", width="stretch", key="bom_demand_editor")
        if st.button("Append uploaded demand", key="bom_append_demand"):
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

st.title("BOM Planner")

if hasattr(st, "page_link"):
    nav = st.container(border=True)
    nav.caption("Navigation")
    c1, c2, c3, c4 = nav.columns(4)
    c1.page_link("app.py", label="Main Planner", icon="🏠")
    c2.page_link("pages/quick_plan.py", label="Quick Plan", icon="📦")
    c3.page_link("pages/batch_plan.py", label="Batch Plan", icon="🚚")
    c4.page_link("pages/bom_planner.py", label="BOM Planner", icon="🧾")

render_demand_upload_box(conn)

runs = pd.read_sql_query("SELECT bom_run_id, name, created_at FROM bom_runs ORDER BY bom_run_id DESC", conn)
selected_run = st.selectbox("BOM Run", runs["bom_run_id"].tolist() if not runs.empty else [], format_func=lambda x: f"Run {x}") if not runs.empty else None

policy = BomPlanningPolicy(
    container_policy="NO_MIX" if st.checkbox("container_policy: NO_MIX", value=True) else "MIX_OK",
    truck_policy="MIX_OK" if st.checkbox("truck_policy: MIX_OK", value=True) else "NO_MIX",
    allow_stacking_in_trucks=st.checkbox("allow_stacking_in_trucks", value=False),
    default_ocean_equipment=st.text_input("default_ocean_equipment", value="CNT_40_DRY_HC"),
    default_truck_equipment=st.text_input("default_truck_equipment", value="TRL_53_STD"),
)

tabs = st.tabs(["Import BOM", "Pack Plan", "Container Plan", "Truck Plan", "Schedule / Ship-by", "Export"])

with tabs[0]:
    upload = st.file_uploader("Upload BOM CSV/XLSX", type=["csv", "xlsx"])
    run_name = st.text_input("Run name", value=f"BOM {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    if upload is not None:
        frame = read_bom_upload(upload.name, upload.getvalue())
        mapped, errors, warnings = validate_bom_frame(conn, frame)
        st.write("Mapping/preview")
        st.dataframe(mapped.head(100), width="stretch")
        for w in warnings:
            st.warning(w)
        for e in errors:
            st.error(e)
        if st.button("Save BOM Run", type="primary", disabled=bool(errors)):
            bom_run_id = create_bom_run(conn, run_name, mapped[mapped["sku_id"].notna()].copy())
            st.success(f"Saved bom_run_id={bom_run_id}")

with tabs[1]:
    if selected_run and st.button("Generate Pack Plan", key="pack"):
        generate_pack_plan(conn, int(selected_run))
    if selected_run:
        df = pd.read_sql_query(
            """
            SELECT p.phase_name, p.need_date, sm.part_number, p.required_kg, p.shipped_kg, p.excess_kg, p.packs_required, pr.pack_name
            FROM pack_plan_lines p
            JOIN sku_master sm ON sm.sku_id = p.sku_id
            JOIN packaging_rules pr ON pr.id = p.pack_rule_id
            WHERE p.bom_run_id = ?
            ORDER BY p.phase_name, p.need_date, sm.part_number
            """,
            conn,
            params=(selected_run,),
        )
        st.dataframe(df, width="stretch", hide_index=True)

with tabs[2]:
    if selected_run and st.button("Generate Container Plan", key="container"):
        generate_container_plan(conn, int(selected_run), policy)
    if selected_run:
        df = pd.read_sql_query(
            """
            SELECT c.phase_name, c.need_date, sm.part_number, c.equipment_code, c.packs_fit, c.containers_needed,
                   ROUND(c.cube_util*100,1) cube_util_pct, ROUND(c.weight_util*100,1) weight_util_pct, c.limiting_constraint
            FROM container_plan_lines c
            JOIN sku_master sm ON sm.sku_id = c.sku_id
            WHERE c.bom_run_id = ?
            ORDER BY c.phase_name, c.need_date, sm.part_number
            """,
            conn,
            params=(selected_run,),
        )
        st.dataframe(df, width="stretch", hide_index=True)
        if not df.empty:
            st.write("Totals")
            st.dataframe(df.groupby(["phase_name", "need_date"], as_index=False)["containers_needed"].sum(), hide_index=True)

with tabs[3]:
    if selected_run and st.button("Generate Truck Plan", key="truck"):
        generate_truck_plan(conn, int(selected_run), policy)
    if selected_run:
        summary = pd.read_sql_query("SELECT * FROM truck_plan_runs WHERE bom_run_id = ?", conn, params=(selected_run,))
        st.dataframe(summary, width="stretch", hide_index=True)
        trucks = pd.read_sql_query("SELECT * FROM truck_plan_trucks WHERE bom_run_id = ?", conn, params=(selected_run,))
        items = pd.read_sql_query("SELECT * FROM truck_plan_truck_items WHERE bom_run_id = ?", conn, params=(selected_run,))
        for _, tr in trucks.iterrows():
            with st.expander(f"{tr['phase_name']} {tr['need_date']} Truck {int(tr['truck_index'])}"):
                st.write(tr.to_dict())
                t_items = items[
                    (items["phase_name"] == tr["phase_name"]) &
                    (items["need_date"] == tr["need_date"]) &
                    (items["truck_index"] == tr["truck_index"])
                ]
                st.dataframe(t_items, hide_index=True)

with tabs[4]:
    if selected_run and st.button("Generate Schedule", key="schedule"):
        generate_schedule_summary(conn, int(selected_run))
    if selected_run:
        df = pd.read_sql_query(
            """
            SELECT s.phase_name, s.need_date, sm.part_number, s.mode, s.lead_days, s.ship_by_date
            FROM schedule_summary s
            JOIN sku_master sm ON sm.sku_id = s.sku_id
            WHERE s.bom_run_id = ?
            ORDER BY s.phase_name, s.need_date, sm.part_number, s.mode
            """,
            conn,
            params=(selected_run,),
        )
        st.dataframe(df, width="stretch", hide_index=True)

with tabs[5]:
    if selected_run:
        names = [
            "pack_plan_lines",
            "container_plan_lines",
            "truck_plan_runs",
            "truck_plan_trucks",
            "truck_plan_truck_items",
            "schedule_summary",
        ]
        for name in names:
            df = pd.read_sql_query(f"SELECT * FROM {name} WHERE bom_run_id = ?", conn, params=(selected_run,))
            st.download_button(
                f"Download {name}.csv",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name=f"{name}_{selected_run}.csv",
                mime="text/csv",
                key=f"dl_{name}",
            )
