from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from db import get_conn, map_import_demand_rows, run_migrations
from planning_engine import plan_quick_run
from seed import seed_if_empty
from field_specs import TABLE_SPECS
from validators import validate_dates, validate_with_specs

st.set_page_config(page_title="Quick Plan", layout="wide")
run_migrations()
seed_if_empty()

from pathlib import Path



def render_demand_upload_box(conn) -> None:
    with st.expander("Upload data to fill this page", expanded=False):
        st.caption("Upload demand_template.csv-compatible demand to append rows used by planning screens.")
        demand_template = Path("templates") / "demand_template.csv"
        if demand_template.exists():
            st.download_button(
                "Download demand_template.csv",
                data=demand_template.read_bytes(),
                file_name="demand_template.csv",
                mime="text/csv",
                key="quick_demand_template",
            )
        upload = st.file_uploader("Upload demand csv", type=["csv"], key="quick_demand_upload")
        if upload is None:
            return
        frame = pd.read_csv(upload)
        edited = st.data_editor(frame, num_rows="dynamic", width="stretch", key="quick_demand_editor")
        if st.button("Append uploaded demand", key="quick_append_demand"):
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
                """
                SELECT sm.sku_id, sm.part_number, s.supplier_code
                FROM sku_master sm
                JOIN suppliers s ON s.supplier_id = sm.supplier_id
                """,
                conn,
            )
            merged, map_errors = map_import_demand_rows(edited, sku_catalog, {})
            if map_errors:
                st.error("; ".join(map_errors))
                st.stop()
            cols = ["sku_id", "need_date", "qty", "coo_override", "priority", "notes", "phase", "mode_override", "service_scope", "miles"]
            to_insert = merged[cols]
            with conn:
                to_insert.to_sql("demand_lines", conn, if_exists="append", index=False)
            st.success(f"Appended {len(to_insert)} demand rows")

st.title("Quick Plan")

if hasattr(st, "page_link"):
    nav = st.container(border=True)
    nav.caption("Navigation")
    c1, c2, c3, c4 = nav.columns(4)
    c1.page_link("app.py", label="Main Planner", icon="🏠")
    c2.page_link("pages/quick_plan.py", label="Quick Plan", icon="📦")
    c3.page_link("pages/batch_plan.py", label="Batch Plan", icon="🚚")
    c4.page_link("pages/bom_planner.py", label="BOM Planner", icon="🧾")

conn = get_conn()

render_demand_upload_box(conn)


sku_df = pd.read_sql_query(
    """
    SELECT sm.sku_id, sm.part_number, sm.description, sm.default_coo, s.supplier_code,
           sm.part_number || ' [' || s.supplier_code || ']' AS sku_label
    FROM sku_master sm
    JOIN suppliers s ON s.supplier_id = sm.supplier_id
    ORDER BY sm.part_number, s.supplier_code
    """,
    conn,
)

if sku_df.empty:
    st.warning("No SKUs found. Add SKU records first.")
    st.stop()

sku_choice = st.selectbox("Supplier / Part Number", sku_df["sku_label"].tolist())
sku_row = sku_df[sku_df["sku_label"] == sku_choice].iloc[0]
sku_id = int(sku_row["sku_id"])

required_units = st.number_input("Required units", min_value=0.0, step=1.0, value=0.0)
need_date = st.date_input("Need date", value=date.today())
coo_override = st.text_input("COO override (optional)", value="")

pack_rules = pd.read_sql_query(
    "SELECT id, pack_name, units_per_pack, kg_per_unit FROM packaging_rules WHERE sku_id = ? ORDER BY is_default DESC, id",
    conn,
    params=(sku_id,),
)
pack_options = ["(default)"] + [f"{int(r['id'])} - {r['pack_name']}" for _, r in pack_rules.iterrows()]
pack_choice = st.selectbox("Pack rule (optional)", pack_options)
pack_rule_id = None if pack_choice == "(default)" else int(pack_choice.split(" - ")[0])

lanes = pd.read_sql_query("SELECT origin_code, dest_code FROM lanes ORDER BY origin_code, dest_code", conn)
lane_options = ["(none)"] + [f"{r['origin_code']} -> {r['dest_code']}" for _, r in lanes.iterrows()]
lane_choice = st.selectbox("Lane (optional)", lane_options)
lane_origin = None
lane_dest = None
if lane_choice != "(none)":
    lane_origin, lane_dest = [x.strip() for x in lane_choice.split("->")]

service_scope = st.selectbox("Service scope", ["P2P", "P2D", "D2P", "D2D"], index=0)
modes = st.multiselect("Mode filter", ["AIR", "OCEAN", "TRUCK", "DRAY"], default=["AIR", "OCEAN", "TRUCK"])

juris_df = pd.read_sql_query("SELECT jurisdiction_code FROM jurisdiction_weight_rules WHERE active = 1 ORDER BY jurisdiction_code", conn)
juris_options = juris_df["jurisdiction_code"].tolist() if not juris_df.empty else ["US_FED_INTERSTATE"]
jurisdiction_code = st.selectbox("Jurisdiction", juris_options, index=juris_options.index("US_FED_INTERSTATE") if "US_FED_INTERSTATE" in juris_options else 0)

truck_df = pd.read_sql_query("SELECT truck_config_code, description FROM truck_configs WHERE active = 1 ORDER BY truck_config_code", conn)
truck_options = [f"{r['truck_config_code']} - {r['description']}" for _, r in truck_df.iterrows()] if not truck_df.empty else ["5AXLE_TL - default"]
truck_choice = st.selectbox("Truck/Chassis Config", truck_options, index=0)
truck_config_code = truck_choice.split(" - ")[0]

if st.button("Run Plan", type="primary"):
    result = plan_quick_run(
        conn=conn,
        sku_id=sku_id,
        required_units=required_units,
        need_date=need_date.isoformat(),
        coo_override=coo_override.strip() or None,
        pack_rule_id=pack_rule_id,
        lane_origin_code=lane_origin,
        lane_dest_code=lane_dest,
        service_scope=service_scope,
        modes=modes,
        jurisdiction_code=jurisdiction_code,
        truck_config_code=truck_config_code,
    )

    sku = result["sku"]
    st.subheader("Summary")
    st.write(
        f"**{sku['part_number']}** — {sku.get('description') or ''}  \\n"
        f"COO: `{sku['coo']}` | Required units: `{result['required_units']}` | "
        f"Shipped units: `{result['shipped_units']}` | Excess: `{result['excess_units']}` | Packs: `{result['packs_required']}`"
    )

    eq_df = pd.DataFrame(result["equipment"])
    if not eq_df.empty:
        eq_df["cube_util%"] = (eq_df["cube_util"] * 100).round(1)
        eq_df["weight_util%"] = (eq_df["weight_util"] * 100).round(1)
        st.subheader("Equipment fit")
        st.dataframe(
            eq_df[["mode", "equipment_code", "equipment_name", "packs_per_layer", "layers_allowed", "packs_fit", "limiting_constraint", "equipment_count", "cube_util%", "weight_util%", "est_cost"]],
            width="stretch",
            hide_index=True,
        )



    if result.get("warnings"):
        for w in result["warnings"]:
            st.warning(w)

    if not eq_df.empty:
        st.subheader("Constraint breakdown")
        for _, row in eq_df.iterrows():
            with st.expander(f"{row['equipment_code']} / {row['equipment_name']}", expanded=False):
                st.json(row.get("constraint_breakdown", []))

    excluded_df = pd.DataFrame(result.get("excluded_equipment", []))
    if not excluded_df.empty:
        st.subheader("Excluded equipment")
        st.dataframe(excluded_df[["mode", "equipment_code", "equipment_name", "reason"]], width="stretch", hide_index=True)

    mode_df = pd.DataFrame(result["mode_summary"])
    if not mode_df.empty:
        st.subheader("Mode summary")
        st.dataframe(
            mode_df[["mode", "cost_best", "carrier_best", "lead_days", "ship_by_date"]],
            width="stretch",
            hide_index=True,
        )

    if result.get("rate_breakdown"):
        st.subheader("Rate breakdown")
        for mode, rows in result["rate_breakdown"].items():
            with st.expander(f"{mode} breakdown", expanded=False):
                st.json(rows)
