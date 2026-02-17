from __future__ import annotations


import pandas as pd
import streamlit as st

from db import get_conn, run_migrations
from models import Equipment, PackagingRule
from planner import allocate_tranches, build_shipments, recommend_modes
from seed import ensure_templates, seed_if_empty

st.set_page_config(page_title="Logistics Planner", layout="wide")
run_migrations()
seed_if_empty()
ensure_templates()


def read_table(name: str) -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql_query(f"SELECT * FROM {name}", conn)


def replace_table(name: str, df: pd.DataFrame) -> None:
    conn = get_conn()
    with conn:
        conn.execute(f"DELETE FROM {name}")
        if not df.empty:
            df.to_sql(name, conn, if_exists="append", index=False)


def equipment_from_row(row: dict) -> Equipment:
    return Equipment(
        name=row.get("name"),
        mode=row.get("mode"),
        length_m=float(row.get("length_m") or 0),
        width_m=float(row.get("width_m") or 0),
        height_m=float(row.get("height_m") or 0),
        max_payload_kg=float(row.get("max_payload_kg") or 0),
        volumetric_factor=float(row["volumetric_factor"]) if pd.notna(row.get("volumetric_factor")) else None,
    )


st.title("Local Logistics Planning App")

cfg_tab, data_tab, alloc_tab, rec_tab, ship_tab, exp_tab = st.tabs(
    ["Configure", "Master Data + Import", "Allocation", "Recommendations", "Shipment Builder", "Export"]
)

with cfg_tab:
    st.subheader("Equipment Presets")
    eq = read_table("equipment_presets")
    eq_edited = st.data_editor(eq, num_rows="dynamic", width="stretch")
    if st.button("Save Equipment"):
        replace_table("equipment_presets", eq_edited)
        st.success("Equipment saved")

    st.subheader("Lead Times")
    leads = read_table("lead_times")
    leads_edit = st.data_editor(leads, num_rows="dynamic", width="stretch")
    if st.button("Save Lead Times"):
        replace_table("lead_times", leads_edit)
        st.success("Lead times saved")

    st.subheader("Lead Time Overrides (SKU+Mode)")
    ov = read_table("lead_time_overrides")
    ov_edit = st.data_editor(ov, num_rows="dynamic", width="stretch")
    if st.button("Save Lead Overrides"):
        replace_table("lead_time_overrides", ov_edit)
        st.success("Overrides saved")

    st.subheader("Rates")
    rates = read_table("rates")
    rates_edit = st.data_editor(rates, num_rows="dynamic", width="stretch")
    if st.button("Save Rates"):
        replace_table("rates", rates_edit)
        st.success("Rates saved")

with data_tab:
    st.subheader("SKU Master")
    sku_df = read_table("sku_master")
    sku_edit = st.data_editor(sku_df, num_rows="dynamic", width="stretch")
    if st.button("Save SKU Master"):
        replace_table("sku_master", sku_edit)

    st.subheader("Packaging Rules")
    pack_df = read_table("packaging_rules")
    pack_edit = st.data_editor(pack_df, num_rows="dynamic", width="stretch")
    if st.button("Save Packaging Rules"):
        replace_table("packaging_rules", pack_edit)

    st.subheader("Import Demand/BOM CSV")
    st.caption("Templates in ./templates")
    up = st.file_uploader("Upload demand csv", type=["csv"])
    if up is not None:
        imported = pd.read_csv(up)
        st.dataframe(imported)
        if st.button("Load into demand_lines"):
            conn = get_conn()
            with conn:
                imported.to_sql("demand_lines", conn, if_exists="append", index=False)
            st.success("Demand imported")

with alloc_tab:
    demands = read_table("demand_lines")
    packs = read_table("packaging_rules")
    if demands.empty or packs.empty:
        st.info("Need demand_lines and packaging_rules first")
    else:
        line = st.selectbox("Demand line", demands["id"].tolist())
        d = demands[demands["id"] == line].iloc[0]
        p = packs[packs["sku"] == d["sku"]].iloc[0]
        rule = PackagingRule(**p.to_dict())

        st.write("Define tranches")
        tr_input = st.data_editor(
            pd.DataFrame(
                [
                    {"tranche_name": "T1", "allocation_type": "percent", "allocation_value": 60},
                    {"tranche_name": "T2", "allocation_type": "percent", "allocation_value": 40},
                ]
            ),
            num_rows="dynamic",
            width="stretch",
        )
        rows = allocate_tranches(
            d["qty"],
            rule,
            [(r["tranche_name"], r["allocation_type"], float(r["allocation_value"])) for _, r in tr_input.iterrows()],
        )
        out = pd.DataFrame([r.__dict__ for r in rows])
        st.dataframe(out, width="stretch")

with rec_tab:
    demands = read_table("demand_lines")
    packs = read_table("packaging_rules")
    eq = read_table("equipment_presets")
    rates = read_table("rates")
    lead = read_table("lead_times")
    lead_ov = read_table("lead_time_overrides")

    if not demands.empty and not packs.empty and not eq.empty:
        line = st.selectbox("Line for recommendation", demands["id"].tolist(), key="rec_line")
        d = demands[demands["id"] == line].iloc[0]
        p = packs[packs["sku"] == d["sku"]].iloc[0]
        rule = PackagingRule(**p.to_dict())
        need_date = pd.to_datetime(d["need_date"]).date()
        coo = d["coo_override"] if pd.notna(d["coo_override"]) else read_table("sku_master").set_index("sku").loc[d["sku"], "default_coo"]

        eq_by_mode: dict[str, list[Equipment]] = {}
        for mode, g in eq.groupby("mode"):
            eq_by_mode[mode] = [equipment_from_row(x) for x in g.to_dict("records")]

        lead_tbl = {(r["country_of_origin"], r["mode"]): int(r["lead_days"]) for _, r in lead.iterrows()}
        sku_ov = {(r["sku"], r["mode"]): int(r["lead_days"]) for _, r in lead_ov.iterrows()}
        recs = recommend_modes(
            sku=d["sku"],
            coo=coo,
            need_date=need_date,
            ordered_units=d["qty"],
            pack_rule=rule,
            equipment_by_mode=eq_by_mode,
            rates=rates.to_dict("records"),
            lead_table=lead_tbl,
            sku_lead_override=sku_ov,
            manual_lead_override=st.number_input("Manual lead override", min_value=0, value=0),
        )
        st.dataframe(pd.DataFrame(recs), width="stretch")

with ship_tab:
    st.write("Greedy consolidation preview (volume then weight)")
    demo = st.data_editor(
        pd.DataFrame(
            [
                {"mode": "Ocean", "volume_m3": 20.0, "weight_kg": 9000.0, "ship_by": "2026-03-01", "cost": 4000},
                {"mode": "Ocean", "volume_m3": 35.0, "weight_kg": 10000.0, "ship_by": "2026-03-05", "cost": 4000},
                {"mode": "Truck", "volume_m3": 40.0, "weight_kg": 6000.0, "ship_by": "2026-03-10", "cost": 2000},
            ]
        ),
        num_rows="dynamic",
        width="stretch",
    )
    eq = read_table("equipment_presets")
    if not eq.empty:
        eq_map = {mode: equipment_from_row(group.iloc[0].to_dict()) for mode, group in eq.groupby("mode")}
        shipments = build_shipments(demo.to_dict("records"), eq_map)
        st.dataframe(pd.DataFrame(shipments), width="stretch")

with exp_tab:
    st.subheader("Export reports")
    demand = read_table("demand_lines")
    booking = read_table("rates")
    excess = pd.DataFrame(columns=["sku", "tranche", "excess_units"])

    def dl(df: pd.DataFrame, name: str):
        st.download_button(name, data=df.to_csv(index=False).encode(), file_name=name, mime="text/csv")

    dl(demand, "shipment_plan.csv")
    dl(booking, "booking_summary.csv")
    dl(excess, "excess_report.csv")

st.caption("All data stored locally in SQLite planner.db")
