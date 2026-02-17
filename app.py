from __future__ import annotations

import io

import pandas as pd
import streamlit as st

from db import compute_grid_diff, delete_rows, get_conn, run_migrations, upsert_rows
from models import Equipment, PackagingRule
from planner import allocate_tranches, build_shipments, recommend_modes
from seed import ensure_templates, seed_if_empty
from validators import require_cols, validate_dates, validate_positive

st.set_page_config(page_title="Logistics Planner", layout="wide")
run_migrations()
seed_if_empty()
ensure_templates()


def read_table(name: str) -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql_query(f"SELECT * FROM {name}", conn)


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

def save_grid(table: str, original: pd.DataFrame, edited: pd.DataFrame, key_cols: list[str]) -> None:
    inserts, updates, deletes = compute_grid_diff(original, edited, key_cols)
    conn = get_conn()
    with conn:
        upsert_rows(conn, table, pd.concat([inserts, updates], ignore_index=True), key_cols)
        delete_rows(conn, table, deletes, key_cols)


st.title("Local Logistics Planning App")
section = st.sidebar.radio("Section", ["Planner", "Admin"])

if section == "Admin":
    st.sidebar.header("Admin")
    admin_screen = st.sidebar.selectbox(
        "Admin screen",
        [
            "Equipment presets",
            "SKUs",
            "Pack rules",
            "Lead times",
            "Rates",
            "Demand entry",
        ],
    )

    if admin_screen == "Equipment presets":
        source = read_table("equipment_presets")
        edited = st.data_editor(source, num_rows="dynamic", width="stretch")
        if st.button("Save changes", key="save_eq"):
            errors = require_cols(edited, ["name", "mode"]) + validate_positive(edited, ["length_m", "width_m", "height_m"]) + validate_positive(edited, ["max_payload_kg"], allow_zero=True)
            if errors:
                st.error("; ".join(errors))
            else:
                save_grid("equipment_presets", source, edited, ["id"])
                st.success("Equipment presets saved")

    elif admin_screen == "SKUs":
        source = read_table("sku_master")
        q = st.text_input("Search SKU or description")
        filtered_source = source[source.astype(str).apply(lambda c: c.str.contains(q, case=False, na=False)).any(axis=1)] if q else source
        edited = st.data_editor(filtered_source, num_rows="dynamic", width="stretch")
        if st.button("Save changes", key="save_sku"):
            errors = require_cols(edited, ["sku", "default_coo"])
            if errors:
                st.error("; ".join(errors))
            else:
                # When search is active, only persist edits for the visible slice.
                # Diffing against the full table would treat hidden rows as deletions.
                save_grid("sku_master", filtered_source, edited, ["sku"])
                st.success("SKUs saved")

    elif admin_screen == "Pack rules":
        source = read_table("packaging_rules")
        edited = st.data_editor(source, num_rows="dynamic", width="stretch")
        if st.button("Save changes", key="save_pack"):
            errors = require_cols(edited, ["sku", "pack_type"]) + validate_positive(edited, ["units_per_pack", "pack_length_m", "pack_width_m", "pack_height_m"]) + validate_positive(edited, ["kg_per_unit", "pack_tare_kg"], allow_zero=True)
            defaults = edited.groupby("sku")["is_default"].sum() if not edited.empty else pd.Series(dtype=int)
            if not defaults.empty and (defaults < 1).any():
                errors.append("Each SKU must have at least one default pack type")
            if errors:
                st.error("; ".join(errors))
            else:
                save_grid("packaging_rules", source, edited, ["id"])
                st.success("Pack rules saved")

    elif admin_screen == "Lead times":
        lt_source = read_table("lead_times")
        lt_edited = st.data_editor(lt_source, num_rows="dynamic", width="stretch")
        ov_source = read_table("lead_time_overrides")
        ov_edited = st.data_editor(ov_source, num_rows="dynamic", width="stretch")
        if st.button("Save changes", key="save_lead"):
            errors = require_cols(lt_edited, ["country_of_origin", "mode", "lead_days"]) + require_cols(ov_edited, ["sku", "mode", "lead_days"]) + validate_positive(lt_edited, ["lead_days"], allow_zero=True) + validate_positive(ov_edited, ["lead_days"], allow_zero=True)
            if errors:
                st.error("; ".join(errors))
            else:
                save_grid("lead_times", lt_source, lt_edited, ["id"])
                save_grid("lead_time_overrides", ov_source, ov_edited, ["id"])
                st.success("Lead times and overrides saved")

    elif admin_screen == "Rates":
        source = read_table("rates")
        edited = st.data_editor(source, num_rows="dynamic", width="stretch")
        if st.button("Save changes", key="save_rates"):
            errors = require_cols(edited, ["mode", "pricing_model", "rate_value"]) + validate_positive(edited, ["rate_value"], allow_zero=True)
            errors += validate_dates(edited, ["effective_start", "effective_end"])
            if errors:
                st.error("; ".join(errors))
            else:
                save_grid("rates", source, edited, ["id"])
                st.success("Rates saved")

    elif admin_screen == "Demand entry":
        source = read_table("demand_lines")
        edited = st.data_editor(source, num_rows="dynamic", width="stretch")
        st.caption("Optional CSV import")
        upload = st.file_uploader("Upload demand csv", type=["csv"])
        if upload is not None:
            imported = pd.read_csv(upload)
            st.write("Imported preview (editable)")
            imported_edit = st.data_editor(imported, num_rows="dynamic", width="stretch")
            if st.button("Append imported rows"):
                conn = get_conn()
                with conn:
                    imported_edit.to_sql("demand_lines", conn, if_exists="append", index=False)
                st.success("Imported rows appended")

        paste = st.text_area("Paste CSV grid (header required)")
        if st.button("Append pasted rows") and paste.strip():
            pasted_df = pd.read_csv(io.StringIO(paste.strip()))
            conn = get_conn()
            with conn:
                pasted_df.to_sql("demand_lines", conn, if_exists="append", index=False)
            st.success("Pasted rows appended")

        if st.button("Save changes", key="save_demand"):
            errors = require_cols(edited, ["sku", "need_date", "qty"]) + validate_positive(edited, ["qty"], allow_zero=True) + validate_dates(edited, ["need_date"])
            if errors:
                st.error("; ".join(errors))
            else:
                save_grid("demand_lines", source, edited, ["id"])
                st.success("Demand lines saved")

else:
    alloc_tab, rec_tab, ship_tab, exp_tab = st.tabs(["Allocation", "Recommendations", "Shipment Builder", "Export"])

    with alloc_tab:
        demands = read_table("demand_lines")
        packs = read_table("packaging_rules")
        if demands.empty or packs.empty:
            st.info("Need demand_lines and packaging_rules first")
        else:
            line = st.selectbox("Demand line", demands["id"].tolist())
            d = demands[demands["id"] == line].iloc[0]
            pack_rows = packs[(packs["sku"] == d["sku"]) & (packs["is_default"] == 1)]
            p = pack_rows.iloc[0] if not pack_rows.empty else packs[packs["sku"] == d["sku"]].iloc[0]
            rule = PackagingRule(**{k: p[k] for k in PackagingRule.__dataclass_fields__.keys()})

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
            st.dataframe(pd.DataFrame([r.__dict__ for r in rows]), width="stretch")

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
            pack_rows = packs[(packs["sku"] == d["sku"]) & (packs["is_default"] == 1)]
            p = pack_rows.iloc[0] if not pack_rows.empty else packs[packs["sku"] == d["sku"]].iloc[0]
            rule = PackagingRule(**{k: p[k] for k in PackagingRule.__dataclass_fields__.keys()})
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
