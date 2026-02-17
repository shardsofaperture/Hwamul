from __future__ import annotations

import io
import sqlite3
from datetime import date

import pandas as pd
import streamlit as st

from db import (
    compute_grid_diff,
    delete_rows,
    export_data_bundle,
    get_conn,
    import_data_bundle,
    purge_demand_before,
    run_migrations,
    upsert_rows,
    vacuum_db,
)
from models import Equipment, PackagingRule
from planner import allocate_tranches, build_shipments, recommend_modes
from rate_engine import RateTestInput, compute_rate_total, select_best_rate_card
from seed import ensure_templates, seed_if_empty
from validators import require_cols, validate_dates, validate_positive

st.set_page_config(page_title="Logistics Planner", layout="wide")
run_migrations()
seed_if_empty()
ensure_templates()


def read_table(name: str) -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql_query(f"SELECT * FROM {name}", conn)


def validate_date_ranges(df: pd.DataFrame, start_col: str, end_col: str, label: str) -> list[str]:
    if start_col not in df.columns or end_col not in df.columns:
        return []
    start = pd.to_datetime(df[start_col].replace("", pd.NA), errors="coerce")
    end = pd.to_datetime(df[end_col].replace("", pd.NA), errors="coerce")
    bad = start.notna() & end.notna() & (start > end)
    return [f"{label}: {start_col} must be <= {end_col}"] if bad.any() else []


def normalize_bools(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = out[col].fillna(0).astype(int)
    return out


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

def save_grid(table: str, original: pd.DataFrame, edited: pd.DataFrame, key_cols: list[str]) -> tuple[bool, str | None]:
    inserts, updates, deletes = compute_grid_diff(original, edited, key_cols)
    conn = get_conn()
    try:
        with conn:
            upsert_rows(conn, table, pd.concat([inserts, updates], ignore_index=True), key_cols)
            delete_rows(conn, table, deletes, key_cols)
    except sqlite3.IntegrityError as exc:
        return False, str(exc)
    return True, None


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
            "Carriers",
            "Rate cards",
            "Rate Test",
            "Demand entry",
            "Data management",
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
                ok, err = save_grid("equipment_presets", source, edited, ["id"])
                if ok:
                    st.success("Equipment presets saved")
                else:
                    st.error(f"Could not save equipment presets: {err}")

    elif admin_screen == "SKUs":
        source = read_table("sku_master")
        q = st.text_input("Search SKU or description")
        filtered_source = source[source.astype(str).apply(lambda c: c.str.contains(q, case=False, na=False)).any(axis=1)] if q else source
        edited = st.data_editor(filtered_source, num_rows="dynamic", width="stretch")
        if st.button("Save changes", key="save_sku"):
            errors = require_cols(edited, ["part_number", "default_coo"])
            if errors:
                st.error("; ".join(errors))
            else:
                # When search is active, only persist edits for the visible slice.
                # Diffing against the full table would treat hidden rows as deletions.
                ok, err = save_grid("sku_master", filtered_source, edited, ["part_number"])
                if ok:
                    st.success("SKUs saved")
                    st.rerun()
                else:
                    st.error(f"Could not save SKUs: {err}. If changing existing SKU codes, update dependent pack and demand rows first.")

    elif admin_screen == "Pack rules":
        source = read_table("packaging_rules")
        edited = st.data_editor(source, num_rows="dynamic", width="stretch")
        if st.button("Save changes", key="save_pack"):
            errors = require_cols(edited, ["part_number", "pack_type"]) + validate_positive(edited, ["units_per_pack", "pack_length_m", "pack_width_m", "pack_height_m"]) + validate_positive(edited, ["kg_per_unit", "pack_tare_kg"], allow_zero=True)
            defaults = edited.groupby("part_number")["is_default"].sum() if not edited.empty else pd.Series(dtype=int)
            if not defaults.empty and (defaults < 1).any():
                errors.append("Each SKU must have at least one default pack type")
            if errors:
                st.error("; ".join(errors))
            else:
                ok, err = save_grid("packaging_rules", source, edited, ["id"])
                if ok:
                    st.success("Pack rules saved")
                else:
                    st.error(f"Could not save pack rules: {err}")

    elif admin_screen == "Lead times":
        lt_source = read_table("lead_times")
        lt_edited = st.data_editor(lt_source, num_rows="dynamic", width="stretch")
        ov_source = read_table("lead_time_overrides")
        ov_edited = st.data_editor(ov_source, num_rows="dynamic", width="stretch")
        if st.button("Save changes", key="save_lead"):
            errors = require_cols(lt_edited, ["country_of_origin", "mode", "lead_days"]) + require_cols(ov_edited, ["part_number", "mode", "lead_days"]) + validate_positive(lt_edited, ["lead_days"], allow_zero=True) + validate_positive(ov_edited, ["lead_days"], allow_zero=True)
            if errors:
                st.error("; ".join(errors))
            else:
                ok_lt, err_lt = save_grid("lead_times", lt_source, lt_edited, ["id"])
                ok_ov, err_ov = save_grid("lead_time_overrides", ov_source, ov_edited, ["id"])
                if ok_lt and ok_ov:
                    st.success("Lead times and overrides saved")
                else:
                    st.error(f"Could not save lead times/overrides: {err_lt or err_ov}")

    elif admin_screen == "Rates":
        source = read_table("rates")
        edited = st.data_editor(source, num_rows="dynamic", width="stretch")
        if st.button("Save changes", key="save_rates"):
            errors = require_cols(edited, ["mode", "pricing_model", "rate_value"]) + validate_positive(edited, ["rate_value"], allow_zero=True)
            errors += validate_dates(edited, ["effective_start", "effective_end"])
            if errors:
                st.error("; ".join(errors))
            else:
                ok, err = save_grid("rates", source, edited, ["id"])
                if ok:
                    st.success("Rates saved")
                else:
                    st.error(f"Could not save rates: {err}")

    elif admin_screen == "Carriers":
        source = read_table("carrier")
        edited = st.data_editor(source, num_rows="dynamic", width="stretch")
        if st.button("Save changes", key="save_carriers"):
            edited = normalize_bools(edited, ["is_active"])
            errors = require_cols(edited, ["code", "name"])
            if errors:
                st.error("; ".join(errors))
            else:
                ok, err = save_grid("carrier", source, edited, ["id"])
                if ok:
                    st.success("Carriers saved")
                else:
                    st.error(f"Could not save carriers: {err}")

    elif admin_screen == "Rate cards":
        cards_source = read_table("rate_card")
        cards_edited = st.data_editor(cards_source, num_rows="dynamic", width="stretch")
        st.divider()
        st.subheader("Rate charges")
        card_options = cards_edited["id"].dropna().astype(int).tolist() if not cards_edited.empty else []
        charges_source = read_table("rate_charge")
        selected_card_id = None
        if card_options:
            selected_card_id = st.selectbox("Select rate_card", card_options)
            charge_view = charges_source[charges_source["rate_card_id"] == selected_card_id]
        else:
            st.info("Create and save at least one rate card before editing charges.")
            charge_view = charges_source.iloc[0:0]
        charges_edited = st.data_editor(charge_view, num_rows="dynamic", width="stretch")

        if st.button("Save rate master", key="save_rate_master"):
            cards_edited = normalize_bools(cards_edited, ["is_active"])
            card_required = [
                "mode", "service_scope", "equipment", "dim_class",
                "origin_type", "origin_code", "dest_type", "dest_code",
                "currency", "uom_pricing", "base_rate", "effective_from",
            ]
            errors = require_cols(cards_edited, card_required)
            errors += validate_positive(cards_edited, ["base_rate", "priority"], allow_zero=True)
            errors += validate_dates(cards_edited, ["effective_from", "effective_to", "contract_start", "contract_end"])
            errors += validate_date_ranges(cards_edited, "effective_from", "effective_to", "Rate cards")
            errors += validate_date_ranges(cards_edited, "contract_start", "contract_end", "Rate cards")

            if not charges_edited.empty:
                errors += require_cols(charges_edited, ["rate_card_id", "charge_code", "charge_name", "calc_method", "amount", "applies_when"])
                errors += validate_positive(charges_edited, ["amount"], allow_zero=True)
                errors += validate_dates(charges_edited, ["effective_from", "effective_to"])
                errors += validate_date_ranges(charges_edited, "effective_from", "effective_to", "Rate charges")

            if errors:
                st.error("; ".join(sorted(set(errors))))
            else:
                ok_cards, err_cards = save_grid("rate_card", cards_source, cards_edited, ["id"])
                ok_charges, err_charges = save_grid("rate_charge", charge_view, charges_edited, ["id"])
                if ok_cards and ok_charges:
                    st.success("Rate cards and charges saved")
                    st.rerun()
                else:
                    st.error(f"Could not save rate master: {err_cards or err_charges}")

    elif admin_screen == "Rate Test":
        cards = read_table("rate_card")
        carriers = read_table("carrier")
        charges = read_table("rate_charge")
        with st.form("rate_test_form"):
            c1, c2, c3 = st.columns(3)
            with c1:
                ship_date = st.date_input("Ship date", value=date.today())
                mode = st.text_input("Mode", value="OCEAN")
                equipment = st.text_input("Equipment", value="40DV")
                service_scope = st.selectbox("Service scope", ["P2P", "P2D", "D2P", "D2D"])
            with c2:
                origin_type = st.text_input("Origin type", value="PORT")
                origin_code = st.text_input("Origin code", value="USLAX")
                dest_type = st.text_input("Dest type", value="PORT")
                dest_code = st.text_input("Dest code", value="CNSHA")
            with c3:
                carrier_id = st.selectbox("Carrier", [None] + carriers.get("id", pd.Series(dtype=int)).dropna().astype(int).tolist())
                weight_kg = st.number_input("Weight kg", min_value=0.0, value=1000.0)
                volume_m3 = st.number_input("Volume m3", min_value=0.0, value=10.0)
                containers_count = st.number_input("Containers count", min_value=0.0, value=1.0)

            f1, f2 = st.columns(2)
            with f1:
                reefer = st.checkbox("Reefer")
                flatrack = st.checkbox("Flatrack")
                dg = st.checkbox("DG")
            with f2:
                oh = st.checkbox("Over Height")
                ow = st.checkbox("Over Width")
                ohw = st.checkbox("Over Height + Width")
                miles = st.number_input("Miles", min_value=0.0, value=0.0)
            submit = st.form_submit_button("Run rate test")

        if submit:
            shipment = RateTestInput(
                ship_date=ship_date, mode=mode, equipment=equipment, service_scope=service_scope,
                origin_type=origin_type, origin_code=origin_code, dest_type=dest_type, dest_code=dest_code,
                carrier_id=carrier_id, reefer=reefer, flatrack=flatrack, over_height=oh,
                over_width=ow, over_height_width=ohw, dg=dg, weight_kg=weight_kg,
                volume_m3=volume_m3, miles=miles or None, containers_count=containers_count or None,
            )
            card = select_best_rate_card(cards.to_dict("records"), shipment)
            if not card:
                st.warning("No matching active/date-valid rate_card found.")
            else:
                st.write("Picked rate card")
                st.json({k: card[k] for k in ["id", "carrier_id", "mode", "equipment", "service_scope", "origin_type", "origin_code", "dest_type", "dest_code", "effective_from", "priority"] if k in card})
                result = compute_rate_total(card, charges.to_dict("records"), shipment)
                st.dataframe(pd.DataFrame(result["items"]), width="stretch")
                st.success(f"Grand total: {result['grand_total']} {result['currency']}")

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
            errors = require_cols(edited, ["part_number", "need_date", "qty"]) + validate_positive(edited, ["qty"], allow_zero=True) + validate_dates(edited, ["need_date"])
            if errors:
                st.error("; ".join(errors))
            else:
                ok, err = save_grid("demand_lines", source, edited, ["id"])
                if ok:
                    st.success("Demand lines saved")
                else:
                    st.error(f"Could not save demand lines: {err}")

    elif admin_screen == "Data management":
        st.subheader("Bulk export / import")
        c1, c2, c3 = st.columns(3)
        with c1:
            full_blob = export_data_bundle("full")
            st.download_button(
                "Download full bundle",
                data=full_blob,
                file_name=f"hwamul_full_{date.today().isoformat()}.json",
                mime="application/json",
            )
        with c2:
            recent_blob = export_data_bundle("recent")
            st.download_button(
                "Download recent master data",
                data=recent_blob,
                file_name=f"hwamul_recent_{date.today().isoformat()}.json",
                mime="application/json",
            )
        with c3:
            history_blob = export_data_bundle("history")
            st.download_button(
                "Download history-only",
                data=history_blob,
                file_name=f"hwamul_history_{date.today().isoformat()}.json",
                mime="application/json",
            )

        st.divider()
        st.caption("Import is an upsert by business key: existing rows are updated, new rows inserted.")
        bundle_upload = st.file_uploader("Upload export bundle (.json)", type=["json"], key="bundle_upload")
        if st.button("Import bundle", key="import_bundle"):
            if bundle_upload is None:
                st.warning("Choose a bundle file first")
            else:
                try:
                    stats = import_data_bundle(bundle_upload.getvalue())
                    if stats:
                        st.success("Imported rows: " + ", ".join([f"{k}={v}" for k, v in stats.items()]))
                    else:
                        st.info("Bundle was valid but contained no rows to import")
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Import failed: {exc}")

        st.divider()
        st.subheader("Cleanup tools")
        cutoff = st.date_input("Delete demand history before", value=date.today(), key="purge_cutoff")
        if st.button("Purge historical demand", key="purge_demand"):
            deleted = purge_demand_before(cutoff.isoformat())
            st.success(
                "Deleted rows: "
                f"demand_lines={deleted['demand_lines']}, tranche_allocations={deleted['tranche_allocations']}"
            )
        if st.button("Compact database (VACUUM)", key="vacuum_db"):
            vacuum_db()
            st.success("Database compacted")

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
            pack_rows = packs[(packs["part_number"] == d["part_number"]) & (packs["is_default"] == 1)]
            p = pack_rows.iloc[0] if not pack_rows.empty else packs[packs["part_number"] == d["part_number"]].iloc[0]
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
            pack_rows = packs[(packs["part_number"] == d["part_number"]) & (packs["is_default"] == 1)]
            p = pack_rows.iloc[0] if not pack_rows.empty else packs[packs["part_number"] == d["part_number"]].iloc[0]
            rule = PackagingRule(**{k: p[k] for k in PackagingRule.__dataclass_fields__.keys()})
            need_date = pd.to_datetime(d["need_date"]).date()
            coo = d["coo_override"] if pd.notna(d["coo_override"]) else read_table("sku_master").set_index("part_number").loc[d["part_number"], "default_coo"]

            eq_by_mode: dict[str, list[Equipment]] = {}
            for mode, g in eq.groupby("mode"):
                eq_by_mode[mode] = [equipment_from_row(x) for x in g.to_dict("records")]

            lead_tbl = {(r["country_of_origin"], r["mode"]): int(r["lead_days"]) for _, r in lead.iterrows()}
            part_number_ov = {(r["part_number"], r["mode"]): int(r["lead_days"]) for _, r in lead_ov.iterrows()}
            recs = recommend_modes(
                part_number=d["part_number"],
                coo=coo,
                need_date=need_date,
                ordered_units=d["qty"],
                pack_rule=rule,
                equipment_by_mode=eq_by_mode,
                rates=rates.to_dict("records"),
                lead_table=lead_tbl,
                part_number_lead_override=part_number_ov,
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
        excess = pd.DataFrame(columns=["part_number", "tranche", "excess_units"])

        def dl(df: pd.DataFrame, name: str):
            st.download_button(name, data=df.to_csv(index=False).encode(), file_name=name, mime="text/csv")

        dl(demand, "shipment_plan.csv")
        dl(booking, "booking_summary.csv")
        dl(excess, "excess_report.csv")

st.caption("All data stored locally in SQLite planner.db")
