from __future__ import annotations

import io
import sqlite3
from math import ceil
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

from constraints_engine import max_units_per_conveyance
from fit_engine import equipment_count_for_packs

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
    map_import_demand_rows,
    resolve_pack_rule_for_demand,
    normalize_pack_dimension_to_meters,
)
from models import Equipment, PackagingRule
from planner import allocate_tranches, build_shipments, recommend_modes, customs_report, phase_cost_rollup, norm_mode
from rate_engine import RateTestInput, compute_rate_total, select_best_rate_card
from seed import TEMPLATE_SPECS, ensure_templates, seed_if_empty
from field_specs import TABLE_SPECS, build_help_text, field_guide_df, table_column_config
from validators import require_cols, validate_dates, validate_positive, validate_with_specs

st.set_page_config(page_title="Logistics Planner", layout="wide")
run_migrations()
seed_if_empty()
ensure_templates()


def read_table(name: str) -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql_query(f"SELECT * FROM {name}", conn)




def read_sku_catalog() -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql_query(
        """
        SELECT
            sm.sku_id,
            sm.part_number,
            sm.supplier_id,
            s.supplier_code,
            s.supplier_name,
            sm.plant_code,
            sm.supplier_duns,
            sm.description,
            sm.source_location,
            sm.incoterm,
            sm.uom,
            sm.default_coo,
            sm.part_number || ' [' || s.supplier_code || ' @ ' || sm.plant_code || ']' AS sku_label
        FROM sku_master sm
        JOIN suppliers s ON s.supplier_id = sm.supplier_id
        ORDER BY sm.part_number, s.supplier_code
        """,
        conn,
    )

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
        name=row.get("equipment_code") or row.get("name"),
        mode=row.get("mode"),
        length_m=float(row.get("length_m") or 0),
        width_m=float(row.get("width_m") or 0),
        height_m=float(row.get("height_m") or 0),
        max_payload_kg=float(row.get("max_payload_kg") or 0),
        volumetric_factor=float(row["volumetric_factor"]) if pd.notna(row.get("volumetric_factor")) else None,
    )

def save_grid(table: str, original: pd.DataFrame, edited: pd.DataFrame, key_cols: list[str]) -> tuple[bool, str | None]:
    conn = get_conn()
    table_cols = {
        row[1]
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    original_db = original[[c for c in original.columns if c in table_cols]].copy()
    edited_db = edited[[c for c in edited.columns if c in table_cols]].copy()
    inserts, updates, deletes = compute_grid_diff(original_db, edited_db, key_cols)
    try:
        with conn:
            upsert_rows(conn, table, pd.concat([inserts, updates], ignore_index=True), key_cols)
            delete_rows(conn, table, deletes, key_cols)
    except sqlite3.IntegrityError as exc:
        return False, str(exc)
    return True, None


def render_about(title: str, body: str) -> None:
    with st.expander("About this page", expanded=False):
        st.markdown(f"**{title}**\n\n{body}")


def render_field_guide(table_key: str) -> None:
    if table_key not in TABLE_SPECS:
        return
    with st.expander("Field guide (columns)", expanded=False):
        st.dataframe(field_guide_df(table_key), width="stretch", hide_index=True)


def render_docs_page(doc_file: str) -> None:
    docs_dir = Path(__file__).resolve().parent / "docs"
    content = (docs_dir / doc_file).read_text(encoding="utf-8")
    st.markdown(content)


st.title("Local Logistics Planning App")
if hasattr(st.sidebar, "page_link"):
    st.sidebar.page_link("pages/quick_plan.py", label="Quick Plan", icon="📦")
    st.sidebar.page_link("pages/batch_plan.py", label="Batch Plan", icon="🚚")
    st.sidebar.page_link("pages/bom_planner.py", label="BOM Planner", icon="🧾")
section = st.sidebar.radio("Section", ["Planner", "Admin", "Docs"])

if section == "Admin":
    st.sidebar.header("Admin")
    admin_screen = st.sidebar.selectbox(
        "Admin screen",
        [
            "Equipment presets",
            "Suppliers",
            "SKUs",
            "Pack rules",
            "Lead times",
            "Rates",
            "Carriers",
            "Rate cards",
            "Customs / HTS",
            "Rate Test",
            "Phase defaults",
            "Lanes",
            "Demand entry",
            "Data management",
        ],
    )

    if admin_screen == "Equipment presets":
        render_about(
            "Equipment presets",
            """Manage equipment dimensions and payload caps used by recommendation and shipment builders.

Prerequisites: none.

Steps: 1) Add/update rows. 2) Keep size/payload values non-negative. 3) Save changes.

Example: mode OCEAN, name 40DV, max_payload_kg 26700.""",
        )
        source = read_table("equipment_presets")
        render_field_guide("equipment")
        edited = st.data_editor(source, num_rows="dynamic", width="stretch", column_config=table_column_config("equipment"))
        if st.button("Save changes", key="save_eq"):
            errors = validate_with_specs("equipment", edited)
            if errors:
                st.error("; ".join(errors))
            else:
                ok, err = save_grid("equipment_presets", source, edited, ["id"])
                if ok:
                    st.success("Equipment presets saved")
                else:
                    st.error(f"Could not save equipment presets: {err}")

    elif admin_screen == "Suppliers":
        render_about(
            "Suppliers",
            """Create supplier master records used by supplier-specific SKUs and pack rules.

Prerequisites: none.

Steps: 1) Enter supplier_code and supplier_name. 2) Capture Incoterms reference text when available. 3) Save.

Example: supplier_code MAEU, supplier_name Maersk Line, incoterms_ref FOB SHANGHAI.""",
        )
        source = read_table("suppliers")
        render_field_guide("suppliers")
        edited = st.data_editor(source, num_rows="dynamic", width="stretch", column_config=table_column_config("suppliers"))
        if st.button("Save changes", key="save_suppliers"):
            errors = validate_with_specs("suppliers", edited)
            if errors:
                st.error("; ".join(errors))
            else:
                ok, err = save_grid("suppliers", source, edited, ["supplier_id"])
                if ok:
                    st.success("Suppliers saved")
                else:
                    st.error(f"Could not save suppliers: {err}")

    elif admin_screen == "SKUs":
        source = read_sku_catalog()
        q = st.text_input("Search SKU or description", help="Filter rows by part number, supplier code/name, or description. Example: PN_10001")
        filtered_source = source[source.astype(str).apply(lambda c: c.str.contains(q, case=False, na=False)).any(axis=1)] if q else source
        render_about("SKUs", "Define supplier-and-plant specific SKU records with logistics profile defaults.\n\nPrerequisites: at least one supplier.\n\nSteps: 1) Search optional. 2) Edit part_number, plant_code, COO, source location, Incoterm, UOM, and optional DUNS. 3) Save.\n\nExample: PN_10001 + supplier_id 1 + plant_code US_TX_DAL + source_location CNSHA + incoterm FOB + uom KG + COO CN + supplier_duns 123456789.")
        render_field_guide("skus")
        edited = st.data_editor(
            filtered_source,
            num_rows="dynamic",
            width="stretch",
            column_config=table_column_config("skus"),
            disabled=["supplier_code", "supplier_name", "sku_label"],
        )
        if st.button("Save changes", key="save_sku"):
            errors = validate_with_specs("skus", edited)
            if errors:
                st.error("; ".join(errors))
            else:
                # When search is active, only persist edits for the visible slice.
                # Diffing against the full table would treat hidden rows as deletions.
                ok, err = save_grid("sku_master", filtered_source, edited, ["sku_id"])
                if ok:
                    st.success("SKUs saved")
                    st.rerun()
                else:
                    st.error(f"Could not save SKUs: {err}. If changing existing SKU codes, update dependent pack and demand rows first.")

    elif admin_screen == "Pack rules":
        sku_catalog = read_sku_catalog()
        if sku_catalog.empty:
            st.warning("No SKUs found. Create a SKU first.")
            st.stop()

        render_about(
            "Standard pack profile",
            "Maintain one standard pack profile per supplier-specific SKU.\n\n"
            "Prerequisites: SKU exists.\n\n"
            "Steps: 1) Pick SKU. 2) Enter standard pack dimensions/weight. 3) Set stacking and conveyance allowances. 4) Save.\n\n"
            "Notes: ordering rounds up to whole packs and always enforces at least 1 pack.",
        )
        search = st.text_input("Search SKU (PN, supplier code/name, description)", key="pack_sku_search", help="Filter SKU list before selecting. Example: MAEU")
        if search:
            mask = (
                sku_catalog["part_number"].str.contains(search, case=False, na=False)
                | sku_catalog["supplier_code"].str.contains(search, case=False, na=False)
                | sku_catalog["supplier_name"].str.contains(search, case=False, na=False)
                | sku_catalog["description"].fillna("").str.contains(search, case=False, na=False)
            )
            sku_catalog = sku_catalog[mask]
        if sku_catalog.empty:
            st.info("No SKU matches your search.")
            st.stop()

        sku_catalog = sku_catalog.copy()
        sku_catalog["select_label"] = (
            sku_catalog["part_number"]
            + " | SUP:" + sku_catalog["supplier_code"]
            + " (" + sku_catalog["supplier_name"] + ")"
            + " | COO:" + sku_catalog["default_coo"]
            + " | " + sku_catalog["description"].fillna("")
        )
        sku_options = sku_catalog["sku_id"].tolist()
        selected_sku_id = st.selectbox(
            "Select SKU", help="Select the supplier-specific SKU whose pack rules you want to edit. Example: PN_10001 [MAEU]",
            options=sku_options,
            format_func=lambda sid: sku_catalog.loc[sku_catalog["sku_id"] == sid, "select_label"].iloc[0],
            key="selected_sku_id",
        )

        selected_sku = sku_catalog[sku_catalog["sku_id"] == selected_sku_id].iloc[0]
        conn = get_conn()
        c1, c2, c3, c4 = st.columns(4)
        c1.text_input("PN", value=str(selected_sku["part_number"]), disabled=True)
        c2.text_input("Supplier", value=f"{selected_sku['supplier_code']} ({selected_sku['supplier_name']})", disabled=True)
        c3.text_input("COO", value=str(selected_sku["default_coo"]), disabled=True)
        c4.text_input("Description", value=str(selected_sku["description"] or ""), disabled=True)

        st.markdown("#### Conveyance restrictions for selected SKU")
        equipment_rules = pd.read_sql_query(
            """
            SELECT
                ep.id AS equipment_id,
                ep.equipment_code,
                ep.name,
                ep.mode,
                COALESCE(ser.allowed, 1) AS allowed
            FROM equipment_presets ep
            LEFT JOIN sku_equipment_rules ser
              ON ser.equipment_id = ep.id
             AND ser.sku_id = ?
            WHERE ep.active = 1
            ORDER BY UPPER(ep.mode), UPPER(ep.equipment_code), UPPER(ep.name)
            """,
            conn,
            params=(int(selected_sku_id),),
        )
        equipment_rules["allowed"] = equipment_rules["allowed"].fillna(1).astype(int).astype(bool)
        edited_rules = st.data_editor(
            equipment_rules,
            width="stretch",
            hide_index=True,
            disabled=["equipment_id", "equipment_code", "name", "mode"],
            column_config={
                "equipment_id": st.column_config.NumberColumn("equipment_id"),
                "equipment_code": st.column_config.TextColumn("equipment_code"),
                "name": st.column_config.TextColumn("equipment"),
                "mode": st.column_config.TextColumn("mode"),
                "allowed": st.column_config.CheckboxColumn("allowed"),
            },
            key=f"sku_eq_rules_{int(selected_sku_id)}",
        )
        if st.button("Save conveyance restrictions", key=f"save_sku_eq_rules_{int(selected_sku_id)}"):
            with conn:
                conn.execute("DELETE FROM sku_equipment_rules WHERE sku_id = ?", (int(selected_sku_id),))
                conn.executemany(
                    "INSERT INTO sku_equipment_rules(sku_id, equipment_id, allowed) VALUES (?, ?, ?)",
                    [
                        (int(selected_sku_id), int(row["equipment_id"]), 1 if bool(row["allowed"]) else 0)
                        for _, row in edited_rules.iterrows()
                    ],
                )
            st.success("Conveyance restrictions saved")

        pack_source = pd.read_sql_query(
            """
            SELECT
                id,
                sku_id,
                pack_name,
                pack_type,
                is_default,
                units_per_pack,
                kg_per_unit,
                pack_tare_kg,
                dim_l_m * 100.0 AS dim_l_cm,
                dim_w_m * 100.0 AS dim_w_cm,
                dim_h_m * 100.0 AS dim_h_cm,
                min_order_packs,
                increment_packs,
                stackable,
                max_stack
            FROM packaging_rules
            WHERE sku_id = ?
            ORDER BY is_default DESC, id
            """,
            get_conn(),
            params=(selected_sku_id,),
        )
        if pack_source.empty:
            with conn:
                conn.execute(
                    """
                    INSERT INTO packaging_rules(
                        sku_id, pack_name, pack_type, is_default, units_per_pack, kg_per_unit, pack_tare_kg,
                        dim_l_m, dim_w_m, dim_h_m, min_order_packs, increment_packs, stackable, max_stack
                    ) VALUES (?, 'STANDARD_PACK', 'STANDARD', 1, 1, 1, 0, 0.1, 0.1, 0.1, 1, 1, 1, NULL)
                    """,
                    (selected_sku_id,),
                )
            st.info("Created a default standard pack profile for this SKU. Update fields below and save.")
            st.rerun()

        default_pack = pack_source.sort_values(["is_default", "id"], ascending=[False, True]).iloc[0]
        if len(pack_source) > 1:
            st.warning("Multiple pack rules exist for this SKU. The standard pack profile below uses the default row.")

        standard_pack_kg = float(default_pack["units_per_pack"] or 0) * float(default_pack["kg_per_unit"] or 0) + float(default_pack["pack_tare_kg"] or 0)
        with st.form("standard_pack_form"):
            st.markdown("#### Standard pack profile")
            f1, f2, f3, f4 = st.columns(4)
            f1.text_input("SKU (part number)", value=str(selected_sku["part_number"]), disabled=True)
            f2.text_input("Vendor", value=f"{selected_sku['supplier_code']} ({selected_sku['supplier_name']})", disabled=True)
            dim_l_cm = f3.number_input("Length (cm)", min_value=0.01, value=float(default_pack["dim_l_cm"]), step=1.0)
            dim_w_cm = f4.number_input("Width (cm)", min_value=0.01, value=float(default_pack["dim_w_cm"]), step=1.0)

            g1, g2, g3, g4 = st.columns(4)
            dim_h_cm = g1.number_input("Height (cm)", min_value=0.01, value=float(default_pack["dim_h_cm"]), step=1.0)
            standard_pack_kg_input = g2.number_input("Standard pack weight (kg)", min_value=0.001, value=max(0.001, standard_pack_kg), step=0.5)
            stackable = g3.checkbox("Stackable", value=bool(default_pack["stackable"]))
            max_stack = g4.number_input("Max stack", min_value=1, value=int(default_pack["max_stack"] or 1), step=1, disabled=not stackable)

            submitted = st.form_submit_button("Save standard pack profile")

        if submitted:
            with conn:
                conn.execute("UPDATE packaging_rules SET is_default = 0 WHERE sku_id = ?", (int(selected_sku_id),))
                conn.execute(
                    """
                    UPDATE packaging_rules
                    SET
                        pack_name = ?,
                        pack_type = 'STANDARD',
                        is_default = 1,
                        units_per_pack = 1,
                        kg_per_unit = ?,
                        pack_tare_kg = 0,
                        dim_l_m = ?,
                        dim_w_m = ?,
                        dim_h_m = ?,
                        min_order_packs = 1,
                        increment_packs = 1,
                        stackable = ?,
                        max_stack = ?
                    WHERE id = ?
                    """,
                    (
                        f"STD_{selected_sku['part_number']}",
                        float(standard_pack_kg_input),
                        normalize_pack_dimension_to_meters(dim_l_cm),
                        normalize_pack_dimension_to_meters(dim_w_cm),
                        normalize_pack_dimension_to_meters(dim_h_cm),
                        1 if stackable else 0,
                        int(max_stack) if stackable else None,
                        int(default_pack["id"]),
                    ),
                )
            st.success("Standard pack profile saved")
            st.rerun()

        st.divider()
        st.markdown("#### Bulk import standard-pack master data (SKU + vendor)")
        st.caption(
            "Use this file to manage pack master data in one place and map each row to a supplier-specific SKU via part_number + supplier_code. "
            "For standard-pack modeling, this import fixes units_per_pack=1 and stores your pack weight in standard_pack_kg."
        )
        pack_mdm_template = Path("templates") / "pack_mdm_template.csv"
        if pack_mdm_template.exists():
            st.download_button(
                "Download pack_mdm_template.csv",
                data=pack_mdm_template.read_bytes(),
                file_name="pack_mdm_template.csv",
                mime="text/csv",
            )
        pack_upload = st.file_uploader(
            "Upload pack master data csv",
            type=["csv"],
            key="pack_mdm_upload",
            help="Columns: part_number, supplier_code, standard_pack_kg, dim_l_cm, dim_w_cm, dim_h_cm, stackable, optional max_stack/pack_name/is_default.",
        )
        if pack_upload is not None:
            imported_pack = pd.read_csv(pack_upload)
            st.write("Imported pack master preview (editable)")
            imported_pack_edit = st.data_editor(imported_pack, num_rows="dynamic", width="stretch", key="pack_mdm_editor")
            if st.button("Apply pack master import", key="apply_pack_mdm"):
                required_pack_cols = [name for name, spec in TABLE_SPECS["pack_rules_import"].items() if spec.required]
                missing_pack_cols = [col for col in required_pack_cols if col not in imported_pack_edit.columns]
                if missing_pack_cols:
                    st.error(
                        "Pack master import is missing required column(s): "
                        + ", ".join(missing_pack_cols)
                        + ". Expected required columns: "
                        + ", ".join(required_pack_cols)
                    )
                    st.stop()

                import_errors = validate_with_specs("pack_rules_import", imported_pack_edit)
                if import_errors:
                    st.error("; ".join(import_errors))
                    st.stop()

                sku_lookup = sku_catalog[["sku_id", "part_number", "supplier_code"]].drop_duplicates()
                merged_pack = imported_pack_edit.merge(sku_lookup, on=["part_number", "supplier_code"], how="left")
                unresolved = merged_pack[merged_pack["sku_id"].isna()][["part_number", "supplier_code"]].drop_duplicates()
                if not unresolved.empty:
                    missing_keys = ", ".join([f"{r.part_number}/{r.supplier_code}" for r in unresolved.itertuples(index=False)])
                    st.error(f"Could not map these part_number + supplier_code rows to an SKU: {missing_keys}")
                    st.stop()

                merged_pack = merged_pack.copy()
                merged_pack["pack_name"] = merged_pack["pack_name"].astype(str) if "pack_name" in merged_pack.columns else ""
                merged_pack["pack_name"] = merged_pack.apply(
                    lambda r: r["pack_name"] if str(r["pack_name"]).strip() and str(r["pack_name"]).strip().lower() != "nan" else f"STD_{r['part_number']}",
                    axis=1,
                )
                if "is_default" not in merged_pack.columns:
                    merged_pack["is_default"] = 1
                merged_pack["is_default"] = merged_pack["is_default"].fillna(1).astype(int)

                upsert_df = pd.DataFrame(
                    {
                        "sku_id": merged_pack["sku_id"].astype(int),
                        "pack_name": merged_pack["pack_name"],
                        "pack_type": "STANDARD",
                        "is_default": merged_pack["is_default"],
                        "units_per_pack": 1.0,
                        "kg_per_unit": pd.to_numeric(merged_pack["standard_pack_kg"], errors="coerce"),
                        "pack_tare_kg": 0.0,
                        "dim_l_m": merged_pack["dim_l_cm"].apply(normalize_pack_dimension_to_meters),
                        "dim_w_m": merged_pack["dim_w_cm"].apply(normalize_pack_dimension_to_meters),
                        "dim_h_m": merged_pack["dim_h_cm"].apply(normalize_pack_dimension_to_meters),
                        "min_order_packs": 1,
                        "increment_packs": 1,
                        "stackable": merged_pack["stackable"].fillna(0).astype(int),
                        "max_stack": merged_pack["max_stack"] if "max_stack" in merged_pack.columns else None,
                    }
                )

                with conn:
                    for sku_id in upsert_df.loc[upsert_df["is_default"] == 1, "sku_id"].drop_duplicates().tolist():
                        conn.execute("UPDATE packaging_rules SET is_default = 0 WHERE sku_id = ?", (int(sku_id),))
                    upsert_rows(conn, "packaging_rules", upsert_df, ["sku_id", "pack_name"])
                st.success(f"Imported {len(upsert_df)} standard pack profile row(s)")
                st.rerun()

        with st.expander("Advanced pack rule editor (optional)", expanded=False):
            render_field_guide("pack_rules")
            edited = st.data_editor(
                pack_source,
                num_rows="dynamic",
                width="stretch",
                column_order=[
                    "id", "sku_id", "pack_name", "pack_type", "units_per_pack", "kg_per_unit", "pack_tare_kg",
                    "dim_l_cm", "dim_w_cm", "dim_h_cm", "min_order_packs", "increment_packs", "stackable", "max_stack", "is_default"
                ],
                disabled=["sku_id"],
                key="pack_rules_editor",
                column_config=table_column_config("pack_rules"),
            )

            if st.button("Save advanced changes", key="save_pack"):
                errors = validate_with_specs("pack_rules", edited)
                if edited["is_default"].sum() > 1:
                    errors.append("Only one default pack rule is allowed per SKU")
                if errors:
                    st.error("; ".join(errors))
                else:
                    original_for_save = pack_source.rename(columns={"dim_l_cm": "dim_l_m", "dim_w_cm": "dim_w_m", "dim_h_cm": "dim_h_m"})
                    edited_for_save = edited.rename(columns={"dim_l_cm": "dim_l_m", "dim_w_cm": "dim_w_m", "dim_h_cm": "dim_h_m"})
                    for dim_col in ["dim_l_m", "dim_w_m", "dim_h_m"]:
                        original_for_save[dim_col] = original_for_save[dim_col].apply(normalize_pack_dimension_to_meters)
                        edited_for_save[dim_col] = edited_for_save[dim_col].apply(normalize_pack_dimension_to_meters)
                    ok, err = save_grid("packaging_rules", original_for_save, edited_for_save, ["id"])
                    if ok:
                        st.success("Pack rules saved")
                        st.rerun()
                    else:
                        st.error(f"Could not save pack rules: {err}")

    elif admin_screen == "Lead times":
        render_about("Lead times", """Define default lead times by COO/mode and SKU overrides.

Prerequisites: SKU exists for overrides.

Steps: 1) Update lead_days rows. 2) Save both grids.

Example: CN + OCEAN = 35 days.""")
        lt_source = read_table("lead_times")
        render_field_guide("lead_times")
        lt_edited = st.data_editor(lt_source, num_rows="dynamic", width="stretch", column_config=table_column_config("lead_times"))
        ov_source = read_table("lead_time_overrides")
        with st.expander("Field guide (columns)", expanded=False):
            st.markdown("- sku_id: int >= 1 (example 10)\n- mode: text (example OCEAN)\n- lead_days: int >= 0 (example 35)")
        ov_edited = st.data_editor(ov_source, num_rows="dynamic", width="stretch")
        if st.button("Save changes", key="save_lead"):
            errors = validate_with_specs("lead_times", lt_edited)
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
        render_about("Rates", "Legacy rates table for mode pricing scenarios.\n\nPrerequisites: none.\n\nSteps: edit rate rows and save.\n\nExample: OCEAN + FLAT_PER_CONTAINER + 4000.")
        source = read_table("rates")
        with st.expander("Field guide (columns)", expanded=False):
            st.markdown("Use numeric `rate_value >= 0` and date columns in `YYYY-MM-DD` when provided.")
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
        render_about("Carriers", "Manage carrier master used by rate cards.\n\nPrerequisites: none.\n\nSteps: 1) Enter code and name. 2) Set active flag. 3) Save.\n\nExample: code MAEU, name Maersk.")
        source = read_table("carrier")
        with st.expander("Field guide (columns)", expanded=False):
            st.markdown("- code: text 2-32 uppercase/code style (example MAEU)\n- name: text label\n- is_active: bool 0/1")
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
        render_about("Rate cards", "Maintain base rates and accessorial charges with effective dates.\n\nPrerequisites: carriers optional, route definitions known.\n\nSteps: 1) Edit rate cards. 2) Select card. 3) Edit charges. 4) Save.\n\nExample: OCEAN 40DV P2D USLAX->CNSHA.")
        cards_source = read_table("rate_card")
        render_field_guide("rate_cards")
        cards_edited = st.data_editor(cards_source, num_rows="dynamic", width="stretch", column_config=table_column_config("rate_cards"))
        st.divider()
        st.subheader("Rate charges")
        card_options = cards_edited["id"].dropna().astype(int).tolist() if not cards_edited.empty else []
        charges_source = read_table("rate_charge")
        selected_card_id = None
        if card_options:
            selected_card_id = st.selectbox("Select rate_card", card_options, help="Choose a saved rate card id before editing charges.")
            charge_view = charges_source[charges_source["rate_card_id"] == selected_card_id]
        else:
            st.info("Create and save at least one rate card before editing charges.")
            charge_view = charges_source.iloc[0:0]
        render_field_guide("rate_charges")
        charges_edited = st.data_editor(charge_view, num_rows="dynamic", width="stretch", column_config=table_column_config("rate_charges"))

        if st.button("Save rate master", key="save_rate_master"):
            cards_edited = normalize_bools(cards_edited, ["is_active"])
            card_required = [
                "mode", "service_scope", "equipment", "dim_class",
                "origin_type", "origin_code", "dest_type", "dest_code",
                "currency", "uom_pricing", "base_rate", "effective_from",
            ]
            errors = validate_with_specs("rate_cards", cards_edited)
            errors += validate_dates(cards_edited, ["effective_from", "effective_to", "contract_start", "contract_end"])
            errors += validate_date_ranges(cards_edited, "effective_from", "effective_to", "Rate cards")
            errors += validate_date_ranges(cards_edited, "contract_start", "contract_end", "Rate cards")

            if not charges_edited.empty:
                errors += validate_with_specs("rate_charges", charges_edited)
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

    elif admin_screen == "Customs / HTS":
        render_about(
            "Customs / HTS",
            "Track HTS and tariff rates for customs reporting over time, including required documentation and section flags. Capture both COO and ship-from country, where ship-from is used for rate reference.\n\n"
            "Prerequisites: optional SKU linkage when a material maps to a supplier-specific SKU.\n\n"
            "Steps: 1) Add HTS rows by effective dates. 2) Capture section 232/301 flags and documentation requirements. 3) Save.",
        )
        source = read_table("customs_hts_rates")
        render_field_guide("customs_hts")
        edited = st.data_editor(
            source,
            num_rows="dynamic",
            width="stretch",
            column_config=table_column_config("customs_hts"),
        )
        if st.button("Save changes", key="save_customs_hts"):
            edited = normalize_bools(
                edited,
                [
                    "section_232",
                    "section_301",
                    "domestic_trucking_required",
                    "port_to_ramp_required",
                    "special_documentation_required",
                ],
            )
            errors = validate_with_specs("customs_hts", edited)
            errors += validate_date_ranges(edited, "effective_from", "effective_to", "Customs HTS")
            if errors:
                st.error("; ".join(sorted(set(errors))))
            else:
                ok, err = save_grid("customs_hts_rates", source, edited, ["id"])
                if ok:
                    st.success("Customs HTS rates saved")
                else:
                    st.error(f"Could not save customs HTS rates: {err}")

    elif admin_screen == "Rate Test":
        render_about("Rate Test", "Test how a hypothetical shipment selects a rate card and computes total cost.\n\nPrerequisites: active rate cards/charges exist.\n\nSteps: fill shipment fields, run test, review selected card and line items.\n\nExample: OCEAN 40DV P2D USLAX to CNSHA.")
        cards = read_table("rate_card")
        carriers = read_table("carrier")
        charges = read_table("rate_charge")
        with st.form("rate_test_form"):
            c1, c2, c3 = st.columns(3)
            with c1:
                ship_date = st.date_input("Ship date", value=date.today(), help="Shipment date in YYYY-MM-DD. Example: 2026-01-15")
                mode = st.text_input("Mode", value="OCEAN", help=build_help_text("rate_cards", "mode"))
                equipment = st.text_input("Equipment", value="CNT_40_DRY_STD", help=build_help_text("rate_cards", "equipment"))
                service_scope = st.selectbox("Service scope", ["P2P", "P2D", "D2P", "D2D"], help=build_help_text("rate_cards", "service_scope"))
            with c2:
                origin_type = st.text_input("Origin type", value="PORT", help=build_help_text("rate_cards", "origin_type"))
                origin_code = st.text_input("Origin code", value="USLAX", help=build_help_text("rate_cards", "origin_code"))
                dest_type = st.text_input("Dest type", value="PORT", help=build_help_text("rate_cards", "dest_type"))
                dest_code = st.text_input("Dest code", value="CNSHA", help=build_help_text("rate_cards", "dest_code"))
            with c3:
                carrier_id = st.selectbox("Carrier", [None] + carriers.get("id", pd.Series(dtype=int)).dropna().astype(int).tolist())
                weight_kg = st.number_input("Weight kg", min_value=0.0, value=1000.0, help=">= 0. Example: 12000")
                volume_m3 = st.number_input("Volume m3", min_value=0.0, value=10.0, help=">= 0. Example: 28.5")
                containers_count = st.number_input("Containers count", min_value=0.0, value=1.0, help=">= 0. Example: 2")

            f1, f2 = st.columns(2)
            with f1:
                reefer = st.checkbox("Reefer")
                flatrack = st.checkbox("Flatrack")
                dg = st.checkbox("DG")
            with f2:
                oh = st.checkbox("Over Height")
                ow = st.checkbox("Over Width")
                ohw = st.checkbox("Over Height + Width")
                miles = st.number_input("Miles", min_value=0.0, value=0.0, help=">= 0. Example: 320")
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

    elif admin_screen == "Phase defaults":
        render_about("Phase defaults", "Configure default mode/service scope/manual lead by phase.")
        source = read_table("phase_defaults")
        if source.empty:
            source = pd.DataFrame({"phase": ["Trial1", "Trial2", "Sample", "Speed-up", "Validation", "SOP"], "default_mode": ["AIR", "AIR", "OCEAN", "OCEAN", "OCEAN", "OCEAN"], "default_service_scope": ["P2D"] * 6, "manual_lead_override": [None] * 6})
        edited = st.data_editor(source, num_rows="dynamic", width="stretch")
        if st.button("Save changes", key="save_phase_defaults"):
            ok, err = save_grid("phase_defaults", source, edited, ["phase"])
            if ok:
                st.success("Phase defaults saved")
            else:
                st.error(f"Could not save phase defaults: {err}")

    elif admin_screen == "Lanes":
        render_about("Lanes", "Default service scope/miles per route.")
        source = read_table("lanes")
        edited = st.data_editor(source, num_rows="dynamic", width="stretch")
        if st.button("Save changes", key="save_lanes"):
            ok, err = save_grid("lanes", source, edited, ["id"])
            if ok:
                st.success("Lanes saved")
            else:
                st.error(f"Could not save lanes: {err}")

    elif admin_screen == "Demand entry":
        render_about("Demand entry", "Create/edit demand lines and optional CSV imports.\n\nPrerequisites: suppliers and SKUs exist.\n\nSteps: 1) Add lines with sku_id, need_date, qty. 2) Optionally import CSV. 3) Save.\n\nExample: sku_id 10, need_date 2026-04-01, qty 1200.")
        source = pd.read_sql_query("""
        SELECT d.*, sm.part_number, s.supplier_code, sm.part_number || ' [' || s.supplier_code || ']' AS sku_label
        FROM demand_lines d
        JOIN sku_master sm ON sm.sku_id = d.sku_id
        JOIN suppliers s ON s.supplier_id = sm.supplier_id
        ORDER BY d.id
        """, get_conn())
        render_field_guide("demand_grid")
        edited = st.data_editor(source, num_rows="dynamic", width="stretch", column_config=table_column_config("demand_grid"), disabled=["part_number", "supplier_code", "sku_label"])
        demand_template = Path("templates") / "demand_template.csv"
        if demand_template.exists():
            st.download_button("Download demand_template.csv", data=demand_template.read_bytes(), file_name="demand_template.csv", mime="text/csv")
        st.caption("Optional CSV import")
        upload = st.file_uploader("Upload demand csv", type=["csv"], help="CSV with part_number, supplier_code(optional), need_date YYYY-MM-DD, qty, optional phase/mode_override/service_scope/miles.")
        if upload is not None:
            imported = pd.read_csv(upload)
            st.write("Imported preview (editable)")
            with st.expander("Field guide (columns)", expanded=False):
                st.markdown("Imported demand columns must include part_number, need_date (YYYY-MM-DD), qty (>=0), and optional supplier_code/coo_override/priority/notes/phase/mode_override/service_scope/miles.")
            imported_edit = st.data_editor(imported, num_rows="dynamic", width="stretch")
            if st.button("Append imported rows"):
                import_frame = imported_edit.copy()
                required_import_cols = [name for name, spec in TABLE_SPECS["demand_import"].items() if spec.required]
                missing_required = [col for col in required_import_cols if col not in import_frame.columns]
                if missing_required:
                    st.error(
                        "Demand import is missing required column(s): "
                        + ", ".join(missing_required)
                        + ". Expected required columns: "
                        + ", ".join(required_import_cols)
                    )
                    st.stop()

                import_errors = validate_with_specs("demand_import", import_frame) + validate_dates(import_frame, ["need_date"])
                if import_errors:
                    st.error("; ".join(import_errors))
                    st.stop()

                sku_catalog = read_sku_catalog()
                supplier_choices = st.session_state.setdefault("import_supplier_map", {})
                merged, map_errors = map_import_demand_rows(import_frame, sku_catalog, supplier_choices)
                if "supplier_code" not in import_frame.columns:
                    ambiguous_parts = merged.groupby("part_number")["sku_id"].nunique(dropna=True)
                    for pn in ambiguous_parts[ambiguous_parts > 1].index.tolist():
                        sup_opts = sku_catalog[sku_catalog["part_number"] == pn]["supplier_code"].tolist()
                        current = supplier_choices.get(pn, sup_opts[0] if sup_opts else None)
                        supplier_choices[pn] = st.selectbox(f"Select supplier for {pn}", sup_opts, index=sup_opts.index(current) if current in sup_opts else 0, key=f"imp_sup_{pn}")
                    merged, map_errors = map_import_demand_rows(import_frame, sku_catalog, supplier_choices)
                if map_errors:
                    st.error("; ".join(map_errors))
                else:
                    cols = ["sku_id", "need_date", "qty", "coo_override", "priority", "notes", "phase", "mode_override", "service_scope", "miles"]
                    if "pack_rule_id" in merged.columns:
                        cols.append("pack_rule_id")
                    to_insert = merged[cols]
                    conn = get_conn()
                    with conn:
                        to_insert.to_sql("demand_lines", conn, if_exists="append", index=False)
                    st.success("Imported rows appended")

        paste = st.text_area("Paste CSV grid (header required)")
        if st.button("Append pasted rows") and paste.strip():
            st.warning("Use CSV import with supplier_code to map to sku_id.")

        if st.button("Save changes", key="save_demand"):
            errors = validate_with_specs("demand_grid", edited) + validate_dates(edited, ["need_date"])
            if errors:
                st.error("; ".join(errors))
            else:
                ok, err = save_grid("demand_lines", source, edited, ["id"])
                if ok:
                    st.success("Demand lines saved")
                else:
                    st.error(f"Could not save demand lines: {err}")

    elif admin_screen == "Data management":
        render_about("Data management", "Export/import JSON bundles, purge old demand, and compact DB.\n\nPrerequisites: none.\n\nSteps: download/upload bundle as needed, then run cleanup actions carefully.\n\nExample: export full bundle before large edits.")
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
        cutoff = st.date_input("Delete demand history before", value=date.today(), key="purge_cutoff", help="Deletes demand/tranche rows older than this YYYY-MM-DD date.")
        if st.button("Purge historical demand", key="purge_demand"):
            deleted = purge_demand_before(cutoff.isoformat())
            st.success(
                "Deleted rows: "
                f"demand_lines={deleted['demand_lines']}, tranche_allocations={deleted['tranche_allocations']}"
            )
        if st.button("Compact database (VACUUM)", key="vacuum_db"):
            vacuum_db()
            st.success("Database compacted")

elif section == "Planner":
    alloc_tab, rec_tab, cube_tab, ship_tab, exp_tab = st.tabs(["Allocation", "Recommendations", "Cube Out", "Shipment Builder", "Export"])

    with alloc_tab:
        render_about("Allocation", "Preview tranche allocation for one demand line using selected pack rule.\n\nPrerequisites: demand lines + pack rules exist.\n\nSteps: pick demand line, optional override, edit tranches, review result.\n\nExample: T1 60%, T2 40%.")
        demands = read_table("demand_lines")
        packs = read_table("packaging_rules")
        skus = read_sku_catalog()
        if demands.empty or packs.empty:
            st.info("Need demand_lines and packaging_rules first")
        else:
            line = st.selectbox("Demand line", demands["id"].tolist(), help="Select demand line id to allocate.")
            d = demands[demands["id"] == line].iloc[0]
            conn = get_conn()
            d_row = conn.execute("SELECT * FROM demand_lines WHERE id = ?", (int(d["id"]),)).fetchone()
            p = resolve_pack_rule_for_demand(conn, d_row)
            sku_pack_options = packs[packs["sku_id"] == d["sku_id"]][["id", "pack_name"]]
            selected_override = st.selectbox("Pack rule override", [None] + sku_pack_options["id"].tolist(), format_func=lambda x: "Default" if x is None else sku_pack_options.loc[sku_pack_options["id"] == x, "pack_name"].iloc[0], key=f"rec_pack_override_{line}", help="Optional explicit pack rule for this demand line.")
            if st.button("Save pack override", key=f"save_rec_pack_override_{line}"):
                with conn:
                    conn.execute("UPDATE demand_lines SET pack_rule_id = ? WHERE id = ?", (selected_override, int(d["id"])))
                st.rerun()
            rule = PackagingRule(**{k: p[k] for k in PackagingRule.__dataclass_fields__.keys() if k in p.keys()})

            st.write("Define tranches")
            with st.expander("Field guide (columns)", expanded=False):
                st.markdown("- tranche_name: text (example T1)\n- allocation_type: text percent|units (example percent)\n- allocation_value: decimal >=0 (example 60)")
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
                need_date=pd.to_datetime(d["need_date"]).date(),
                sku_id=int(d["sku_id"]),
            )
            st.dataframe(pd.DataFrame([r.__dict__ for r in rows]), width="stretch")

    with rec_tab:
        render_about("Recommendations", "Calculate mode recommendations from lead times, equipment, and advanced rate cards.")
        demands = read_table("demand_lines")
        packs = read_table("packaging_rules")
        skus = read_sku_catalog()
        eq = pd.read_sql_query("SELECT * FROM equipment_presets WHERE active = 1", get_conn())
        rates = read_table("rates")
        rate_cards = read_table("rate_card")
        rate_charges = read_table("rate_charge")
        lead = read_table("lead_times")
        lead_ov = read_table("lead_time_overrides")
        phase_defaults = read_table("phase_defaults")

        if not demands.empty and not packs.empty and not eq.empty:
            phase_filter = st.selectbox("Phase filter", ["All"] + sorted([str(x) for x in demands.get("phase", pd.Series(dtype=str)).fillna("").unique().tolist() if str(x)]), index=0)
            demand_view = demands if phase_filter == "All" else demands[demands["phase"] == phase_filter]
            if demand_view.empty:
                st.info("No demand lines for selected phase")
            else:
                line = st.selectbox("Line for recommendation", demand_view["id"].tolist(), key="rec_line", help="Select demand line id to evaluate.")
                d = demands[demands["id"] == line].iloc[0]
                conn = get_conn()
                d_row = conn.execute("SELECT * FROM demand_lines WHERE id = ?", (int(d["id"]),)).fetchone()
                p = resolve_pack_rule_for_demand(conn, d_row)
                sku_pack_options = packs[packs["sku_id"] == d["sku_id"]][["id", "pack_name"]]
                selected_override = st.selectbox("Pack rule override", [None] + sku_pack_options["id"].tolist(), format_func=lambda x: "Default" if x is None else sku_pack_options.loc[sku_pack_options["id"] == x, "pack_name"].iloc[0], key=f"alloc_pack_override_{line}")
                if st.button("Save pack override", key=f"save_alloc_pack_override_{line}"):
                    with conn:
                        conn.execute("UPDATE demand_lines SET pack_rule_id = ? WHERE id = ?", (selected_override, int(d["id"])))
                    st.rerun()
                rule = PackagingRule(**{k: p[k] for k in PackagingRule.__dataclass_fields__.keys() if k in p.keys()})
                need_date = pd.to_datetime(d["need_date"]).date()
                coo = d["coo_override"] if pd.notna(d["coo_override"]) else read_table("sku_master").set_index("sku_id").loc[d["sku_id"], "default_coo"]

                eq_by_mode: dict[str, list[Equipment]] = {}
                for mode, g in eq.groupby("mode"):
                    eq_by_mode[norm_mode(mode)] = [equipment_from_row(x) for x in g.to_dict("records")]

                lead_tbl = {(str(r["country_of_origin"]).strip().upper(), norm_mode(r["mode"])): int(r["lead_days"]) for _, r in lead.iterrows()}
                sku_ov = {(int(r["sku_id"]), norm_mode(r["mode"])): int(r["lead_days"]) for _, r in lead_ov.iterrows()}
                phase_cfg = {r["phase"]: r for _, r in phase_defaults.iterrows()} if not phase_defaults.empty else {}
                lanes = read_table("lanes")
                route_info = None
                sku_row = skus[skus["sku_id"] == d["sku_id"]].iloc[0]
                lane_match = lanes[(lanes["origin_code"] == sku_row["supplier_code"]) & (lanes["dest_code"] == sku_row["plant_code"])] if not lanes.empty else pd.DataFrame()
                service_scope = str(d.get("service_scope") or phase_cfg.get(str(d.get("phase") or ""), {}).get("default_service_scope") or "P2P")
                miles = float(d.get("miles")) if pd.notna(d.get("miles")) else None
                if not lane_match.empty:
                    lane = lane_match.iloc[0]
                    if not d.get("service_scope") and pd.notna(lane.get("default_service_scope")):
                        service_scope = str(lane.get("default_service_scope"))
                    if miles is None and pd.notna(lane.get("default_miles")):
                        miles = float(lane.get("default_miles"))
                    route_info = {"supplier_code": sku_row["supplier_code"], "plant_code": sku_row["plant_code"]}

                manual_input = int(st.number_input("Manual lead override", min_value=0, value=int(d.get("manual_lead_override", 0) or 0), help="Optional extra lead days >=0. Example: 3"))
                recs = recommend_modes(
                    sku_id=int(d["sku_id"]),
                    part_number=str(sku_row["part_number"]),
                    coo=coo,
                    need_date=need_date,
                    requested_units=d["qty"],
                    pack_rule=rule,
                    equipment_by_mode=eq_by_mode,
                    rates=rates.to_dict("records"),
                    lead_table=lead_tbl,
                    sku_lead_override=sku_ov,
                    manual_lead_override=(manual_input if manual_input > 0 else None),
                    phase=str(d.get("phase") or ""),
                    phase_defaults=phase_cfg,
                    rate_cards=rate_cards.to_dict("records") if not rate_cards.empty else [],
                    rate_charges=rate_charges.to_dict("records") if not rate_charges.empty else [],
                    service_scope=service_scope,
                    mode_override=(norm_mode(str(d.get("mode_override"))) if pd.notna(d.get("mode_override")) else None),
                    route_info=route_info,
                    miles=miles,
                )
                st.dataframe(pd.DataFrame(recs), width="stretch")
                if recs and recs[0].get("cost_items"):
                    with st.expander("Selected recommendation cost detail", expanded=False):
                        st.dataframe(pd.DataFrame(recs[0]["cost_items"]), width="stretch")


    with cube_tab:
        render_about("Cube Out", "Select multiple SKUs and calculate how many equipment units are required based on each SKU default pack rule. Enter required quantity in the SKU UOM (KG/METER/GALLON/EA/etc.).")
        skus = read_sku_catalog()
        packs = read_table("packaging_rules")
        eq = pd.read_sql_query("SELECT * FROM equipment_presets WHERE active = 1", get_conn())

        if skus.empty or packs.empty or eq.empty:
            st.info("Need SKUs, packaging rules, and equipment presets before running cube-out.")
        else:
            sku_options = skus.copy()
            sku_options["cube_label"] = (
                sku_options["part_number"]
                + " | SUP:" + sku_options["supplier_code"]
                + " | PLANT:" + sku_options["plant_code"].fillna("")
                + " | UOM:" + sku_options["uom"].fillna("EA")
            )
            selected_ids = st.multiselect(
                "Select SKUs",
                options=sku_options["sku_id"].tolist(),
                format_func=lambda sid: sku_options.loc[sku_options["sku_id"] == sid, "cube_label"].iloc[0],
                help="Choose one or more part numbers to cube out together.",
            )
            if not selected_ids:
                st.info("Select one or more SKUs to begin cube-out.")
            else:
                defaults = packs[packs["is_default"] == 1][["sku_id", "pack_name", "units_per_pack", "kg_per_unit", "pack_tare_kg", "dim_l_m", "dim_w_m", "dim_h_m"]]
                calc_rows = skus[skus["sku_id"].isin(selected_ids)][["sku_id", "part_number", "supplier_code", "plant_code", "uom"]].merge(defaults, on="sku_id", how="left")
                calc_rows["qty_required"] = 0.0
                calc_rows["uom"] = calc_rows["uom"].replace("", pd.NA).fillna("EA")

                edited = st.data_editor(
                    calc_rows[["sku_id", "part_number", "supplier_code", "plant_code", "uom", "pack_name", "units_per_pack", "kg_per_unit", "dim_l_m", "dim_w_m", "dim_h_m", "qty_required"]],
                    width="stretch",
                    num_rows="fixed",
                    disabled=["sku_id", "part_number", "supplier_code", "plant_code", "uom", "pack_name", "units_per_pack", "kg_per_unit", "dim_l_m", "dim_w_m", "dim_h_m"],
                    column_config={"qty_required": st.column_config.NumberColumn("qty_required", min_value=0.0, help="Required amount in this SKU UOM")},
                )

                results: list[dict] = []
                for _, row in edited.iterrows():
                    qty_required = float(row.get("qty_required") or 0)
                    units_per_pack = float(row.get("units_per_pack") or 0)
                    if qty_required <= 0 or units_per_pack <= 0:
                        continue

                    packs_needed = int(ceil(qty_required / units_per_pack))
                    gross_pack_weight = (units_per_pack * float(row.get("kg_per_unit") or 0)) + float(row.get("pack_tare_kg") or 0)
                    pack_cube = float(row.get("dim_l_m") or 0) * float(row.get("dim_w_m") or 0) * float(row.get("dim_h_m") or 0)
                    total_weight_kg = packs_needed * gross_pack_weight
                    total_volume_m3 = packs_needed * pack_cube

                    for _, eq_row in eq.iterrows():
                        eq_dict = eq_row.to_dict()
                        eq_obj = equipment_from_row(eq_dict)
                        fit = max_units_per_conveyance(
                            sku_id=int(row["sku_id"]),
                            pack_rule={
                                "units_per_pack": units_per_pack,
                                "kg_per_unit": float(row.get("kg_per_unit") or 0),
                                "pack_tare_kg": float(row.get("pack_tare_kg") or 0),
                                "dim_l_m": float(row.get("dim_l_m") or 0),
                                "dim_w_m": float(row.get("dim_w_m") or 0),
                                "dim_h_m": float(row.get("dim_h_m") or 0),
                                "stackable": int(eq_row.get("stackable", 1) if "stackable" in eq_row else 1),
                                "max_stack": None,
                            },
                            equipment=eq_dict,
                            context={"container_on_chassis": norm_mode(eq_obj.mode) in {"TRUCK", "DRAY"}},
                        )
                        packs_fit = int(fit["max_units"])
                        equipment_needed = equipment_count_for_packs(packs_needed, packs_fit)
                        if equipment_needed == 0:
                            continue
                        results.append({
                            "sku_id": int(row["sku_id"]),
                            "part_number": row["part_number"],
                            "uom": row["uom"],
                            "qty_required": qty_required,
                            "packs_needed": packs_needed,
                            "total_weight_kg": round(total_weight_kg, 3),
                            "total_volume_m3": round(total_volume_m3, 3),
                            "mode": eq_obj.mode,
                            "equipment": eq_obj.name,
                            "equipment_needed": equipment_needed,
                            "limiting_constraint": fit.get("limiting_constraint"),
                            "fit_diagnostics": "constraints_engine:max_units_per_conveyance@1.0.0",
                        })

                if results:
                    out = pd.DataFrame(results)
                    st.dataframe(out, width="stretch")
                    st.download_button("Download cube-out results", data=out.to_csv(index=False).encode(), file_name="cube_out_results.csv", mime="text/csv")
                else:
                    st.info("Enter qty_required > 0 for at least one SKU with a default pack rule.")


    with ship_tab:
        render_about("Shipment Builder", "Simulate greedy consolidation into equipment presets.\n\nPrerequisites: equipment presets exist.\n\nSteps: edit demo rows, run auto calculation, inspect shipment output.\n\nExample: two OCEAN rows combine when capacity allows.")
        st.write("Greedy consolidation preview (volume then weight)")
        with st.expander("Field guide (columns)", expanded=False):
            st.markdown("- mode: text (example OCEAN)\n- volume_m3: decimal >=0\n- weight_kg: decimal >=0\n- ship_by: date YYYY-MM-DD\n- cost: decimal >=0")
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
        eq = pd.read_sql_query("SELECT * FROM equipment_presets WHERE active = 1", get_conn())
        if not eq.empty:
            eq_map = {norm_mode(mode): equipment_from_row(group.iloc[0].to_dict()) for mode, group in eq.groupby("mode")}
            shipments = build_shipments(demo.to_dict("records"), eq_map)
            st.dataframe(pd.DataFrame(shipments), width="stretch")

    with exp_tab:
        render_about("Export", "Download CSV reports for shipment planning artifacts, customs reporting, and phase rollups.")
        st.subheader("Export reports")
        demand = read_table("demand_lines")
        booking = read_table("rates")
        excess = pd.DataFrame(columns=["part_number", "tranche", "excess_units"])

        def dl(df: pd.DataFrame, name: str):
            st.download_button(name, data=df.to_csv(index=False).encode(), file_name=name, mime="text/csv")

        dl(demand, "shipment_plan.csv")
        dl(booking, "booking_summary.csv")
        dl(excess, "excess_report.csv")

        if not demand.empty:
            sku = read_table("sku_master")
            customs_rates = read_table("customs_hts_rates")
            rep_input = demand.merge(sku[["sku_id", "part_number", "default_coo", "plant_code"]], on="sku_id", how="left")
            rep_input["unit_price"] = 1.0
            rep_input["supplier_code"] = rep_input.get("supplier_code", "")
            rep_input["port"] = ""
            rep_input["importer"] = ""
            rep_input["exporter"] = ""
            rep_input["incoterms"] = ""
            custom_rows = customs_report(rep_input.to_dict("records"), sku.to_dict("records"), customs_rates.to_dict("records") if not customs_rates.empty else [])
            customs_df = pd.DataFrame(custom_rows)
            dl(customs_df, "customs_report.csv")

            phase_rows = [
                {"phase": r.get("phase", ""), "mode": r.get("mode_override", ""), "estimated_cost": 0, "base_cost": 0, "domestic_legs_cost": 0, "weight_kg": float(r.get("qty", 0)), "volume_m3": 0, "arrival_date": r.get("need_date", "")}
                for _, r in rep_input.iterrows()
            ]
            phase_df = pd.DataFrame(phase_cost_rollup(phase_rows, custom_rows))
            dl(phase_df, "phase_summary.csv")


elif section == "Docs":
    st.header("In-app Docs")
    doc_page = st.sidebar.selectbox(
        "Docs page",
        ["Quick Start", "Data Model", "Rates Guide", "Customs Guide", "Import Templates", "FAQ/Troubleshooting"],
        help="Open built-in setup and operations documentation.",
    )
    if doc_page == "Quick Start":
        render_docs_page("quick_start.md")
    elif doc_page == "Data Model":
        render_docs_page("data_model.md")
    elif doc_page == "Rates Guide":
        render_docs_page("rates_guide.md")
    elif doc_page == "Customs Guide":
        render_docs_page("customs_guide.md")
    elif doc_page == "Import Templates":
        render_docs_page("import_templates.md")
        st.subheader("Download CSV templates")
        templates_dir = Path("templates")
        for _, fname in TEMPLATE_SPECS:
            template_path = templates_dir / fname
            csv_blob = template_path.read_bytes()
            st.download_button(f"Download {fname}", data=csv_blob, file_name=fname, mime="text/csv")
    else:
        render_docs_page("faq.md")


st.caption("All data stored locally in SQLite planner.db")
