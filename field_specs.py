from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any

import pandas as pd
import streamlit as st


@dataclass(frozen=True)
class FieldSpec:
    field_type: str
    required: bool = False
    description: str = ""
    example: str = ""
    fmt: str = ""
    max_length: int | None = None
    regex: str | None = None
    allowed_chars: str = ""
    min_value: float | int | None = None
    max_value: float | int | None = None
    choices: list[str] | None = None
    notes: str = ""


TABLE_SPECS: dict[str, dict[str, FieldSpec]] = {
    "suppliers": {
        "supplier_code": FieldSpec("text", required=True, max_length=32, regex=r"^[A-Z0-9_-]{2,32}$", allowed_chars="A-Z, 0-9, _, -", description="Unique supplier code", example="MAEU"),
        "supplier_name": FieldSpec("text", required=True, max_length=120, description="Supplier display name", example="Maersk Line"),
        "incoterms_ref": FieldSpec("text", max_length=200, description="Supplier Incoterms reference", example="FOB SHANGHAI; EXW SHENZHEN"),
    },
    "skus": {
        "part_number": FieldSpec("text", required=True, max_length=64, regex=r"^[A-Z0-9_.-]{2,64}$", allowed_chars="A-Z, 0-9, ., _, -", description="Part number", example="PN_10001"),
        "plant_code": FieldSpec("text", required=True, max_length=40, regex=r"^[A-Z0-9_.-]{2,40}$", allowed_chars="A-Z, 0-9, ., _, -", description="Plant code owning this SKU", example="US_TX_DAL"),
        "supplier_id": FieldSpec("int", required=True, min_value=1, description="Supplier foreign key", example="1"),
        "supplier_duns": FieldSpec("text", max_length=13, regex=r"^[0-9-]{9,13}$", allowed_chars="0-9 and -", description="Supplier DUNS number", example="123456789"),
        "description": FieldSpec("text", max_length=200, description="SKU description", example="40DV container"),
        "source_location": FieldSpec("text", max_length=120, description="Supplier/source location code or name", example="CNSHA"),
        "incoterm": FieldSpec("text", max_length=3, regex=r"^[A-Z]{3}$", allowed_chars="A-Z", description="Default Incoterm", example="FOB"),
        "uom": FieldSpec("text", max_length=16, regex=r"^[A-Z]{1,16}$", allowed_chars="A-Z", description="Order unit of measure", example="KG"),
        "default_coo": FieldSpec("text", required=True, max_length=2, regex=r"^[A-Z]{2}$", allowed_chars="A-Z", description="Default country of origin ISO-2", example="CN"),
    },
    "pack_rules": {
        "pack_name": FieldSpec("text", required=True, max_length=64, regex=r"^[A-Z0-9_.-]{2,64}$", allowed_chars="A-Z, 0-9, ., _, -", description="Pack rule name", example="CARTON_STD"),
        "pack_type": FieldSpec("text", required=True, max_length=32, description="Pack type", example="STANDARD"),
        "units_per_pack": FieldSpec("decimal", required=True, min_value=1, description="Units in each pack", example="120", notes="Drives pack count and shipment split."),
        "kg_per_unit": FieldSpec("decimal", required=True, min_value=0, description="Weight per unit", example="0.95", notes="Used for shipment weight calculations."),
        "pack_tare_kg": FieldSpec("decimal", required=True, min_value=0, description="Pack tare weight", example="12.0", notes="Added to gross shipment weight."),
        "dim_l_cm": FieldSpec("decimal", required=True, min_value=0.01, description="Pack length input shown in centimeters; stored as meters", example="120", notes="Enter centimeters (e.g., 120) in admin. Save normalizes to meters in DB; legacy meter input (e.g., 1.20) is accepted and stored as-is."),
        "dim_w_cm": FieldSpec("decimal", required=True, min_value=0.01, description="Pack width input shown in centimeters; stored as meters", example="80", notes="Enter centimeters (e.g., 80) in admin. Save normalizes to meters in DB; legacy meter input (e.g., 0.80) is accepted and stored as-is."),
        "dim_h_cm": FieldSpec("decimal", required=True, min_value=0.01, description="Pack height input shown in centimeters; stored as meters", example="90", notes="Enter centimeters (e.g., 90) in admin. Save normalizes to meters in DB; legacy meter input (e.g., 0.90) is accepted and stored as-is."),
        "min_order_packs": FieldSpec("int", required=True, min_value=1, description="Minimum order in packs", example="1"),
        "increment_packs": FieldSpec("int", required=True, min_value=1, description="Order increment in packs", example="1"),
        "stackable": FieldSpec("bool", required=True, description="Whether packs can stack", example="1"),
        "max_stack": FieldSpec("int", min_value=1, description="Max stack count when stackable", example="3"),
        "is_default": FieldSpec("bool", required=True, description="Default rule for this SKU", example="1"),
    },
    "pack_rules_import": {
        "part_number": FieldSpec("text", required=True, max_length=64, regex=r"^[A-Z0-9_.-]{2,64}$", allowed_chars="A-Z, 0-9, ., _, -", description="Part number from SKU master", example="MFG-88421"),
        "supplier_code": FieldSpec("text", required=True, max_length=32, regex=r"^[A-Z0-9_-]{2,32}$", allowed_chars="A-Z, 0-9, _, -", description="Supplier code for SKU/vendor mapping", example="DEFAULT"),
        "pack_kg": FieldSpec("decimal", required=True, min_value=0.001, description="Gross weight for one standard pack in kilograms", example="24.5", notes="Import models a single canonical pack profile per SKU/vendor and stores this as kg_per_unit with units_per_pack=1."),
        "length_mm": FieldSpec("decimal", required=True, min_value=1, description="Pack length in millimeters", example="1200", notes="Converted to meters for storage (mm / 1000)."),
        "width_mm": FieldSpec("decimal", required=True, min_value=1, description="Pack width in millimeters", example="800", notes="Converted to meters for storage (mm / 1000)."),
        "height_mm": FieldSpec("decimal", required=True, min_value=1, description="Pack height in millimeters", example="900", notes="Converted to meters for storage (mm / 1000)."),
        "is_stackable": FieldSpec("bool", required=True, description="Whether packs can be stacked", example="1"),
        "max_stack": FieldSpec("int", min_value=1, description="Max stack count when is_stackable=1", example="3"),
        "ship_from_city": FieldSpec("text", required=True, max_length=120, description="Origin city for ship-from location", example="SHANGHAI"),
        "ship_from_port_code": FieldSpec("text", required=True, max_length=10, regex=r"^[A-Z0-9]{3,10}$", allowed_chars="A-Z, 0-9", description="Origin UN/LOCODE or internal port code", example="CNSHA"),
        "ship_from_duns": FieldSpec("text", required=True, max_length=13, regex=r"^[0-9-]{9,13}$", allowed_chars="0-9 and -", description="Ship-from DUNS", example="123456789"),
        "ship_from_location_code": FieldSpec("text", required=True, max_length=40, regex=r"^[A-Z0-9_.-]{2,40}$", allowed_chars="A-Z, 0-9, ., _, -", description="Internal origin location code", example="CN_SHA_PDC"),
        "ship_to_locations": FieldSpec("text", required=True, max_length=400, regex=r"^[A-Z0-9_.-]+(\|[A-Z0-9_.-]+)*$", allowed_chars="A-Z, 0-9, ., _, -, |", description="Pipe-delimited allowed destination locations", example="USLAX_DC01|USLGB_DC02"),
        "allowed_modes": FieldSpec("text", required=True, max_length=120, regex=r"^[A-Z]+(\|[A-Z]+)*$", allowed_chars="A-Z and |", description="Pipe-delimited transport modes", example="OCEAN|TRUCK", notes="Use canonical values such as TRUCK, OCEAN, AIR."),
        "incoterm": FieldSpec("text", required=True, max_length=3, regex=r"^[A-Z]{3}$", allowed_chars="A-Z", choices=["EXW", "FCA", "CPT", "CIP", "DAP", "DPU", "DDP", "FAS", "FOB", "CFR", "CIF"], description="Incoterm (ICC 2020)", example="FOB"),
        "incoterm_named_place": FieldSpec("text", required=True, max_length=120, description="Named place associated with the incoterm", example="SHANGHAI PORT"),
        "pack_name": FieldSpec("text", max_length=64, regex=r"^[A-Z0-9_.-]{2,64}$", allowed_chars="A-Z, 0-9, ., _, -", description="Optional display label for pack profile", example="STD_MFG-88421", notes="Display-only alias. Leave blank to auto-generate STD_<part_number>."),
        "pack_type": FieldSpec("text", max_length=32, description="Optional packaging type", example="PALLET"),
        "qty_per_pack": FieldSpec("decimal", min_value=0.001, description="Optional quantity per pack", example="120"),
        "pack_material": FieldSpec("text", max_length=64, description="Optional pack material", example="WOOD"),
        "gross_weight_kg": FieldSpec("decimal", min_value=0.001, description="Optional gross weight override in kilograms", example="24.5"),
        "net_weight_kg": FieldSpec("decimal", min_value=0.001, description="Optional net weight in kilograms", example="23.0"),
        "stacking_notes": FieldSpec("text", max_length=300, description="Optional stacking guidance", example="Do not exceed 3 high in humid storage"),
        "notes": FieldSpec("text", max_length=500, description="Optional freeform notes", example="Validated by packaging engineering on 2026-01-15"),
    },
    "lead_times": {
        "country_of_origin": FieldSpec("text", required=True, max_length=2, regex=r"^[A-Z]{2}$", allowed_chars="A-Z", description="COO ISO-2", example="CN"),
        "mode": FieldSpec("text", required=True, max_length=24, choices=["TRUCK", "OCEAN", "AIR"], description="Transport mode", example="OCEAN"),
        "lead_days": FieldSpec("int", required=True, min_value=0, description="Lead time days", example="35", notes="Affects recommendation ETA feasibility."),
    },
    "demand_grid": {
        "sku_id": FieldSpec("int", required=True, min_value=1, description="SKU identifier", example="10"),
        "need_date": FieldSpec("date", required=True, fmt="YYYY-MM-DD", description="Required delivery date", example="2026-04-01", notes="Used to back-calculate ship date."),
        "qty": FieldSpec("decimal", required=True, min_value=0, description="Requested quantity", example="1200", notes="Drives allocation and shipments."),
        "coo_override": FieldSpec("text", max_length=2, regex=r"^[A-Z]{2}$", allowed_chars="A-Z", description="Optional COO override", example="VN"),
        "priority": FieldSpec("text", max_length=20, description="Planning priority label", example="HIGH"),
        "phase": FieldSpec("text", max_length=32, choices=["Trial1", "Trial2", "Sample", "Speed-up", "Validation", "SOP"], description="Production phase", example="Trial1"),
        "mode_override": FieldSpec("text", max_length=24, description="Optional mode override", example="AIR"),
        "service_scope": FieldSpec("text", max_length=8, choices=["P2P", "P2D", "D2P", "D2D"], description="Service scope override", example="D2D"),
        "miles": FieldSpec("decimal", min_value=0, description="Optional trucking miles override", example="320"),
    },
    "demand_import": {
        "part_number": FieldSpec("text", required=True, max_length=64, regex=r"^[A-Z0-9_.-]{2,64}$", allowed_chars="A-Z, 0-9, ., _, -", description="Part number from SKU master", example="MFG-88421"),
        "supplier_code": FieldSpec("text", max_length=32, regex=r"^[A-Z0-9_-]{2,32}$", allowed_chars="A-Z, 0-9, _, -", description="Optional supplier code; required when part_number exists under multiple suppliers", example="DEFAULT"),
        "need_date": FieldSpec("date", required=True, fmt="YYYY-MM-DD", description="Required delivery date", example="2026-03-10"),
        "qty": FieldSpec("decimal", required=True, min_value=0, description="Requested quantity", example="250"),
        "coo_override": FieldSpec("text", max_length=2, regex=r"^[A-Z]{2}$", allowed_chars="A-Z", description="Optional COO override", example="VN"),
        "priority": FieldSpec("text", max_length=20, description="Planning priority label", example="HIGH"),
        "notes": FieldSpec("text", max_length=200, description="Optional planner notes", example="launch"),
        "phase": FieldSpec("text", max_length=32, choices=["Trial1", "Trial2", "Sample", "Speed-up", "Validation", "SOP"], description="Production phase", example="Trial1"),
        "mode_override": FieldSpec("text", max_length=24, description="Optional mode override", example="AIR"),
        "service_scope": FieldSpec("text", max_length=8, choices=["P2P", "P2D", "D2P", "D2D"], description="Service scope override", example="D2D"),
        "miles": FieldSpec("decimal", min_value=0, description="Optional trucking miles override", example="320"),
    },
    "equipment": {
        "equipment_code": FieldSpec("text", required=True, max_length=40, regex=r"^[A-Z0-9_]{3,40}$", allowed_chars="A-Z, 0-9, _", description="Canonical equipment code", example="CNT_40_DRY_HC"),
        "name": FieldSpec("text", required=True, max_length=80, description="Preset display name", example="40' Dry (High Cube)"),
        "mode": FieldSpec("text", required=True, max_length=24, choices=["TRUCK", "OCEAN", "AIR"], description="Transport mode", example="OCEAN"),
        "internal_length_m": FieldSpec("decimal", required=True, min_value=0.01, description="Internal length (m)", example="12.03"),
        "internal_width_m": FieldSpec("decimal", required=True, min_value=0.01, description="Internal width (m)", example="2.352"),
        "internal_height_m": FieldSpec("decimal", required=True, min_value=0.01, description="Internal height (m)", example="2.698"),
        "max_payload_kg": FieldSpec("decimal", required=True, min_value=0.01, description="Payload cap", example="26540"),
        "equipment_class": FieldSpec("text", max_length=16, choices=["CONTAINER", "TRAILER", "ULD"], description="Equipment class", example="CONTAINER"),
        "is_reefer": FieldSpec("bool", required=True, description="Reefer flag", example="0"),
        "is_high_cube": FieldSpec("bool", required=True, description="High-cube flag", example="1"),
        "active": FieldSpec("bool", required=True, description="Active for planning", example="1"),
    },
    "rate_cards": {
        "mode": FieldSpec("text", required=True, max_length=24, description="Mode", example="OCEAN"),
        "service_scope": FieldSpec("text", required=True, choices=["P2P", "P2D", "D2P", "D2D"], description="Scope", example="P2D"),
        "equipment": FieldSpec("text", required=True, max_length=40, description="Equipment code", example="CNT_40_DRY_STD"),
        "origin_type": FieldSpec("text", required=True, max_length=16, description="Origin level", example="PORT"),
        "origin_code": FieldSpec("text", required=True, max_length=16, regex=r"^[A-Z0-9_-]{2,16}$", allowed_chars="A-Z, 0-9, _, -", description="Origin code", example="USLAX"),
        "dest_type": FieldSpec("text", required=True, max_length=16, description="Destination level", example="PORT"),
        "dest_code": FieldSpec("text", required=True, max_length=16, regex=r"^[A-Z0-9_-]{2,16}$", allowed_chars="A-Z, 0-9, _, -", description="Destination code", example="CNSHA"),
        "currency": FieldSpec("text", required=True, max_length=3, regex=r"^[A-Z]{3}$", allowed_chars="A-Z", description="ISO currency", example="USD"),
        "base_rate": FieldSpec("decimal", required=True, min_value=0, description="Base charge", example="4000", notes="Starting cost before accessorials."),
        "effective_from": FieldSpec("date", required=True, fmt="YYYY-MM-DD", description="Start date", example="2026-01-01"),
        "effective_to": FieldSpec("date", fmt="YYYY-MM-DD", description="End date", example="2026-03-31"),
        "priority": FieldSpec("int", min_value=0, description="Tie-breaker priority", example="10"),
    },
    "customs_hts": {
        "sku_id": FieldSpec("int", min_value=1, description="Optional SKU identifier", example="10"),
        "material_input": FieldSpec("text", max_length=120, description="Material input description", example="Hot rolled steel coil"),
        "hts_code": FieldSpec("text", required=True, max_length=20, regex=r"^[0-9.]{4,20}$", allowed_chars="0-9, .", description="HTS code", example="7208.39.0015"),
        "country_of_origin": FieldSpec("text", max_length=2, regex=r"^[A-Z]{2}$", allowed_chars="A-Z", description="Country of origin (COO)", example="CN"),
        "ship_from_country": FieldSpec("text", max_length=2, regex=r"^[A-Z]{2}$", allowed_chars="A-Z", description="Ship-from country used for rate reference", example="MX"),
        "tariff_program": FieldSpec("text", max_length=40, description="Tariff program or note", example="MFN"),
        "base_duty_rate": FieldSpec("decimal", required=True, min_value=0, description="Base duty %", example="2.5"),
        "tariff_rate": FieldSpec("decimal", required=True, min_value=0, description="Additional tariff %", example="25"),
        "tariff_rate_notes": FieldSpec("text", max_length=500, description="Tariff rate notes by effective period", example="Raised to 25% per Q3 ruling"),
        "section_232": FieldSpec("bool", required=True, description="Section 232 applicable", example="1"),
        "section_301": FieldSpec("bool", required=True, description="Section 301 applicable", example="0"),
        "domestic_trucking_required": FieldSpec("bool", required=True, description="Domestic trucking required", example="1"),
        "port_to_ramp_required": FieldSpec("bool", required=True, description="Port-to-ramp service required", example="1"),
        "special_documentation_required": FieldSpec("bool", required=True, description="Special documentation required", example="1"),
        "documentation_notes": FieldSpec("text", max_length=500, description="Required documentation notes", example="Mill cert + country of melt and pour"),
        "documentation_url": FieldSpec("text", max_length=500, description="Link to uploaded documentation", example="https://intranet/customs/7208-doc-pack.pdf"),
        "tips": FieldSpec("text", max_length=500, description="Operational tips for this customs code", example="Pre-attach mill cert to broker packet"),
        "effective_from": FieldSpec("date", required=True, fmt="YYYY-MM-DD", description="Rate effective from", example="2026-01-01"),
        "effective_to": FieldSpec("date", fmt="YYYY-MM-DD", description="Rate effective to", example="2026-12-31"),
        "notes": FieldSpec("text", max_length=500, description="Freeform notes", example="Quarterly review"),
    },
    "rate_charges": {
        "rate_card_id": FieldSpec("int", required=True, min_value=1, description="Parent rate card", example="12"),
        "charge_code": FieldSpec("text", required=True, max_length=32, regex=r"^[A-Z0-9_-]{2,32}$", allowed_chars="A-Z, 0-9, _, -", description="Charge code", example="BAF"),
        "charge_name": FieldSpec("text", required=True, max_length=64, description="Charge label", example="Bunker Adjustment"),
        "calc_method": FieldSpec("text", required=True, max_length=24, description="Calculation basis", example="PER_SHIPMENT"),
        "amount": FieldSpec("decimal", required=True, min_value=0, description="Charge amount", example="350"),
        "effective_from": FieldSpec("date", fmt="YYYY-MM-DD", description="Charge start", example="2026-01-01"),
        "effective_to": FieldSpec("date", fmt="YYYY-MM-DD", description="Charge end", example="2026-03-31"),
    },
}


def build_help_text(table_key: str, field: str) -> str:
    spec = TABLE_SPECS.get(table_key, {}).get(field)
    if not spec:
        return ""
    chunks = [spec.description]
    if spec.fmt:
        chunks.append(f"Format: {spec.fmt}")
    if spec.allowed_chars:
        chunks.append(f"Allowed: {spec.allowed_chars}")
    if spec.max_length:
        chunks.append(f"Max length: {spec.max_length}")
    if spec.min_value is not None or spec.max_value is not None:
        chunks.append(f"Range: {spec.min_value if spec.min_value is not None else '-∞'} to {spec.max_value if spec.max_value is not None else '∞'}")
    if spec.example:
        chunks.append(f"Example: {spec.example}")
    return " | ".join([c for c in chunks if c])


def field_guide_df(table_key: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for col, spec in TABLE_SPECS.get(table_key, {}).items():
        rows.append(
            {
                "column": col,
                "type": spec.field_type,
                "format": spec.fmt or "-",
                "allowed": spec.allowed_chars or (f"<= {spec.max_length} chars" if spec.max_length else "-"),
                "range": f"{spec.min_value if spec.min_value is not None else '-∞'} .. {spec.max_value if spec.max_value is not None else '∞'}" if spec.field_type in {"int", "decimal"} else "-",
                "example": spec.example,
                "notes": spec.notes or "-",
            }
        )
    return pd.DataFrame(rows)


def validate_table_rows(table_key: str, df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    specs = TABLE_SPECS.get(table_key, {})
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        for col, spec in specs.items():
            if col not in df.columns:
                continue
            value = row.get(col)
            empty = pd.isna(value) or str(value).strip() == ""
            if spec.required and empty:
                errors.append(f"Row {i} ({col}): required. Example: {spec.example}")
                continue
            if empty:
                continue
            if spec.field_type == "date":
                try:
                    datetime.strptime(str(value), "%Y-%m-%d")
                except ValueError:
                    errors.append(f"Row {i} ({col}): must be YYYY-MM-DD. Example: {spec.example}")
            if spec.field_type in {"int", "decimal"}:
                num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
                if pd.isna(num):
                    errors.append(f"Row {i} ({col}): must be numeric. Example: {spec.example}")
                    continue
                if spec.min_value is not None and num < spec.min_value:
                    errors.append(f"Row {i} ({col}): must be >= {spec.min_value}. Example: {spec.example}")
                if spec.max_value is not None and num > spec.max_value:
                    errors.append(f"Row {i} ({col}): must be <= {spec.max_value}. Example: {spec.example}")
            txt = str(value)
            if spec.max_length and len(txt) > spec.max_length:
                errors.append(f"Row {i} ({col}): max length {spec.max_length}. Example: {spec.example}")
            if spec.regex and not re.fullmatch(spec.regex, txt):
                char_hint = f", {spec.allowed_chars} only" if spec.allowed_chars else ""
                errors.append(f"Row {i} ({col}): invalid format{char_hint}. Example: {spec.example}")
            if spec.choices and txt not in spec.choices:
                errors.append(f"Row {i} ({col}): must be one of {', '.join(spec.choices)}. Example: {spec.example}")
    return errors


def table_column_config(table_key: str) -> dict[str, st.column_config.Column]:
    config: dict[str, st.column_config.Column] = {}
    for col, spec in TABLE_SPECS.get(table_key, {}).items():
        help_text = build_help_text(table_key, col)
        if spec.field_type == "date":
            config[col] = st.column_config.DateColumn(col, help=help_text, format="YYYY-MM-DD")
        elif spec.field_type in {"int", "decimal"}:
            config[col] = st.column_config.NumberColumn(col, help=help_text)
        elif spec.field_type == "bool":
            config[col] = st.column_config.CheckboxColumn(col, help=help_text)
        elif spec.choices:
            config[col] = st.column_config.SelectboxColumn(col, options=spec.choices, help=help_text)
        else:
            config[col] = st.column_config.TextColumn(col, help=help_text)
    return config
