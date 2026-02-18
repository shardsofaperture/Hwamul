from __future__ import annotations

from dataclasses import dataclass
import sqlite3

import pandas as pd


@dataclass(frozen=True)
class PackMasterImportResult:
    suppliers_upserted: int
    ship_from_locations_upserted: int
    skus_upserted: int
    packaging_rules_upserted: int
    ship_to_rows_replaced: int
    allowed_modes_rows_replaced: int
    incoterms_upserted: int


def _split_pipe_list(raw_value: object) -> list[str]:
    if pd.isna(raw_value):
        return []
    tokens = [str(v).strip().upper() for v in str(raw_value).split("|")]
    return sorted({tok for tok in tokens if tok})


def _to_bool_int(raw_value: object) -> int:
    if pd.isna(raw_value):
        return 0
    if isinstance(raw_value, bool):
        return int(raw_value)
    text = str(raw_value).strip().lower()
    return 1 if text in {"1", "true", "yes", "y"} else 0


def _canonical_ship_from_key(row: pd.Series) -> str:
    return "|".join(
        [
            str(row.get("ship_from_city", "")).strip().upper(),
            str(row.get("ship_from_port_code", "")).strip().upper(),
            str(row.get("ship_from_duns", "")).strip().upper(),
            str(row.get("ship_from_location_code", "")).strip().upper(),
        ]
    )


def _normalize_import(import_df: pd.DataFrame) -> pd.DataFrame:
    data = import_df.copy()
    data["part_number"] = data["part_number"].astype(str).str.strip().str.upper()
    data["supplier_code"] = data["supplier_code"].astype(str).str.strip().str.upper()
    data["incoterm"] = data["incoterm"].astype(str).str.strip().str.upper()
    data["incoterm_named_place"] = data["incoterm_named_place"].astype(str).str.strip()
    data["ship_from_city"] = data["ship_from_city"].astype(str).str.strip().str.upper()
    data["ship_from_port_code"] = data["ship_from_port_code"].astype(str).str.strip().str.upper()
    data["ship_from_duns"] = data["ship_from_duns"].astype(str).str.strip()
    data["ship_from_location_code"] = data["ship_from_location_code"].astype(str).str.strip().str.upper()

    data["pack_name"] = data["pack_name"].astype(str) if "pack_name" in data.columns else ""
    data["pack_name"] = data.apply(
        lambda r: str(r["pack_name"]).strip() if str(r["pack_name"]).strip() and str(r["pack_name"]).strip().lower() != "nan" else f"STD_{r['part_number']}",
        axis=1,
    )

    data["pack_kg"] = pd.to_numeric(data["pack_kg"], errors="coerce")
    data["length_mm"] = pd.to_numeric(data["length_mm"], errors="coerce")
    data["width_mm"] = pd.to_numeric(data["width_mm"], errors="coerce")
    data["height_mm"] = pd.to_numeric(data["height_mm"], errors="coerce")
    data["is_stackable"] = data["is_stackable"].apply(_to_bool_int)
    if "max_stack" in data.columns:
        data["max_stack"] = pd.to_numeric(data["max_stack"], errors="coerce")
    else:
        data["max_stack"] = pd.NA

    data["ship_to_values"] = data["ship_to_locations"].apply(_split_pipe_list)
    data["mode_values"] = data["allowed_modes"].apply(_split_pipe_list)
    data["canonical_ship_from_key"] = data.apply(_canonical_ship_from_key, axis=1)
    return data


def _validate_normalized_rows(data: pd.DataFrame) -> None:
    if data[["part_number", "supplier_code", "pack_name"]].duplicated().any():
        dupes = data.loc[data[["part_number", "supplier_code", "pack_name"]].duplicated(), ["part_number", "supplier_code", "pack_name"]]
        raise ValueError(
            "Duplicate part_number/supplier_code/pack_name rows found in upload: "
            + ", ".join(f"{r.part_number}/{r.supplier_code}/{r.pack_name}" for r in dupes.itertuples(index=False))
        )

    bad_pack = data[data["pack_kg"].isna() | (data["pack_kg"] <= 0)]
    if not bad_pack.empty:
        row_nums = ", ".join(map(str, bad_pack.index.tolist()))
        raise ValueError(f"pack_kg must be a positive number for all rows (bad row indexes: {row_nums})")

    bad_dims = data[
        data["length_mm"].isna() | data["width_mm"].isna() | data["height_mm"].isna()
        | (data["length_mm"] <= 0) | (data["width_mm"] <= 0) | (data["height_mm"] <= 0)
    ]
    if not bad_dims.empty:
        row_nums = ", ".join(map(str, bad_dims.index.tolist()))
        raise ValueError(f"length_mm/width_mm/height_mm must be positive numbers for all rows (bad row indexes: {row_nums})")

    empty_sets = data[(data["ship_to_values"].str.len() == 0) | (data["mode_values"].str.len() == 0)]
    if not empty_sets.empty:
        row_nums = ", ".join(map(str, empty_sets.index.tolist()))
        raise ValueError(f"ship_to_locations and allowed_modes must each contain at least one token (bad row indexes: {row_nums})")


def apply_pack_master_import(conn: sqlite3.Connection, import_df: pd.DataFrame) -> PackMasterImportResult:
    data = _normalize_import(import_df)
    _validate_normalized_rows(data)

    suppliers_upserted = 0
    ship_from_upserted = 0
    skus_upserted = 0
    pack_rules_upserted = 0
    ship_to_replaced = 0
    modes_replaced = 0

    with conn:
        for supplier_code in sorted(data["supplier_code"].dropna().unique().tolist()):
            conn.execute(
                """
                INSERT INTO suppliers (supplier_code, supplier_name)
                VALUES (?, ?)
                ON CONFLICT(supplier_code) DO UPDATE SET
                    supplier_name = excluded.supplier_name
                """,
                (supplier_code, supplier_code),
            )
            suppliers_upserted += 1

        supplier_rows = conn.execute("SELECT supplier_id, supplier_code FROM suppliers").fetchall()
        supplier_map = {row["supplier_code"]: row["supplier_id"] for row in supplier_rows}

        for row in data.itertuples(index=False):
            conn.execute(
                """
                INSERT INTO ship_from_locations(canonical_location_key, city, port_code, supplier_duns, internal_location_code)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(canonical_location_key) DO UPDATE SET
                    city = excluded.city,
                    port_code = excluded.port_code,
                    supplier_duns = excluded.supplier_duns,
                    internal_location_code = excluded.internal_location_code
                """,
                (
                    row.canonical_ship_from_key,
                    row.ship_from_city,
                    row.ship_from_port_code,
                    row.ship_from_duns,
                    row.ship_from_location_code,
                ),
            )
            ship_from_upserted += 1

            ship_from_location_id = conn.execute(
                "SELECT ship_from_location_id FROM ship_from_locations WHERE canonical_location_key = ?",
                (row.canonical_ship_from_key,),
            ).fetchone()["ship_from_location_id"]

            supplier_id = supplier_map[row.supplier_code]
            conn.execute(
                """
                INSERT INTO sku_master(part_number, supplier_id, plant_code, supplier_duns, description, source_location, incoterm, incoterm_named_place, uom, default_coo, ship_from_location_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(part_number, supplier_id) DO UPDATE SET
                    plant_code = excluded.plant_code,
                    supplier_duns = excluded.supplier_duns,
                    description = excluded.description,
                    source_location = excluded.source_location,
                    incoterm = excluded.incoterm,
                    incoterm_named_place = excluded.incoterm_named_place,
                    uom = excluded.uom,
                    default_coo = excluded.default_coo,
                    ship_from_location_id = excluded.ship_from_location_id
                """,
                (
                    row.part_number,
                    supplier_id,
                    str(getattr(row, "plant_code", "UNSPECIFIED") or "UNSPECIFIED").strip().upper(),
                    row.ship_from_duns,
                    str(getattr(row, "description", "") or "").strip(),
                    row.ship_from_location_code,
                    row.incoterm,
                    row.incoterm_named_place,
                    str(getattr(row, "uom", "EA") or "EA").strip().upper(),
                    str(getattr(row, "default_coo", "UN") or "UN").strip().upper(),
                    ship_from_location_id,
                ),
            )
            skus_upserted += 1

            sku_row = conn.execute(
                "SELECT sku_id FROM sku_master WHERE part_number = ? AND supplier_id = ?",
                (row.part_number, supplier_id),
            ).fetchone()
            sku_id = int(sku_row["sku_id"])

            conn.execute("UPDATE packaging_rules SET is_default = 0 WHERE sku_id = ?", (sku_id,))
            conn.execute(
                """
                INSERT INTO packaging_rules(
                    sku_id, pack_name, pack_type, is_default, units_per_pack, kg_per_unit, pack_tare_kg,
                    dim_l_m, dim_w_m, dim_h_m, min_order_packs, increment_packs, stackable, max_stack
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sku_id, pack_name) DO UPDATE SET
                    pack_type = excluded.pack_type,
                    is_default = excluded.is_default,
                    units_per_pack = excluded.units_per_pack,
                    kg_per_unit = excluded.kg_per_unit,
                    pack_tare_kg = excluded.pack_tare_kg,
                    dim_l_m = excluded.dim_l_m,
                    dim_w_m = excluded.dim_w_m,
                    dim_h_m = excluded.dim_h_m,
                    min_order_packs = excluded.min_order_packs,
                    increment_packs = excluded.increment_packs,
                    stackable = excluded.stackable,
                    max_stack = excluded.max_stack
                """,
                (
                    sku_id,
                    row.pack_name,
                    "STANDARD",
                    1,
                    1.0,
                    float(row.pack_kg),
                    0.0,
                    float(row.length_mm) / 1000.0,
                    float(row.width_mm) / 1000.0,
                    float(row.height_mm) / 1000.0,
                    1,
                    1,
                    int(row.is_stackable),
                    int(row.max_stack) if pd.notna(row.max_stack) else None,
                ),
            )
            pack_rules_upserted += 1

            conn.execute("DELETE FROM sku_ship_to_locations WHERE sku_id = ?", (sku_id,))
            for dest in row.ship_to_values:
                conn.execute(
                    "INSERT INTO sku_ship_to_locations(sku_id, destination_code) VALUES (?, ?)",
                    (sku_id, dest),
                )
                ship_to_replaced += 1

            conn.execute("DELETE FROM sku_allowed_modes WHERE sku_id = ?", (sku_id,))
            for mode in row.mode_values:
                conn.execute(
                    "INSERT INTO sku_allowed_modes(sku_id, mode_code) VALUES (?, ?)",
                    (sku_id, mode),
                )
                modes_replaced += 1

            conn.execute(
                "UPDATE sku_master SET incoterm = ?, incoterm_named_place = ? WHERE sku_id = ?",
                (row.incoterm, row.incoterm_named_place, sku_id),
            )

    return PackMasterImportResult(
        suppliers_upserted=suppliers_upserted,
        ship_from_locations_upserted=ship_from_upserted,
        skus_upserted=skus_upserted,
        packaging_rules_upserted=pack_rules_upserted,
        ship_to_rows_replaced=ship_to_replaced,
        allowed_modes_rows_replaced=modes_replaced,
        incoterms_upserted=skus_upserted,
    )
