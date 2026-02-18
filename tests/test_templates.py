import csv

from field_specs import TABLE_SPECS
from seed import TEMPLATE_SPECS, ensure_templates


def test_generated_template_headers_match_table_specs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    ensure_templates()

    for table_key, filename in TEMPLATE_SPECS:
        template_path = tmp_path / "templates" / filename
        assert template_path.exists(), f"missing template for {table_key}: {filename}"

        with template_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            header = next(reader)

        assert header == list(TABLE_SPECS[table_key].keys())




def test_pack_mdm_canonical_columns_present():
    cols = TABLE_SPECS["pack_rules_import"]
    required = {
        "part_number",
        "supplier_code",
        "pack_kg",
        "length_cm",
        "width_cm",
        "height_cm",
        "is_stackable",
        "max_stack",
        "ship_from_city",
        "ship_from_port_code",
        "ship_from_duns",
        "ship_from_location_code",
        "ship_to_locations",
        "allowed_modes",
        "incoterm",
        "incoterm_named_place",
        "plant_code",
        "uom",
        "default_coo",
    }
    assert required.issubset(set(cols.keys()))
    assert "is_default" not in cols


def test_pack_mdm_template_header_matches_v2_spec(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    ensure_templates()

    template_path = tmp_path / "templates" / "pack_mdm_template.csv"
    with template_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)

    expected = list(TABLE_SPECS["pack_rules_import"].keys())
    assert header == expected
    for col in [
        "ship_from_city",
        "ship_from_port_code",
        "ship_from_duns",
        "ship_from_location_code",
        "ship_to_locations",
        "allowed_modes",
        "incoterm",
        "incoterm_named_place",
        "plant_code",
        "uom",
        "default_coo",
    ]:
        assert col in header


def test_streamlined_template_catalog():
    names = {filename for _, filename in TEMPLATE_SPECS}
    assert "pack_mdm_template.csv" in names
    assert "raw_bom_template.csv" in names
    assert "carrier_template.csv" in names
    assert "rate_cards_template.csv" in names
    assert "rate_charges_template.csv" in names
    assert "lanes_template.csv" in names
    assert "suppliers_template.csv" not in names
    assert "skus_template.csv" not in names
    assert "pack_rules_template.csv" not in names


def test_raw_bom_template_columns_present():
    cols = TABLE_SPECS["raw_bom_import"]
    assert {"part_number", "raw_qty", "raw_weight_kg"}.issubset(set(cols.keys()))
