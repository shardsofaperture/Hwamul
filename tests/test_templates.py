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
        "length_mm",
        "width_mm",
        "height_mm",
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
    }
    assert required.issubset(set(cols.keys()))
    assert "is_default" not in cols
