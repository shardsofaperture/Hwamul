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
