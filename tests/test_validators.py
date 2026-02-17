import pandas as pd

from validators import require_cols, validate_dates, validate_positive, validate_with_specs


def test_require_cols_flags_missing_or_blank():
    df = pd.DataFrame([{"part_number": "A", "default_coo": ""}])
    assert require_cols(df, ["part_number", "default_coo", "description"]) == ["default_coo", "description"]


def test_validate_positive_rules():
    df = pd.DataFrame([{"qty": 0, "weight": -1, "dims": 1}])
    assert "qty must be numeric and > 0" in validate_positive(df, ["qty"])
    assert "weight must be numeric and >= 0" in validate_positive(df, ["weight"], allow_zero=True)
    assert validate_positive(df, ["dims"]) == []


def test_validate_dates_detects_invalid():
    df = pd.DataFrame([{"need_date": "2026-01-01"}, {"need_date": "bad-date"}])
    assert validate_dates(df, ["need_date"]) == ["need_date has invalid dates"]


def test_validate_with_specs_date_regex_and_ranges():
    df = pd.DataFrame(
        [
            {"supplier_code": "ok", "supplier_name": "Valid"},
            {"supplier_code": "MAEU_TOO_LONG_CODE_12345678901234567890", "supplier_name": "Valid"},
        ]
    )
    errors = validate_with_specs("suppliers", df)
    assert any("Row 1 (supplier_code): invalid format" in e for e in errors)
    assert any("Row 2 (supplier_code): max length" in e for e in errors)



def test_validate_with_specs_numeric_and_date_messages():
    demand = pd.DataFrame([{"sku_id": 1, "need_date": "2026/01/01", "qty": -1}])
    errors = validate_with_specs("demand", demand)
    assert "Row 1 (need_date): must be YYYY-MM-DD. Example: 2026-04-01" in errors
    assert "Row 1 (qty): must be >= 0. Example: 1200" in errors
