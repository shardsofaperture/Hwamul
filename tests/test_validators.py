import pandas as pd

from validators import require_cols, validate_dates, validate_positive


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
