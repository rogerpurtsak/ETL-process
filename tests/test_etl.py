from __future__ import annotations

import zipfile
from io import BytesIO

import pandas as pd

from etl_ecb_exchange_rates import (
    CURRENCIES,
    build_output_rows,
    csv_bytes_to_dataframe,
    extract_average_history_rates,
    extract_latest_rates,
    read_first_csv_from_zip,
)


def make_zip_with_csv(filename: str, content: str) -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, content.encode("utf-8"))
    return buf.getvalue()


def test_read_first_csv_from_zip_reads_csv() -> None:
    zip_bytes = make_zip_with_csv("eurofxref.csv", "Date, USD\n2020-01-01, 1.1\n")
    csv_bytes = read_first_csv_from_zip(zip_bytes)
    assert b"Date" in csv_bytes
    assert b"2020-01-01" in csv_bytes


def test_csv_bytes_to_dataframe_strips_columns() -> None:
    csv = "Date, USD, JPY\n2020-01-01, 1.1, 120\n"
    df = csv_bytes_to_dataframe(csv.encode("utf-8"))
    assert "USD" in df.columns
    assert "JPY" in df.columns
    assert " USD" not in df.columns


def test_extract_latest_rates_from_one_row_daily() -> None:
    daily_df = pd.DataFrame([{"Date": "2020-01-01", "USD": 1.2, "SEK": 10.0, "GBP": 0.8, "JPY": 130.0}])
    rates = extract_latest_rates(daily_df, ["USD", "SEK", "GBP", "JPY"])
    assert rates["USD"] == 1.2
    assert rates["JPY"] == 130.0


def test_extract_average_history_rates_ignores_missing_values() -> None:
    hist_df = pd.DataFrame(
        {
            "Date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            "USD": [1.0, "", 3.0],  # NaN gets ignored
            "SEK": [10.0, 11.0, 12.0],
            "GBP": [0.8, 0.9, 1.0],
            "JPY": [120.0, 121.0, 122.0],
        }
    )

    means = extract_average_history_rates(hist_df, ["USD", "SEK", "GBP", "JPY"])
    # USD mean should be (1.0 + 3.0)/2 = 2.0
    assert abs(means["USD"] - 2.0) < 1e-9
    assert abs(means["SEK"] - 11.0) < 1e-9


def test_build_output_rows_combines_dicts_in_currency_order() -> None:
    latest = {"USD": 1.1, "SEK": 10.0, "GBP": 0.8, "JPY": 120.0}
    means = {"USD": 1.2, "SEK": 9.5, "GBP": 0.85, "JPY": 110.0}

    rows = build_output_rows(latest, means, CURRENCIES)

    assert [r.currency_code for r in rows] == CURRENCIES
    assert rows[0].rate == 1.1
    assert rows[0].mean_historical_rate == 1.2
