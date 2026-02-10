# for type hints#
from __future__ import annotations
#makes the code output cleaner#
from dataclasses import dataclass
#allows to open zip from the memory without the needing to save it#
from io import BytesIO
#like in java, doesnt change#
from typing import Final

#to unpack the zip file#
import zipfile

import pandas as pd
import requests

DAILY_ZIP_URL: Final[str] = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref.zip"
HISTORY_ZIP_URL: Final[str] = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"

CURRENCIES: Final[list[str]] = ["USD", "SEK", "GBP", "JPY"]
OUTPUT_FILE: Final[str] = "exchange_rates.md"

# for the constructor fields #
# frozen makes the object immutable, output is read only #
@dataclass(frozen=True)
class ExchangeRow:
    currency_code: str
    rate: float
    mean_historical_rate: float

# returns as a zip in memory with bytes #
def download_zip(url: str, timeout: int = 30) -> bytes:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.content

def list_zip_files(zip_bytes: bytes) -> list[str]:
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        return zf.namelist()


def read_first_csv_from_zip(zip_bytes: bytes) -> bytes:
    """returns list of files inside the zip"""
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf: # BytesIO makes a filelike object with the input bytes .ZipFile helps open the object as zf
        names = zf.namelist() # returns all the filenames in the zip
        csv_names: list[str] = [n for n in names if n.lower().endswith(".csv")]

        if not csv_names:
            raise ValueError(f"no csv files {zf.namelist()}")

        csv_bytes = zf.read(csv_names[0])
        return csv_bytes

def csv_bytes_to_dataframe(csv_bytes: bytes) -> pd.DataFrame:
    """converts the csv_bytes to dataframe which is much easier for tables sorting, filtering, finding the median etc."""
    df = pd.read_csv(BytesIO(csv_bytes))
    df.columns = df.columns.str.strip() # after the commas are spaces
    return df

def extract_latest_rates(daily_df: pd.DataFrame, currencies: list[str]) -> dict[str, float]:
    """takes the latest daily rates and returns them as dict"""
    daily_last_row = daily_df.iloc[-1]

    rates: dict[str, float] = {}
    for cur in currencies:
        if cur not in daily_df.columns:
            raise ValueError(f"{cur} not in the ecb column: {list(daily_df.columns)}")
        rates[cur] = float(daily_last_row[cur])

    return rates

def extract_average_history_rates(hist_df: pd.DataFrame, currencies: list[str]) -> dict[str, float]:
    means: dict[str, float] = {}

    for cur in currencies:
        if cur not in hist_df.columns:
            raise ValueError(f"{cur} not in the ecb historical columns: {list(hist_df.columns)}")
        series = pd.to_numeric(hist_df[cur], errors="coerce") # if in the field there is no number it will be NaN (notanumber)
        means[cur] = float(series.dropna().mean()) # removes the NaN fields, calculates the average with mean()

    return means

def build_output_rows(latest_rates: dict[str, float], historical_means: dict[str, float], currencies: list[str]) -> list[ExchangeRow]:
    """builds the output rows with the correct object"""
    rows: list[ExchangeRow] = []

    for cur in currencies:
        rows.append(ExchangeRow(currency_code=cur, rate=latest_rates[cur], mean_historical_rate=historical_means[cur]))

    return rows

def write_markdown_table(rows: list[ExchangeRow], output_path: str) -> None:
    """writes to the md file from the dataframe table"""
    output_df = pd.DataFrame([{
        "Currency Code": r.currency_code,
        "Rate": r.rate,
        "Mean Historical Rate": r.mean_historical_rate
    } for r in rows
    ])
    output_df["Rate"] = output_df["Rate"].map(lambda x: f"{x:.6f}")
    output_df["Mean Historical Rate"] = output_df["Mean Historical Rate"].map(lambda x: f"{x:.6f}")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(output_df.to_markdown(index=False))
        f.write("\n")

def main() -> None:

    daily_zip = download_zip(DAILY_ZIP_URL)
    hist_zip = download_zip(HISTORY_ZIP_URL)

    daily_df = csv_bytes_to_dataframe(read_first_csv_from_zip(daily_zip))
    hist_df = csv_bytes_to_dataframe(read_first_csv_from_zip(hist_zip))

    latest = extract_latest_rates(daily_df, CURRENCIES)
    means = extract_average_history_rates(hist_df, CURRENCIES)

    rows = build_output_rows(latest, means, CURRENCIES)
    write_markdown_table(rows, OUTPUT_FILE)

    print(f"done wrote {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

