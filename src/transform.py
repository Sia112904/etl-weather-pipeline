#!/usr/bin/env python3
"""
transform.py — Reusable cleaning utilities for weather pipeline.

Usage:
  python src/transform.py --in data/raw_data.json --out data/clean_data.csv --parquet data/clean_data.parquet
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Iterable, List

import pandas as pd


# -----------------------------
# IO helpers
# -----------------------------
def read_json_records(path: str | Path) -> List[dict[str, Any]]:
    """
    Reads a JSON file that may contain:
      - a single JSON object
      - a list of JSON objects
      - newline-delimited JSON (one object per line)

    Returns a list of dicts.
    """
    p = Path(path)
    text = p.read_text(encoding="utf-8").strip()

    # Try NDJSON (newline-delimited)
    if "\n" in text and text.lstrip().startswith("{"):
        try:
            rows = [json.loads(line) for line in text.splitlines() if line.strip()]
            if rows and isinstance(rows[0], dict):
                return rows
        except json.JSONDecodeError:
            pass  # fall through

    # Try standard JSON
    data = json.loads(text)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    raise ValueError(f"Unsupported JSON structure in {path}")


def to_dataframe(records: Iterable[dict[str, Any]]) -> pd.DataFrame:
    """Convert list of records (dicts) into a pandas DataFrame."""
    return pd.DataFrame(list(records))


# -----------------------------
# Cleaning logic
# -----------------------------
def clean_weather_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize weather data columns and types.

    Expected columns (flexible):
      city, temp or temperature, humidity or humidity_percent,
      timestamp (unix s), fetched_at (unix s)

    Output columns (order):
      city, temperature, humidity_percent, timestamp, timestamp_iso,
      fetched_at, fetched_at_iso
    """
    # Normalize column names
    rename_map = {
        "temp": "temperature",
        "humidity": "humidity_percent",
    }
    df = df.rename(columns=rename_map)

    # Ensure required columns exist (fill if missing)
    for col in ["city", "temperature", "humidity_percent", "timestamp", "fetched_at"]:
        if col not in df.columns:
            df[col] = pd.NA

    # Enforce numeric types
    if "temperature" in df:
        df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce").round(2)

    if "humidity_percent" in df:
        df["humidity_percent"] = pd.to_numeric(df["humidity_percent"], errors="coerce")
        df["humidity_percent"] = df["humidity_percent"].clip(lower=0, upper=100)

    # Convert timestamps — keep them timezone-aware to avoid astype errors
    for ts_col, iso_col in [("timestamp", "timestamp_iso"), ("fetched_at", "fetched_at_iso")]:
        df[ts_col] = pd.to_numeric(df[ts_col], errors="coerce")
        df[iso_col] = pd.to_datetime(df[ts_col], unit="s", utc=True, errors="coerce")

    # Drop exact duplicate measurements (city+timestamp)
    if "city" in df and "timestamp" in df:
        df = df.drop_duplicates(subset=["city", "timestamp"])

    # Reorder/select columns
    cols = [
        "city",
        "temperature",
        "humidity_percent",
        "timestamp",
        "timestamp_iso",
        "fetched_at",
        "fetched_at_iso",
    ]
    existing = [c for c in cols if c in df.columns]
    df = df[existing]

    # Sort deterministically
    sort_cols = [c for c in ["city", "timestamp"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(by=sort_cols).reset_index(drop=True)

    return df


# -----------------------------
# CLI setup
# -----------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Clean raw weather JSON into tidy tabular data.")
    ap.add_argument("--in", dest="in_path", required=True, help="Path to raw JSON (object, array, or NDJSON).")
    ap.add_argument("--out", dest="out_csv", required=True, help="Path to write cleaned CSV.")
    ap.add_argument("--parquet", dest="out_parquet", default=None, help="(Optional) Path to write Parquet.")
    ap.add_argument("--log", dest="log_level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR).")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    in_path = Path(args.in_path)
    out_csv = Path(args.out_csv)
    out_parquet = Path(args.out_parquet) if args.out_parquet else None

    logging.info("Reading raw JSON from %s", in_path)
    records = read_json_records(in_path)

    logging.info("Loaded %d record(s)", len(records))
    df_raw = to_dataframe(records)

    logging.info("Cleaning dataframe")
    df_clean = clean_weather_df(df_raw)

    # Ensure output directories exist
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    logging.info("Writing CSV to %s", out_csv)
    df_clean.to_csv(out_csv, index=False)

    if out_parquet:
        logging.info("Writing Parquet to %s", out_parquet)
        out_parquet.parent.mkdir(parents=True, exist_ok=True)
        df_clean.to_parquet(out_parquet, index=False)

    logging.info("Done. Rows: %d, Columns: %d", df_clean.shape[0], df_clean.shape[1])


if __name__ == "__main__":
    main()

