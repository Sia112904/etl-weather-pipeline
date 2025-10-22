#!/usr/bin/env python3
"""
Load cleaned weather data into the database.

Examples:
  python -m src.load data/clean_data.csv
  python -m src.load data/clean_data.parquet
  python -m src.load data/processed.parquet --no-skip-existing
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd

from src.db import SessionLocal
from src.models import WeatherReading


REQUIRED_COLS_SOURCE = {
    # source -> db field name
    "city": "city",
    "temperature": "temperature_c",       # in cleaned files
    "humidity_percent": "humidity_percent",
    "timestamp": "timestamp_unix",
}
REQUIRED_COLS_DB = ["city", "temperature_c", "humidity_percent", "timestamp_unix"]


def read_any(path: Path) -> pd.DataFrame:
    if not path.exists():
        sys.exit(f"[load.py] ERROR: File not found: {path}")
    if path.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    if path.suffix.lower() in {".csv"}:
        return pd.read_csv(path)
    sys.exit(f"[load.py] ERROR: Unsupported file type: {path.suffix} (use .csv or .parquet)")


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Map source column names -> DB field names
    rename_map = {src: dst for src, dst in REQUIRED_COLS_SOURCE.items() if src in df.columns}
    df = df.rename(columns=rename_map)

    # If columns already match DB names, keep them
    for k in REQUIRED_COLS_DB:
        if k not in df.columns:
            # Helpful hint when a column is missing
            sys.exit(
                "[load.py] ERROR: Missing column after normalization: "
                f"'{k}'. Available columns: {list(df.columns)}"
            )

    # Keep only what the DB table needs
    df = df[REQUIRED_COLS_DB].copy()

    # Dtypes/coercions
    df["city"] = df["city"].astype("string").str.strip()
    df["temperature_c"] = pd.to_numeric(df["temperature_c"], errors="coerce")
    df["humidity_percent"] = pd.to_numeric(df["humidity_percent"], errors="coerce").astype("Int64")
    df["timestamp_unix"] = pd.to_numeric(df["timestamp_unix"], errors="coerce").astype("Int64")

    # Drop rows with required nulls
    df = df.dropna(subset=["city", "temperature_c", "humidity_percent", "timestamp_unix"])

    # Cast back to Python ints (after dropna) so SQLAlchemy sees ints, not pandas NA
    df["humidity_percent"] = df["humidity_percent"].astype(int)
    df["timestamp_unix"] = df["timestamp_unix"].astype(int)

    # Deduplicate within the file on business key (city, timestamp_unix)
    df = df.drop_duplicates(subset=["city", "timestamp_unix"]).reset_index(drop=True)

    return df


def fetch_existing_keys(session) -> set[Tuple[str, int]]:
    rows: Iterable[Tuple[str, int]] = (
        session.query(WeatherReading.city, WeatherReading.timestamp_unix).all()
    )
    return set(rows)


def dataframe_to_models(df: pd.DataFrame) -> list[WeatherReading]:
    recs = []
    for r in df.itertuples(index=False):
        recs.append(
            WeatherReading(
                city=str(r.city),
                temperature_c=float(r.temperature_c),
                humidity_percent=int(r.humidity_percent),
                timestamp_unix=int(r.timestamp_unix),
            )
        )
    return recs


def main():
    ap = argparse.ArgumentParser(description="Load cleaned weather data into the DB.")
    ap.add_argument("path", help="Path to cleaned data (.csv or .parquet)")
    ap.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Insert even if (city, timestamp_unix) already exists (not recommended).",
    )
    args = ap.parse_args()

    path = Path(args.path)
    df_raw = read_any(path)
    df = normalize(df_raw)

    with SessionLocal() as session:
        if args.no_skip_existing:
            to_insert = df
            skipped = 0
        else:
            existing = fetch_existing_keys(session)
            mask_new = ~df.apply(lambda r: (r["city"], int(r["timestamp_unix"])) in existing, axis=1)
            to_insert = df[mask_new].copy()
            skipped = len(df) - len(to_insert)

        objs = dataframe_to_models(to_insert)

        inserted = 0
        if objs:
            session.bulk_save_objects(objs)
            session.commit()
            inserted = len(objs)

    total = len(df)
    print(f"[load.py] File: {path}")
    print(f"[load.py] Total valid rows: {total}")
    print(f"[load.py] Skipped (already in DB): {skipped}")
    print(f"[load.py] Inserted: {inserted}")


if __name__ == "__main__":
    main()
