# load_raw_to_df.py
import argparse
import json
from pathlib import Path
import pandas as pd
from pandas import json_normalize

def load_csv(path, **kwargs):
    return pd.read_csv(path, **kwargs)

def load_json(path):
    text = path.read_text(encoding="utf-8")
    # try NDJSON / JSON lines: one JSON object per line
    if "\n" in text and text.strip().startswith("{") and "\n{" in text:
        # treat as JSON lines
        return pd.read_json(path, lines=True)
    # else try loading as JSON array or object
    data = json.loads(text)
    if isinstance(data, list):
        return pd.json_normalize(data, sep="_")
    # if top-level dict with a records-like key, try common keys:
    for k in ("records", "data", "rows", "items"):
        if isinstance(data, dict) and k in data and isinstance(data[k], list):
            return pd.json_normalize(data[k], sep="_")
    # otherwise try normalizing the dict (will create one-row DF)
    return pd.json_normalize(data, sep="_")

def main():
    p = argparse.ArgumentParser(description="Load raw CSV/JSON into pandas and save processed.parquet")
    p.add_argument("input_path", help="path to raw JSON or CSV file")
    p.add_argument("--parse-dates", nargs="*", default=[], help="columns to parse as dates")
    p.add_argument("--sample", type=int, default=5, help="show this many rows (head)")
    args = p.parse_args()

    path = Path(args.input_path)
    if not path.exists():
        raise SystemExit(f"ERROR: file not found: {path}")

    ext = path.suffix.lower()
    print(f"Loading {path} (ext={ext}) ...")

    if ext in (".csv", ".tsv"):
        df = load_csv(path, parse_dates=args.parse_dates or None)
    elif ext in (".json", ".ndjson", ".jsonl"):
        df = load_json(path)
        # if the caller asked to parse dates, try to convert
        if args.parse_dates:
            for col in args.parse_dates:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
    else:
        # fallback: try CSV then JSON
        try:
            df = load_csv(path)
        except Exception:
            df = load_json(path)

    # quick checks
    print("\n=== quick checks ===")
    print("shape:", df.shape)
    print("\ncolumn dtypes:")
    print(df.dtypes)
    print(f"\nhead({args.sample}):")
    print(df.head(args.sample).to_string(index=False))
    print("\nnull counts (top 10):")
    nulls = df.isna().sum().sort_values(ascending=False)
    print(nulls.head(10).to_string())

    # save processed file
    out = path.parent / "processed.parquet"
    df.to_parquet(out, index=False)
    print(f"\nSaved processed parquet to: {out}")

if __name__ == "__main__":
    main()

