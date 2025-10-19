import sys, json, os, math
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
from schema_config import EXPECTED_DTYPES, RANGES, REQUIRED_COLUMNS, UNIQUE_COMBO, NON_NULL

REPORT_PATH = "data/validation_report.json"

def load_csv(path):
    # Read as strings first to avoid silent coercion, then enforce types
    df = pd.read_csv(path, dtype="string")
    return df

def load_parquet(path):
    # Use pandas; parquet already carries types but we'll re-check
    return pd.read_parquet(path)

def coerce_dtypes(df):
    # Start with all columns present
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    out = df.copy()
    # city -> pandas StringDtype
    out["city"] = out["city"].astype("string")

    # numeric coercions with error reporting
    for col, target in EXPECTED_DTYPES.items():
        if col == "city": 
            continue
        if target.startswith("int"):
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
        elif target.startswith("float"):
            out[col] = pd.to_numeric(out[col], errors="coerce")
        # timestamps must be integers (unix seconds)
        if col in ("timestamp","fetched_at"):
            # keep integer representation but also store a dt conversion for sanity checks
            # ensure integer-like
            out[col] = out[col].astype("Int64")
    # finally, set exact dtypes
    out = out.astype({
        "city": "string",
        "temperature": "float64",
        "humidity_percent": "Int64",
        "timestamp": "Int64",
        "fetched_at": "Int64",
    })
    return out

def run_checks(df, source_name):
    problems = []

    # Non-null checks
    for col in NON_NULL:
        nulls = df[col].isna().sum()
        if nulls > 0:
            problems.append(f"[{source_name}] Column '{col}' has {nulls} nulls")

    # Range checks
    for col, (lo, hi) in RANGES.items():
        if col in df.columns:
            bad = df[(df[col].notna()) & ((df[col] < lo) | (df[col] > hi))]
            if len(bad):
                problems.append(f"[{source_name}] {col} has {len(bad)} values outside [{lo}, {hi}]")

    # Type checks
    expected = EXPECTED_DTYPES.copy()
    actual = {c: str(df[c].dtype) for c in expected if c in df.columns}
    # Normalize Int64 (nullable) comparable to int64
    def norm(t):
        return {"Int64":"int64","string":"string"}.get(t, t)
    mismatches = {c: (norm(actual.get(c,"<missing>")), norm(expected[c])) 
                  for c in expected if norm(actual.get(c,"<missing>")) != norm(expected[c])}
    if mismatches:
        for col,(got,exp) in mismatches.items():
            problems.append(f"[{source_name}] dtype mismatch for '{col}': got {got}, expected {exp}")

    # Uniqueness check
    if all(c in df.columns for c in UNIQUE_COMBO):
        dups = df.duplicated(UNIQUE_COMBO).sum()
        if dups:
            problems.append(f"[{source_name}] {dups} duplicate rows on {UNIQUE_COMBO}")

    # Timestamp sanity: fetched_at >= timestamp (generally)
    if all(c in df.columns for c in ("timestamp","fetched_at")):
        bad = df[(df["timestamp"].notna()) & (df["fetched_at"].notna()) & (df["fetched_at"] < df["timestamp"])]
        if len(bad):
            problems.append(f"[{source_name}] {len(bad)} rows where fetched_at < timestamp")

    return problems

def main():
    inputs = [
        ("CSV", "data/clean_data.csv", load_csv),
        ("PARQUET", "data/clean_data.parquet", load_parquet),
    ]
    all_problems = []
    summaries = {}

    for label, path, loader in inputs:
        if not os.path.exists(path):
            all_problems.append(f"[{label}] Missing file: {path}")
            continue
        try:
            raw = loader(path)
            df = coerce_dtypes(raw)
            probs = run_checks(df, label)
            if label == "CSV":
                # Re-save a normalized CSV to ensure types are clean if needed
                df.to_csv("data/clean_data.normalized.csv", index=False)
            if label == "PARQUET":
                # Re-save normalized parquet
                df.to_parquet("data/clean_data.normalized.parquet", index=False)
            summaries[label] = {
                "rows": len(df),
                "columns": df.columns.tolist(),
                "dtypes": {c: str(df[c].dtype) for c in df.columns}
            }
            all_problems.extend(probs)
        except Exception as e:
            all_problems.append(f"[{label}] Exception: {e}")

    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "problems": all_problems,
        "summaries": summaries
    }
    os.makedirs("data", exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2)

    if all_problems:
        print("VALIDATION FAILED. See", REPORT_PATH)
        for p in all_problems:
            print("-", p)
        sys.exit(1)
    else:
        print("VALIDATION PASSED. See", REPORT_PATH)
        sys.exit(0)

if __name__ == "__main__":
    main()
