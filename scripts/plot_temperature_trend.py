import os, sys, glob
import pandas as pd
import matplotlib.pyplot as plt

# --- 1) Pick a source file (first that exists) ---
candidates = [
    "data/clean_data.parquet",
    "data/clean_data.csv",
    "data/clean_data.normalized.parquet",
    "data/clean_data.normalized.csv",
    "data/processed.parquet",
    "data/processed.csv",
    "data/cleaned_data.parquet",
    "data/cleaned_data.csv",
]
src = next((p for p in candidates if os.path.exists(p)), None)

# Fallback: pick newest .csv/.parquet in data/
if src is None:
    all_data = glob.glob("data/*.csv") + glob.glob("data/*.parquet")
    src = max(all_data, key=os.path.getmtime) if all_data else None

if src is None:
    print(" No data file found in ./data. Expected one of:\n - " + "\n - ".join(candidates))
    sys.exit(1)

print(f" Using source: {src}")

# --- 2) Read the data ---
if src.endswith(".parquet"):
    df = pd.read_parquet(src)
else:
    df = pd.read_csv(src)

if df.empty:
    print(" Dataframe is empty.")
    sys.exit(1)

# --- 3) Normalize columns (time + temperature) ---
time_col = None
for cand in ["datetime", "timestamp", "timestamp_unix"]:
    if cand in df.columns:
        time_col = cand
        break

if time_col is None:
    raise SystemExit(f" Could not find a time column. Looked for: datetime, timestamp, timestamp_unix.\nAvailable: {list(df.columns)}")

temp_col = None
for cand in ["temperature", "temperature_c", "temp"]:
    if cand in df.columns:
        temp_col = cand
        break

if temp_col is None:
    raise SystemExit(f" Could not find a temperature column. Looked for: temperature, temperature_c, temp.\nAvailable: {list(df.columns)}")

# --- 4) Build a proper datetime index ---
s = df[time_col]
if pd.api.types.is_integer_dtype(s) or pd.api.types.is_float_dtype(s):
    # assume unix seconds
    dt = pd.to_datetime(s, unit="s")
else:
    # try parsing strings
    dt = pd.to_datetime(s, errors="coerce")

if dt.isna().all():
    raise SystemExit(" Failed to parse any datetimes from the time column.")

df = df.assign(_dt=dt).dropna(subset=["_dt"]).sort_values("_dt")

# --- 5) Plot ---
plt.figure(figsize=(9, 4.8))
plt.plot(df["_dt"], df[temp_col], linewidth=2)
plt.title("Temperature Trend Over Time")
plt.xlabel("Time")
plt.ylabel("Temperature (°C)")
plt.tight_layout()

# --- 6) Save ---
os.makedirs("reports", exist_ok=True)
out_path = "reports/temperature_trend.png"
plt.savefig(out_path, dpi=150)
print(f" Saved plot to: {out_path}")
