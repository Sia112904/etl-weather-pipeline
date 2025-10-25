import os
import pandas as pd
import matplotlib.pyplot as plt

# --- Always prefer the freshly updated CSV ---
src = "data/clean_data.csv"
if not os.path.exists(src):
    src = "data/clean_data.parquet"

print(" Using source:", src)

df = pd.read_csv(src) if src.endswith(".csv") else pd.read_parquet(src)

# --- Parse timestamps correctly ---
if "timestamp" in df.columns:
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
elif "timestamp_unix" in df.columns:
    df["timestamp"] = pd.to_datetime(df["timestamp_unix"], unit="s", errors="coerce")
elif "timestamp_iso" in df.columns:
    df["timestamp"] = pd.to_datetime(df["timestamp_iso"], errors="coerce")

# --- Identify temperature column ---
temp_col = "temperature"
if "temperature_c" in df.columns:
    temp_col = "temperature_c"

# --- Plot ---
plt.figure(figsize=(10,5))
plt.plot(df["timestamp"], df[temp_col], color="tab:red", label="Temperature (°C)")
plt.xlabel("Date / Time (UTC)")
plt.ylabel("Temperature (°C)")
plt.title("Temperature Trend (Last 7 Days, Dallas)")
plt.legend()
plt.grid(True, alpha=0.4)
plt.tight_layout()

os.makedirs("reports", exist_ok=True)
out = "reports/temperature_trend.png"
plt.savefig(out)
plt.close()
print("✅ Saved plot to:", out)
