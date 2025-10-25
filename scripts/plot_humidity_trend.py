import os
import pandas as pd
import matplotlib.pyplot as plt

src = "data/clean_data.csv"
df = pd.read_csv(src)

df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")

plt.figure(figsize=(10,5))
plt.plot(df["timestamp"], df["humidity_percent"], color="tab:blue", label="Humidity (%)")
plt.xlabel("Date / Time (UTC)")
plt.ylabel("Humidity (%)")
plt.title("Humidity Trend (Last 7 Days, Dallas)")
plt.legend()
plt.grid(True, alpha=0.4)
plt.tight_layout()

os.makedirs("reports", exist_ok=True)
out = "reports/humidity_trend_v2.png"
plt.savefig(out)
plt.close()
print("✅ Saved:", out)
