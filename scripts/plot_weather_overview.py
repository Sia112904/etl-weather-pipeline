import os
import pandas as pd
import matplotlib.pyplot as plt

src = "data/clean_data.csv"
df = pd.read_csv(src)
df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")

fig, ax1 = plt.subplots(figsize=(10,5))

color1 = "tab:red"
ax1.set_xlabel("Date / Time (UTC)")
ax1.set_ylabel("Temperature (°C)", color=color1)
ax1.plot(df["timestamp"], df["temperature"], color=color1, label="Temperature (°C)")
ax1.tick_params(axis="y", labelcolor=color1)

ax2 = ax1.twinx()
color2 = "tab:blue"
ax2.set_ylabel("Humidity (%)", color=color2)
ax2.plot(df["timestamp"], df["humidity_percent"], color=color2, label="Humidity (%)")
ax2.tick_params(axis="y", labelcolor=color2)

plt.title("Weather Overview (Last 7 Days, Dallas)")
fig.tight_layout()

os.makedirs("reports", exist_ok=True)
out = "reports/weather_overview_v2.png"
plt.savefig(out)
plt.close()
print("✅ Saved:", out)
