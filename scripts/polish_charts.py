#!/usr/bin/env python3
"""
Polish charts: titles, labels, colors; save figures in /visuals/.
Reads from data/clean_data.csv or data/clean_data.parquet (whichever exists).
"""

import os
import sys
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, AutoDateLocator

DATA_CANDIDATES = [
    "data/clean_data.csv",
    "data/clean_data.parquet",
]

OUT_DIR = "visuals"

def _read_df():
    src = next((p for p in DATA_CANDIDATES if os.path.exists(p)), None)
    if src is None:
        sys.exit("No cleaned dataset found. Expected one of: " + ", ".join(DATA_CANDIDATES))
    if src.endswith(".parquet"):
        df = pd.read_parquet(src)
    else:
        df = pd.read_csv(src)

    # Try to normalize column names commonly used in the project
    cols = {c.lower(): c for c in df.columns}
    # Expected canonical names after transform stage
    # temperature, humidity_percent, timestamp (unix seconds), city
    if "timestamp" in cols:
        tcol = cols["timestamp"]
        df["time"] = pd.to_datetime(df[tcol], unit="s", errors="coerce")
    elif "timestamp_unix" in cols:
        tcol = cols["timestamp_unix"]
        df["time"] = pd.to_datetime(df[tcol], unit="s", errors="coerce")
    else:
        # If there's already a datetime-like column, attempt parse
        guess = next((c for c in df.columns if "time" in c.lower() or "date" in c.lower()), None)
        if guess:
            df["time"] = pd.to_datetime(df[guess], errors="coerce")
        else:
            sys.exit("Could not find a timestamp column to plot (looked for 'timestamp', 'timestamp_unix').")

    # Temperature
    temp_col = next((c for c in df.columns if c.lower() in {"temperature","temperature_c","temp"}), None)
    if not temp_col:
        sys.exit("Could not find a temperature column (expected one of: temperature, temperature_c, temp).")

    # Humidity (optional)
    humid_col = next((c for c in df.columns if c.lower() in {"humidity_percent","humidity","humid"}), None)

    # City (optional)
    city_col = next((c for c in df.columns if c.lower() == "city"), None)

    # Keep only needed cols
    keep = ["time", temp_col]
    if humid_col: keep.append(humid_col)
    if city_col: keep.append(city_col)
    df = df.dropna(subset=["time"])[keep].sort_values("time")

    return df, temp_col, humid_col, city_col

def _fmt_axes(ax, title, ylabel):
    ax.set_title(title, pad=12)
    ax.set_xlabel("Time")
    ax.set_ylabel(ylabel)
    ax.grid(True, linewidth=0.6, alpha=0.4)
    ax.legend(frameon=False)
    ax.xaxis.set_major_locator(AutoDateLocator())
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d\n%H:%M'))

def _save_fig(fig, basename):
    os.makedirs(OUT_DIR, exist_ok=True)
    png_path = os.path.join(OUT_DIR, f"{basename}.png")
    svg_path = os.path.join(OUT_DIR, f"{basename}.svg")
    fig.tight_layout()
    fig.savefig(png_path, dpi=200, bbox_inches="tight")
    fig.savefig(svg_path, bbox_inches="tight")
    print(f"Saved: {png_path}")
    print(f"Saved: {svg_path}")

def main():
    df, temp_col, humid_col, city_col = _read_df()

    # Optional subtitle bits
    city_str = f" • {df[city_col].iloc[0]}" if city_col and df[city_col].notna().any() else ""
    time_range = f"{df['time'].min().strftime('%Y-%m-%d')} → {df['time'].max().strftime('%Y-%m-%d')}"
    subtitle = f"{time_range}{city_str}"

    # 1) Temperature trend (polished)
    fig1, ax1 = plt.subplots(figsize=(10, 5.2))
    ax1.plot(df["time"], df[temp_col], label="Temperature (°C)", linewidth=2.0)  # consistent default color cycle
    _fmt_axes(ax1, f"Temperature Trend", "Temperature (°C)")
    ax1.text(0.01, 1.02, subtitle, transform=ax1.transAxes, fontsize=9, va="bottom", ha="left")
    _save_fig(fig1, "temperature_trend_polished")
    plt.close(fig1)

    # 2) Humidity trend (if present)
    if humid_col:
        fig2, ax2 = plt.subplots(figsize=(10, 5.2))
        ax2.plot(df["time"], df[humid_col], label="Humidity (%)", linewidth=2.0)
        _fmt_axes(ax2, f"Humidity Trend", "Relative Humidity (%)")
        ax2.set_ylim(0, 100)  # humidity typically 0–100%
        ax2.text(0.01, 1.02, subtitle, transform=ax2.transAxes, fontsize=9, va="bottom", ha="left")
        _save_fig(fig2, "humidity_trend_polished")
        plt.close(fig2)

    # 3) Combined temperature & humidity (2 y-axes), if humidity exists
    if humid_col:
        fig3, ax3 = plt.subplots(figsize=(10, 5.6))
        ln1 = ax3.plot(df["time"], df[temp_col], label="Temperature (°C)", linewidth=2.0)
        ax3.set_xlabel("Time")
        ax3.set_ylabel("Temperature (°C)")
        ax3.grid(True, linewidth=0.6, alpha=0.4)
        ax3.xaxis.set_major_locator(AutoDateLocator())
        ax3.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d\n%H:%M'))

        ax4 = ax3.twinx()
        ln2 = ax4.plot(df["time"], df[humid_col], label="Humidity (%)", linewidth=2.0, linestyle="--")
        ax4.set_ylabel("Relative Humidity (%)")
        ax4.set_ylim(0, 100)

        # single legend combining both lines
        lines = ln1 + ln2
        labels = [l.get_label() for l in lines]
        ax3.legend(lines, labels, frameon=False, loc="upper left")

        ax3.set_title("Weather Overview: Temperature & Humidity")
        ax3.text(0.01, 1.02, subtitle, transform=ax3.transAxes, fontsize=9, va="bottom", ha="left")

        _save_fig(fig3, "weather_overview_temp_humidity_polished")
        plt.close(fig3)

if __name__ == "__main__":
    main()
