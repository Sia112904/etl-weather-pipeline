#!/usr/bin/env python3

import os
import sys
import json
import time
import logging
import argparse
import requests
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Logging configuration (one line only)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

API_URL = "https://api.openweathermap.org/data/2.5/weather"

# Get API key from .env
def get_api_key():
    key = os.getenv("OPENWEATHER_API_KEY")
    if not key:
        logging.error("OPENWEATHER_API_KEY not found. Set it in your .env file.")
        sys.exit(1)
    return key

# Fetch weather data
def fetch_weather(city, units="metric"):
    params = {"q": city, "appid": get_api_key(), "units": units}
    resp = requests.get(API_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    main = data.get("main", {})
    return {
        "city": data.get("name"),
        "temp": main.get("temp"),
        "humidity": main.get("humidity"),
        "timestamp": data.get("dt"),
        "fetched_at": int(time.time())
    }

# Main function
def main():
    parser = argparse.ArgumentParser(description="Fetch current weather data")
    parser.add_argument("--city", required=True, help="City name (e.g. Dallas,US)")
    parser.add_argument("--units", default="metric", choices=["metric","imperial","standard"])
    parser.add_argument("--out", help="Optional output file path (JSON)")
    args = parser.parse_args()

    weather = fetch_weather(args.city, args.units)
    json_str = json.dumps(weather, indent=2)
    print(json_str)

    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w") as f:
            f.write(json_str + "\n")
        logging.info(f"Saved to %s", args.out)

if __name__ == "__main__":
    main()

