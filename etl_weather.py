#!/usr/bin/env python3
import requests
import json
import sys
import time
import os
from argparse import ArgumentParser

def fetch_weather(city, api_key, units='metric'):
    """
    Fetch current weather from OpenWeatherMap.
    Returns a simplified dict like:
    { "city": "...", "temp": 20.05, "humidity": 67, "timestamp": 163..., "fetched_at": 163... }
    """
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": api_key, "units": units}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        status = resp.status_code if resp is not None else None
        if status == 401:
            print("Error: Unauthorized – check your API key.")
        elif status == 404:
            print("Error: City not found.")
        else:
            print(f"HTTP error occurred: {http_err} (status {status})")
        return {}
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
        return {}
    try:
        data = resp.json()
    except ValueError:
        print("Error: response was not valid JSON.")
        return {}

    # Extract fields (OpenWeather returns 'main.temp', 'main.humidity', 'dt' etc.)
    city_name = data.get("name") or city
    main = data.get("main", {})
    temp = main.get("temp")
    humidity = main.get("humidity")
    timestamp = data.get("dt")  # server-provided epoch seconds
    fetched_at = int(time.time())

    return {
        "city": city_name,
        "temp": temp,
        "humidity": humidity,
        "timestamp": timestamp,
        "fetched_at": fetched_at
    }

def save_data(data, filepath="data/raw_data.json"):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    try:
        with open(filepath, "w") as f:
            json.dump(data, f, indent=4)
        print(f"Data saved to {filepath}")
    except Exception as e:
        print(f"Error saving data: {e}")

def main():
    parser = ArgumentParser(description="Fetch current weather (OpenWeatherMap) and save to data/raw_data.json")
    parser.add_argument("city", help="City name, e.g. 'Dallas' or 'Dallas,US'")
    parser.add_argument("api_key", nargs="?", default=None,
                        help="OpenWeatherMap API key (or set OPENWEATHER_API_KEY env var)")
    parser.add_argument("--units", choices=["metric","imperial","standard"], default="metric",
                        help="Units for temperature (default: metric)")
    parser.add_argument("--outfile", default="data/raw_data.json", help="Output file (default: data/raw_data.json)")
    args = parser.parse_args()

    api_key = args.api_key or os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        print("No API key provided. Pass <api_key> as the second positional arg or set OPENWEATHER_API_KEY.")
        sys.exit(1)

    result = fetch_weather(args.city, api_key, units=args.units)
    save_data(result, filepath=args.outfile)

if __name__ == "__main__":
    main()

