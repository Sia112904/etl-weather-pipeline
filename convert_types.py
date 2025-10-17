import pandas as pd
import json
import os

# Input and output paths
IN_PATH = "data/raw_data.json"
OUT_PATH = "data/clean_data.csv"

def main():
    # Check if input file exists
    if not os.path.exists(IN_PATH):
        print(f"File not found: {IN_PATH}")
        return

    # Load JSON safely
    with open(IN_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    # If JSON is a dict (not list), try to extract the records
    if isinstance(data, dict):
        # Look for common keys holding list of records
        if "list" in data:
            records = data["list"]
        elif "records" in data:
            records = data["records"]
        else:
            # Wrap the dict in a list so pandas can handle it
            records = [data]
    else:
        # Already a list
        records = data

    # Normalize nested JSON if needed
    df = pd.json_normalize(records)

    print("Preview of raw data:")
    print(df.head())

    # Convert timestamp column if it exists
    if 'dt' in df.columns:
        df['datetime'] = pd.to_datetime(df['dt'], unit='s')
        df.drop(columns=['dt'], inplace=True)

    # Convert temperature if it exists
    if 'temp' in df.columns:
        df['temp_celsius'] = df['temp'] - 273.15

    # Convert numeric columns safely
    for col in ['humidity', 'pressure']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Save cleaned CSV
    df.to_csv(OUT_PATH, index=False)
    print(f"Data cleaned and saved to {OUT_PATH}")

if __name__ == "__main__":
    main()

