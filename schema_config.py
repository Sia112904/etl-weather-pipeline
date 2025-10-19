EXPECTED_DTYPES = {
    "city": "string",              # pandas StringDtype
    "temperature": "float64",      # Celsius
    "humidity_percent": "int64",   # 0..100
    "timestamp": "int64",          # Unix seconds
    "fetched_at": "int64"          # Unix seconds (ingestion time)
}

RANGES = {
    "temperature": (-60.0, 60.0),  # plausible Earth surface range
    "humidity_percent": (0, 100)
}

REQUIRED_COLUMNS = list(EXPECTED_DTYPES.keys())
UNIQUE_COMBO = ["city", "timestamp"]   # each city + observation time should be unique
NON_NULL = REQUIRED_COLUMNS            # all required
