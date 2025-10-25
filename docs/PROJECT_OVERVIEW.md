## 1) What this project does
End-to-end ETL for daily weather data:
- **Extract** → raw readings (temperature, humidity, timestamps)
- **Transform** → clean, normalize dtypes, convert timestamps, engineer fields
- **Load** → SQLite (`weather.db`), plus tidy CSV/Parquet in `data/`
- **Visualize** → quick charts (e.g., `reports/temperature_trend.png`) for QA/insights

## 2) Why it exists
- Practice core **data engineering** patterns with reproducible scripts
- Demonstrate **version control** hygiene
- Ship a **portfolio-ready** project for data roles

## 3) Repo structure (high-level)
```
etl-weather-pipeline/
├─ src/                    # ETL modules (extract, transform, load, db, models)
├─ scripts/                # utilities (plots, polish scripts, etc.)
├─ data/                   # raw / processed / cleaned datasets
├─ reports/                # generated charts & artifacts
├─ docs/                   # docs & diagrams
├─ weather.db              # SQLite database (local dev)
├─ venv/                   # python virtual environment
└─ README.md               # quickstart + links
```

## 4) Tools & Versions
- **Language**: Python 3.12 (macOS)
- **Libs**: `pandas`, `pyarrow`, `SQLAlchemy`, `matplotlib` (optional)
- **DB**: SQLite (`weather.db`)
- **Env**: `venv`
- **VCS**: Git + GitHub
- **Diagramming**: Mermaid (GitHub-native) now; draw.io/Canva optional later

### Reproduce environment
```bash
python -V
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 5) Quickstart flow
```bash
# initialize SQLite & seed example row
python -m scripts.init_db

# produce cleaned data (CSV/Parquet)
python src/transform.py

# generate a visualization artifact
python -m scripts.plot_temperature_trend
```

## 6) Architecture (Mermaid)
```mermaid
 flowchart LR
    A[Data Source — API or Raw CSV] -->|raw files| B[Extract — src: extract.py → data/raw]
    B -->|DataFrame| C[Transform — src: transform.py → data/clean_data.csv or parquet]
    C -->|rows| D[Load — src: load.py → weather.db]
    C -->|artifacts| E[Visualize — scripts: plot_temperature_trend.py → reports/temperature_trend.png]
    C -. optional .-> F[Validate / Log (future)]
```

## 7) Status & Next Steps
-  ETL skeleton, cleaning, DB init, first visualization
-  Add validation, scheduling (cron/GitHub Actions), richer charts

---
_Last updated: Oct 25, 2025_
