## OpenWeatherMap Datalakehouse pipeline
ETL pipeline for weather data:
- Extracts from OpenWeather API
- Processes through medallion architecture
- Stores in MinIO as Delta Lake

# Data Flow
```bash
Bronze: Raw API data → MinIO (incremental, 10min intervals)
Silver: Cleaned + temporal features + weather categories
Gold: Daily aggregates by city
```

## Quick Start
```bash
# Clone and setup
git clone <your-repo>
cd weather-data-pipeline
pip install -r requirements.txt

# Configure
cp pipeline.conf.example pipeline.conf
# Edit pipeline.conf with your API keys and MinIO credentials

# Run full pipeline
python main.py
```

## Architecture
```bash
src/
├── core/           # Config & clients
├── layers/         # Data processing
│   ├── bronze/     # Raw extraction
│   ├── silver/     # Cleaning & enrichment  
│   └── gold/       # Aggregations
├── utils/          # File ops & quality
└── orchestration/  # Pipeline flows
```

## Requirements
```
Python 3.8+
MinIO (S3-compatible storage)
OpenWeather API key
See requirements.txt for dependencies
```