# 🧠 Cognitive Performance Analyzer

A data ingestion subsystem designed to collect, validate, clean, and load cognitive performance data into PostgreSQL for analytics and insights generation.

## 📋 Overview

The Cognitive Performance Analyzer is a foundational data engineering pipeline that processes behavioral, cognitive, and environmental data to analyze factors affecting cognitive performance. It combines Python scripting with PostgreSQL for robust data management.

### Key Features

- **Multi-source Data Ingestion** — Reads from CSV files (behavioral, cognitive, external factors)
- **Data Validation** — Schema conformity checks with configurable validation rules
- **Data Cleaning** — Handles missing values, type casting, and standardization
- **Duplicate Handling** — UPSERT pattern prevents duplicate records
- **Rejection Tracking** — Invalid records logged with detailed error reasons
- **Structured Logging** — Full pipeline audit trail with run summaries
- **AI-Powered Reports** — Generates correlation analysis and PDF reports

## 🏗️ Architecture

```
Data Flow:

CSV Files (behavioral, cognitive, external)
    ↓
Reader Layer (pandas)
    ↓
Validation & Cleaning
    ↓
PostgreSQL Loader (UPSERT)
    ↓
Rejected Records Table + Structured Logs
    ↓
AI Insights & PDF Reports
```

### Core Modules

| Module | Responsibility |
|--------|----------------|
| `readers/` | Reads CSV data into pandas DataFrames |
| `validators/` | Validates records against configurable rules |
| `cleaners/` | Cleans and standardizes data |
| `loaders/` | Loads data into PostgreSQL with UPSERT |
| `loggers/` | Structured logging and rejection tracking |

## 📁 Project Structure

```
CognitivePerformanceAnalyzer/
├── src/
│   ├── cleaners/clean.py        # Data cleaning functions
│   ├── config/
│   │   ├── config.py            # Database configuration
│   │   └── validation_rules.yaml # Validation schema
│   ├── loaders/load.py          # PostgreSQL loader
│   ├── loggers/logger.py        # Structured logging
│   ├── readers/csv_reader.py    # CSV file readers
│   ├── validators/validate.py   # Data validation
│   └── run_pipeline.py          # Main pipeline orchestrator
├── tests/                       # PyTest test suite
├── data/                        # Input CSV files
├── notebooks/
│   └── demo_pipeline.ipynb      # Interactive demo
├── reports/
│   ├── scripts/                 # Report generation scripts
│   └── insights/                # Generated reports & visualizations
├── setup_database.py            # Database initialization
├── requirements.txt
└── README.md
```

## 🚀 Getting Started

### Prerequisites

- Python 3.11+
- PostgreSQL 14+

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/JustinDuvivier/cognitive-performance-analyzer.git
   cd cognitive-performance-analyzer
   ```

2. **Create virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment**
   
   Create a `.env` file in the project root:
   ```env
   DB_HOST=localhost
   DB_PORT=5432
   DB_USER=postgres
   DB_PASSWORD=your_password
   DB_NAME=cognitive_performance_db
   OPENAI_API_KEY=your_openai_api_key
   ```
   
   > **Note:** The `OPENAI_API_KEY` is required for generating AI insights in the PDF report. Get your API key from [OpenAI Platform](https://platform.openai.com/api-keys).

5. **Initialize the database**
   ```bash
   python setup_database.py
   ```

### Running the Pipeline

```bash
cd src
python run_pipeline.py
```

Or use the interactive demo notebook:
```bash
jupyter notebook notebooks/demo_pipeline.ipynb
```

## 🗄️ Database Schema

### Main Tables

```sql
-- Persons table
CREATE TABLE persons (
    person_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    location_name TEXT,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6)
);

-- Measurements table (stores all data points)
CREATE TABLE measurements (
    measurement_id SERIAL PRIMARY KEY,
    person_id INTEGER REFERENCES persons(person_id),
    timestamp TIMESTAMP NOT NULL,
    -- Environmental factors
    pressure_hpa DECIMAL(6,2),
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    -- Behavioral metrics
    sleep_hours DECIMAL(4,2),
    steps INTEGER,
    screen_time_minutes INTEGER,
    -- Cognitive scores
    sequence_memory_score INTEGER,
    reaction_time_ms DECIMAL(7,2),
    verbal_memory_words INTEGER,
    -- ... additional fields
    UNIQUE(person_id, timestamp)
);

-- Rejected records for audit
CREATE TABLE rejected_records (
    reject_id SERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    reason TEXT NOT NULL,
    rejected_at TIMESTAMP DEFAULT NOW()
);
```

## ⚙️ Configuration

### Validation Rules (`src/config/validation_rules.yaml`)

```yaml
measurements_external:
  pressure_hpa:
    type: float
    min: 870
    max: 1084
    nullable: true
  temperature:
    type: float
    min: -50
    max: 60
    nullable: true
```

## 🧪 Testing

Run the test suite:
```bash
PYTHONPATH=src pytest tests/ -v
```

Run with coverage:
```bash
PYTHONPATH=src pytest tests/ --cov=src --cov-report=term-missing
```

**Current Coverage: 80%**

## 📊 Generated Reports

The pipeline can generate:
- **Correlation Heatmap** — Visual analysis of factor relationships
- **AI-Powered PDF Report** — Automated insights and recommendations

```bash
python reports/scripts/generate_insights.py
python reports/scripts/generate_report.py
```

## 🔄 Pipeline Output Example

```
============================================================
COGNITIVE PERFORMANCE PIPELINE - Starting
============================================================
Reading external factors from CSV...
Read 168 external records from CSV
Reading user tracking data from CSVs...
Read 168 user tracking records

PIPELINE SUMMARY
Duration: 0.06 seconds

Total Records:
  Read: 336
  Validated: 336
  Loaded: 336
  Rejected: 0

Database Counts:
  persons: 4
  measurements: 168
  rejected_records: 0
============================================================
✅ COGNITIVE PERFORMANCE PIPELINE - COMPLETE
============================================================
```

## 🚀 Stretch Goals (Future Enhancements)

| Area | Feature                                                                       |
|------|-------------------------------------------------------------------------------|
| **Data Expansion** | Track additional Apple Watch metrics: noise exposure, respiratory rate, stand hours. |
| **Data Collection** | Run pipeline over longer periods (3-6 months) for more accurate correlations. |
| **Analytics** | Add lag analysis to correlate yesterday's factors with today's performance.   |
| **Orchestration** | Schedule automated pipeline runs using Airflow.                               |


