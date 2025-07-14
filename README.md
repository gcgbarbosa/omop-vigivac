# OMOP CDM Data Ingestion

## Project Description

This project provides tools and scripts for processing and managing
OMOP Common Data Model (CDM) data.
It includes functionalities for data ingestion,
conversion from Parquet to CSV, and handling various OMOP CDM tables.

## Installation

To set up the project, follow these steps:

1. **Clone the repository:**

   ```bash
   git clone https://github.com/your-repo/vigivac-omop.git
   cd vigivac-omop
   ```

2. **Install dependencies:**

   This project uses `uv` for dependency management. Ensure you have `uv` installed.

   ```bash
   uv sync
   ```

## Usage

### Data Ingestion

To ingest data, run the `ingestion.py` script:

```bash
python ingestion.py
```

### Parquet to CSV Conversion

To convert Parquet files to CSV, use the `parquet_to_csv.py` script:

```bash
python parquet_to_csv.py
```

### Spark Session

The `spark_session.py` script provides utilities
for setting up and managing Spark sessions:

```bash
python spark_session.py
```

### PostgreSQL Parameters

Configuration for PostgreSQL database connections can be found in `postgress_parameters.py`.

## Project Structure

```
.
├── ingestion.py            # Script for data ingestion
├── parquet_to_csv.py       # Script for converting Parquet files to CSV
├── postgress_parameters.py # PostgreSQL connection parameters
├── spark_session.py        # Utilities for Spark session management
├── omop_cdm/               # Contains scripts for individual OMOP CDM tables
│   ├── care_site.py
│   ├── condition_occurence.py
│   ├── drug_exposure.py
│   ├── location.py
│   ├── measurement.py
│   ├── observation_period.py
│   ├── person.py
│   └── visit_occurrence.py
├── artifacts/              # Directory for output artifacts (e.g., processed data)
├── csv/                    # Directory for CSV output files
├── parquet/                # Directory for Parquet input/output files
├── zip/                    # Directory for zipped input files
├── pyproject.toml          # Project metadata and dependencies (Poetry/uv)
├── uv.lock                 # uv lock file for reproducible environments
└── README.md               # This README file
```
