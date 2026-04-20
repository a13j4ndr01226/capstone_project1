# Music Artist Popularity Tracker by Local Area

## Objective

This project identifies where new and emerging artists are being searched for and listened to. Identifying trend signals for emerging artists in local areas can help event venues and promoters negotiate contracts that drive ticket sales while booking talent earlier and at lower cost.

---

## Executive Summary

This capstone evolved from a local end-to-end ETL prototype into a cloud-scaled analytical pipeline.

The final production submission uses:

- Azure Blob Storage for raw and transformed persistence
- Azure Databricks for scalable PySpark transformations
- Parquet outputs for downstream analytics
- Azure Monitor Workbook dashboards for operational monitoring

The resulting dataset helps identify artists and genres trending in specific locations over time.

---

## Status

> Raw Data Extraction: Complete  
> Batch Ingestion Set Up: Complete  
> Data Exploration: Complete  
> Data Cleaning and Transformation Logic: Complete  
> Set Up Transformation Pipeline: Complete  
> Scaled Execution (Azure Databricks + PySpark): Complete  
> Testing & Validation: Complete  
> Production Deployment: Complete  
> Monitoring Dashboard: Complete  

See:

- [`data_exploration_summary.md`](./data_exploration/data_exploration_summary.md)
- [`data_storage_strategy.md`](./data_exploration/data_storage_strategy.md)
- [`ERD_Normalized_Analytical_Views.png`](./ERD/ERD_Normalized_Analytical_Views.png)

---

## Dataset Characteristics

### Sources

- Spotify API — identifies rising artists through curated playlists
- Google Trends — measures regional search interest over time

### Analytical Grain

artist × genre × location × date

### Data Characteristics

- Time-series popularity data
- Regional market signals
- Multi-genre artists exploded into atomic rows
- Millions of transformed records
- Designed for trend analysis and location comparisons

---

## Final Pipeline Components

### Azure Blob Storage

Used as the cloud system of record for:

- raw extracted files
- transformed analytical outputs
- partitioned production datasets

### Azure Databricks

Used for scalable PySpark transformation workloads including:

- cleansing
- validation
- normalization
- genre explosion logic
- production dataset generation

### Parquet

Used as the final transformed format because it is:

- compressed
- columnar
- query efficient
- scalable

### Azure Monitor Workbook

Used to monitor production storage activity including:

- transactions
- used capacity
- ingress
- egress

---

## Final Architecture Notes

Earlier iterations of the project included PostgreSQL as a relational serving layer using the local ETL workflow.

The final cloud submission prioritizes scalable transformation and curated Parquet outputs in Azure Blob Storage, representing a modern lake-oriented analytical architecture.

The earlier PostgreSQL load logic remains in the repository to demonstrate complete ETL design considerations.

---

## Project Structure

capstone_project1/

├── env/  
├── data/  
├── data_exploration/  
├── ERD/  
├── cloud_architecture/  
├── src/  
│   ├── S1_extract/  
│   ├── S2_transform/  
│   ├── S3_load/  
│   └── utils/  
├── tests/  
├── main.py  
├── requirements.txt  
├── README.md  

Notes:

- Running `main.py` executes the original local ETL workflow.
- Azure production transformation jobs are executed through Databricks.

---

## Before Running It

### 1. Create `.env`

Store credentials in a config/environment file:

SPOTIFY_CLIENT_ID=
SPOTIFY_CLIENT_SECRET=
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_HOST=
POSTGRES_PORT=
POSTGRES_DB=

### 2. Install dependencies:

pip install -r requirements.txt

### 3. Internet Access

Ensure internet access and a valid IP address when running the Google Trends portion, as repeated requests may trigger temporary blocks.

## How to use it

### Local Prototype Workflow

python main.py
  Runs: extract.py, pandas_job.py, and load.py

### Cloud Workflow 

Production transformations are executed in Azure Databricks using PySpark jobs that read raw files from Azure Blob Storage and write transformed Parquet outputs back to Blob Storaage.

## Data Exploration Highlights

See data_exploration_summary.md

- 18.5M records analyzed
- 14.5% of genres were missing — replaced with 'Unknown'
- trend_score nulls replaced with 0 for time series continuity
- Multi-genre entries exploded to one genre per row
- Cleaning and deduplication ensure 1 row per artist × genre × location × date

## Scaled Execution (Step 6 – Cloud Prototype)

As part of Step 6 of the capstone, the existing transformation pipeline was scaled to run on
cloud-based distributed compute using Apache Spark and Azure infrastructure.

Key aspects of this step:

- Existing pandas-based transformation logic was migrated to **PySpark DataFrames**
- All original business rules and validation logic were preserved
- Execution was performed on a **cost-controlled Azure Databricks Spark cluster**
- **Azure Blob Storage** is used as the system of record for both inputs and outputs
- Transformed outputs are written as **partitioned Parquet files** for cloud efficiency
- Transformation metrics (row counts, invalid values, dropped records) are logged to verify correctness

## Step 8 – Testing Deployment and Validation

As part of Step 8, the project was validated in a test-style deployment workflow to confirm that both the utility logic and core PySpark transformation logic behave as expected before productionization.

### Testing Setup

- Created a project-local Python virtual environment for isolated test execution
- Added `pytest` and `pytest-cov` for test execution and coverage reporting
- Added local PySpark-based tests to validate Spark transformation logic outside Databricks
- Used `PYTHONPATH=.` during local test execution so project modules could be imported consistently

### Test Coverage Scope

The Step 8 test suite currently covers:

- Utility functions:
  - `normalize_text`
  - `deduplicate_artists`
- Core PySpark transformation logic:
  - `clean_and_validate`
  - `explode_genres`

### Edge Cases Covered

- whitespace trimming and lowercase normalization
- punctuation, symbol, and emoji removal
- duplicate artist ID handling
- missing genre defaults
- invalid date handling
- null and out-of-range trend score handling
- dropping rows missing required fields
- multi-genre parsing and explosion
- delimiter normalization for `;`, `|`, and `,`
- dropping empty genre tokens

### Current Test Results

- **Passed tests:** 12
- **Failed tests:** 0
- **Overall coverage:** 15%

### Key Testing/Deployment Adjustments Made

- Resolved local test environment setup with a dedicated `venv`
- Installed and configured local PySpark for transformation testing
- Fixed local Spark session configuration for pytest
- Resolved local Spark environment conflicts caused by an older standalone Spark installation
- Updated date parsing behavior in the PySpark cleaning logic to safely handle invalid dates during testing

This testing step complements the Azure Databricks execution completed in Step 6: Databricks validates cloud execution and Blob integration, while the Step 8 test suite validates transformation correctness and edge-case handling locally before production deployment.

## Step 9 – Deploy Production Code & Process Dataset

As part of Step 9, the production PySpark transformation pipeline was deployed and executed on fresh Azure Databricks compute in alignment with the Step 7 deployment architecture.

### Production Deployment Setup
- Created a fresh Azure Databricks compute cluster for the Step 9 run
- Used Databricks Runtime 16.4 LTS with Apache Spark 3.5.2
- Configured a single-node `Standard_D4s_v3` cluster with Photon enabled
- Enabled auto-termination after 10 minutes of inactivity as a cost-control measure
- Connected the production transform job to Azure Blob Storage using environment-based configuration

### Production Execution Improvements
The Step 6 Spark transformation job was updated to support a more production-like execution flow:
- `TRANSFORM_ONE_OFF_INPUT` remains available only as an optional override
- normal runs now use `RAW_ROOT` to automatically discover the latest raw CSV in Azure Blob Storage
- this removed the need to manually point the transform job to a specific input file during standard execution

### Production Run
The deployed Spark job successfully:
- auto-discovered the latest raw input file in Azure Blob Storage
- processed the full raw dataset using PySpark on Azure Databricks
- applied the cleaning and genre explosion transformation logic
- wrote partitioned Parquet output back to Azure Blob Storage

**Input path**
`wasbs://oe-container@spotimusicstorage.blob.core.windows.net/data/raw/2026_04_14/spotify_rising_with_trends_2026_04_14.csv`

**Output path**
`wasbs://oe-container@spotimusicstorage.blob.core.windows.net/data/transformed/2026_04_14/spotify_rising_cleaned_2026_04_14`

### Production Run Metrics
- Rows in: `1,473,492`
- Rows written: `2,143,836`
- Bad dates: `0`
- Out-of-range trend scores: `0`
- Dropped missing id/location/date: `0`
- Rows after genre explosion: `2,143,836`
- Dropped empty genres: `0`

### Final Notes
- Azure Blob Storage served as both the raw input source and transformed output target
- Databricks compute was used only for processing and was terminated after execution to control cloud cost
- This step completed the production deployment and end-to-end processing requirement for the cloud transformation portion of the capstone

## Step 10 – Monitoring Dashboard

Azure-native monitoring was implemented using Azure Monitor and Workbook dashboards to provide visibility into production storage activity and overall pipeline health.

### Components Completed

- Registered `Microsoft.Insights` resource provider
- Created Log Analytics Workspace
- Created Azure Workbook dashboard
- Added shareable dashboard access link for review

### Metrics Monitored

- Transactions
- Used Capacity
- Ingress
- Egress

### Purpose

- Validate production environment health
- Observe storage growth over time
- Monitor data movement activity
- Demonstrate operational readiness of the deployed pipeline

## Storage Strategy

See data_storage_strategy.md

Earlier local relational model included:

- Normalized star schema in PostgreSQL (3NF)
- dim_artists, dim_genres, dim_locations, artist_genres junction table
- fact_artist_trends stores clean, atomic trend data
- fact_genre_trends is derived via aggregation for BI querying
- Views/materializations optimized for filter-heavy queries by genre and region


## Technologies

Python 3.11

Spotify Web API (spotipy) – for playlist and artist metadata

Google Trends (via pytrends) – for regional interest over time

pandas – for data structuring

requests – for API interaction

dotenv – for managing credentials

SQLAlchemy - for database connection between python and PostgreSQL

tqdm – for progress feedback

mathplotlib and seaborn - for data exploration

Apache Spark (PySpark) – distributed processing for cloud-scale transformations

Azure Databricks – managed Spark compute for scalable execution

Azure Blob Storage – cloud persistence layer and system of record

Parquet – columnar storage format for scalable downstream analytics

## Future Enhancements

Potential next steps to further evolve this project include:

- Load curated Parquet outputs into a downstream warehouse or serving layer
- Build Power BI or Tableau dashboards for business users
- Add workflow orchestration using Apache Airflow
- Implement incremental batch processing instead of full refreshes
- Add automated data quality checks and alerting
- Introduce forecasting models for artist demand and market growth
- Expand data sources to include ticket sales, streaming counts, or social media signals
- Evaluate lakehouse architectures for direct cloud querying and analytics

## License

This project is licensed under the MIT License.