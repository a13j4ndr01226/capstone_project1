# Music Artist Popularity Tracker by Local Area

## Objective

This project will identify where new and emerging artists are being searched for and listened to. Identifying 
the trend of emerging artists in local areas will help event venues and promoters negotiate contracts
that will bring in ticket sales and host artists that are on the rise at a better prices.

## Status
> Raw Data Extraction: Complete  
> Batch Ingestion Set Up: Complete  
> Data Exploration: Complete  
> Data Cleaning and Transformation Logic: Complete  
> Set Up Transformation Pipeline: Complete  
> **Scaled Execution (Azure Databricks + PySpark): Complete**


See:
- [`data_exploration_summary.md`](./data_exploration/data_exploration_summary.md)  
- [`data_storage_strategy.md`](./data_exploration/data_storage_strategy.md)  
- [`ERD_Normalized_Analytical_Views.png`](./ERD/ERD_Normalized_Analytical_Views.png)

## Project Structure

capstone_project1/
│
├── env/ -- stores sensitive variables
├── data/ -- stores caches and persisted data from different points of the pipeline
├── data_exploration/
│   ├── data_exploration_summary.md
│   ├── data_exploration.jpynb
│   ├── data_storage_strategy.md 
├── ERD
│   ├── ERD_Normalized + Analytical Views.png
├── src/
│   ├── S1_extract/
│   │   ├── artist_scraper.py
│   │   ├── artist_enricher.py
│   │   ├── extract.py
│   ├── S2_transform/
│   │   ├── dim_persist.py
│   │   ├── transform.py
│   ├── S3_load/
│   │   ├── load.py
│   └── utils/
│       ├── add_genre.py
│       ├── add_timestamp.py
│       ├── auth.py
│       ├── confirm_dir_exists.py
│       ├── count_artists.py
│       ├── dedup_artists.py
│       ├── find_latest_file.py
│       ├── genre_cache.py
│       ├── get_genre.py
│       ├── google_trends_scraper.py
│       ├── json_to_csv.py
│       ├── jsonl_to_csv.py
│       ├── logger_config.py
│       ├── normalize.py
│       ├── spotify_rising_artists.py
│       └── trends_cache.py
├── main.py
├── proposal.md
├── requirements.txt
├── README.md
└── LICENSE

Notes: 
- Running 'main.py' script will run the entire extract, transform, and load pipeline

## Before Running it

1. Create a `.env` file and save it under a folder config/ in the project root to store your API keys:
    SPOTIFY_CLIENT_ID=your_spotify_client_id
    SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
    POSTGRES_USER= ****
    POSTGRES_PASSWORD=****
    POSTGRES_HOST=localhost
    POSTGRES_PORT=****
    POSTGRES_DB=capstone_project1
    POSTGRES_SCHEMA_STAGING=staging
    POSTGRES_TABLE_STAGING=stg_spotify_artist_trend_scores

2. Install dependencies:
pip install -r requirements.txt

3. Ensure internet access and a valid IP address when running the Google Trends portion, as repeated requests 
may trigger temporary blocks.

## How to use it

1. Run the main.py to run the entire pipeline
    a. Alternatively each step can be ran separately using: extract.py, transform.py, and load.py

2. Output files will be saved in their corresponding directories based on the date the scripts are run

3. Log files are similary stored in ther separate logs/ directory


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

## Storage Strategy

See data_storage_strategy.md

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


## License

This project is licensed under the MIT License.