# Data Exploration Report 

**Project:** Music Artist Popularity Tracker by Local Area  
**Author:** Alejandro Peña  
**Date:** July 2025  
**File:** `data_exploration.ipynb`

---

## Overview

This report summarizes the findings from exploratory analysis on the enriched artist trend dataset. The goal of this step was to evaluate data quality, identify necessary cleaning steps, and prepare the data for transformation and loading into a PostgreSQL-based star schema.

---

## Dataset Summary

- **Records:** 18.5M rows
- **Columns:** 6
  - `artist`: string (artist name)
  - `id`: string (Spotify artist ID)
  - `genres`: semicolon-delimited string of genres
  - `location`: US state code (e.g., AK, CA)
  - `date`: date of trend score
  - `trend_score`: float score between 0–100

---

## Column Homogeneity Check

- `id` and `location` columns are consistent and well-formatted
- `genres` column has ~2.67M missing values (14.5%)
- `trend_score` column has 2,250 missing values; some values are greater than the expected 100 max score

---

## Cleaning Summary

- `trend_score` nulls are replaced with zeroes (0) to preserve date continuity
- `genres` nulls are replaced with 'Unknown' instead of removing the records
- `genres` were split and seprated into individual records. 1 genre per row
- `trend_score` outlier values outside 0–100 were removed
- Key column (`id`, `location`, or `date`) were checked for missing vlues. 0 Total rows were dropped confirming no current errors in the extraction process.
- All string columns were stripped for any whitespaces

---

## Key Transformations

- **Genre Normalization**: Split multi-genre strings and exploded them into one genre per row
- **String Cleaning**: Lowercased, stripped, and standardized key fields
- **Deduplication**: Ensured one row per artist–genre–location–date for fact table integrity

---

## Data Use Cases

This dataset will support downstream use by analysts and stakeholders in:

- Identifying top trending artists and genres by region
- Comparing artist momentum over time for booking decisions
- Detecting regional genre preferences
- Supporting forecasting and regional marketing strategies

---

## Storage Strategy

- Data modeled using a **3NF star schema**
- Normalized tables: `dim_artists`, `dim_genres`, `dim_locations`
- Fact tables:
  - `fact_artist_trends`: artist-level scores over time and location
  - `fact_genre_trends`: aggregated genre trends
- Materialized views or summary tables may be created for fast BI querying
- PostgreSQL selected for its robust analytical capabilities and compatibility with the DE stack

