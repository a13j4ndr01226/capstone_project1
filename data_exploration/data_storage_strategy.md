# Data Storage Strategy

**Project:**  Music Artist Popularity Tracker by Local Area  
**Author:** Alejandro Peña  
**Date:** July 2025  
**File:** `data_storage_strategy.md`

---

## Intended Use 

The enriched artist trend dataset is designed for use by data analysts and stakeholders to answer business-critical questions such as:

- **Which artists are trending in a specific region (e.g., Nevada)?**
- **Which genres are rising or declining in specific locations over time?**
- **Should a booking agent be contacted based on artist popularity trends?**
- **Are artists proposed by booking agents gaining momentum locally or regionally?**

These queries are time-sensitive and filter-heavy, typically requiring grouping by artist, genre, location, and date.

---

## Storage Design Overview

To meet these needs, I’ve implemented a two-layered data storage architecture:

### Layer 1: Normalized Star Schema (3NF)

This layer prioritizes:
- **Data consistency**
- **Minimal redundancy**
- **Ease of transformation and maintenance**

**Normalized Tables:**
- `dim_artists`: Unique artists and their identifiers
- `dim_genres`: Unique genre names
- `dim_locations`: US state codes and region labels
- `artist_genres`: Junction table (many-to-many between artists and genres)

**Fact Table:**
- `fact_artist_trends`: Daily trend scores per artist and location (does **not** include genre)

  Note: Genres are stored separately and not included in this fact table to avoid duplicating trend scores across genres and violating normalization.

---

### Layer 2: Denormalized Tables for Querying

To support fast, intuitive analysis (especially for dashboards), I derive denormalized views from the normalized tables:

- **Exploded Artist Trends View (not stored):**
  - Combines `fact_artist_trends` with `artist_genres`
  - Produces 1 row per artist × genre × date × location
  - Used **only for genre-level aggregations** not stored as a base fact table

- **`fact_genre_trends` (aggregated table):**
  - Derived from the exploded view
  - Aggregated trend scores by genre, location, and date

---

## Performance Optimization

- **Indexes** on:
  - `trend_date`
  - `artist_id`
  - `genre_id`
  - `state_code`
- **Materialized views** or summary tables precomputed for genre aggregations
- **Export/Storage Compression**:
  - Compressed columnar formats (e.g., Parquet or GZIP CSV) for archival or external use

---

## Summary

This architecture uses:
- A normalized core (3NF) for integrity and transformation logic
- Derived views to support analytical access patterns
- A clear separation between raw facts (`fact_artist_trends`) and derived aggregations (`fact_genre_trends`)

It ensures data is clean, scalable, and aligned with how stakeholders will interact with it to drive event and booking decisions.