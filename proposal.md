# Music Artist Popularity Tracker by Local Area

## Problem Statement

Talent bookers at music venues, festivals, and clubs often rely on headline names or past experience to decide which artists to book. While this approach helps draw large crowds, booking top-tier talent is costly and can significantly reduce profit margins—especially if ticket sales do not offset high booking fees.

Emerging artists, on the other hand, may offer more affordable booking costs but come with a higher risk of poor turnout—unless there is data showing they are gaining momentum. This project aims to solve that problem by building a data pipeline that tracks rising music artists by genre and location. The goal is to help entertainment venues make informed, data-driven talent booking decisions that balance affordability and audience appeal.

## Data Sources

To support the problem, the following datasets and APIs will be used:

- **Spotify API**:  
  For real-time artist metadata, genre tagging (via playlists), and popularity scoring. Spotify does not offer detailed regional streaming, but regional interest can be inferred using curated playlists and third-party charts.

- **Kaggle: Spotify Top 200 Charts (2020–2021)**  
  https://www.kaggle.com/datasets/sashankpillai/spotify-top-200-charts-20202021  
  Includes daily chart positions by country.

- **Kaggle: Spotify Streaming History Dataset**  
  https://www.kaggle.com/datasets/arshmankhalid/shopify-streaming-history-dataset  
  Provides user-level streaming history and listening trends.

- **Google Trends (via PyTrends)**:  
  Regional search interest for artist names over time.

- **SoundCloud** (Scraped):  
  Early-stage artist activity including track plays, uploads, and social tags.

These data sources collectively provide a multi-platform view of artist momentum across streaming platforms and public search behavior.

## Data Engineering Plan

### Extraction

- Use Spotify API to pull artist metadata and public playlists.
- Use PyTrends to extract Google Trends data by region and time.
- Load Kaggle datasets for historical chart and streaming patterns.
- Write scraping scripts to extract artist-level stats from SoundCloud.

### Transformation

- Normalize artist names across platforms to align data.
- Tag songs and artists by genre using Spotify metadata.
- Aggregate time-series data to calculate popularity trajectories.
- Rank artists based on rate of change in popularity (velocity scoring).
- Filter by location using inferred regional data and Google Trends.

### Load

- Store structured data in BigQuery or PostgreSQL for efficient querying.
- Build materialized views for top artists by genre and region.
- Optionally integrate with dashboard tools (e.g., Looker Studio, Streamlit).

## Cloud Scalability Plan

To ensure the solution is scalable:

- BigQuery will be used as the central data warehouse.
- Scheduled extraction jobs will run using Cloud Scheduler or Prefect.
- Processed data will be stored in staging and analytics layers for future reporting and modeling.
- The stack is designed to support weekly or daily updates as new artist data becomes available.

## Deliverables

- A functional ETL pipeline that aggregates artist popularity metrics across Spotify, SoundCloud, and Google Trends.
- A ranking system to identify rising artists by genre and region.
- A dashboard or report that allows stakeholders to visualize trends and discover emerging talent before their prices peak.
