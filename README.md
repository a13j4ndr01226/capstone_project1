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
> Set Up Transformation Pipeline: Pending

See:
- [`data_exploration_summary.md`](./data_exploration/data_exploration_summary.md)  
- [`data_storage_strategy.md`](./data_exploration/data_storage_strategy.md)  
- [`ERD_Normalized_Analytical_Views.png`](./ERD/ERD_Normalized_Analytical_Views.png)

## Project Structure

capstone_project1/
│
├── src/
│   ├── utils/
│   │   ├── add_genre.py
│   │   ├── add_timestamp.py
│   │   ├── auth.py
│   │   ├── count_artists.py
│   │   ├── dedup_artists.py
│   │   ├── genre_cahce.py
│   │   ├── get_genre.py
│   │   ├── google_trends_scraper.py
│   │   ├── json_to_csv.py
│   │   ├── normalize.py
│   │   ├── spotify_rising_artists.py
│   │   └── trends_cache.py
│   ├── artists_enricher.py
│   └── artists_scraper.py
├── proposal.md
├── requirements.txt
├── README.md
└── LICENSE

Notes: 
- 'artists_scraper.py' and 'artists_enricher.py' are the 2 main codes for extraction.
- Currently data samples are not included in repo but wil be added for reference

## Before Running it

1. Create a `.env` file and save it under a folder config/ in the project root to store your API keys:
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret

2. Install dependencies:
pip install -r requirements.txt

3. Ensure internet access and a valid IP address when running the Google Trends portion, as repeated requests 
may trigger temporary blocks.

## How to use it

1. Run the Spotify scraper to extract rising artists by genre and region
    python artists._scraper.py

2. Run the Google Trends enricher to get daily interst scores for those artists
    python artists_enricher.py

3. Output files will be saved in the data/ directory


## Data Exploration Highlights

See data_exploration_summary.md

- 18.5M records analyzed
- 14.5% of genres were missing — replaced with 'Unknown'
- trend_score nulls replaced with 0 for time series continuity
- Multi-genre entries exploded to one genre per row
- Cleaning and deduplication ensure 1 row per artist × genre × location × date

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

tqdm – for progress feedback

mathplotlib and seaborn - for data exploration
## License

This project is licensed under the MIT License.