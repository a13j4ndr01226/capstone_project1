# Music Artist Popularity Tracker by Local Area

## Objective

This project will identify where new and emerging artists are being searched for and listened to. Identifying 
the trend of emerging artists in local areas will help event venues and promoters negotiate contracts
that will bring in ticket sales and host artists that are on the rise at a better prices.

## Project Structure

capstone_project1/
├── data/
│   ├── cache/
│   │   └── artists_genre_cache.json
│   └── spotify_rising_artists.json
├── src/
│   ├── utils/
│   │   ├── add_genre.py
│   │   ├── dedup_artists.py
│   │   ├── genre_cache.py
│   │   ├── get_genre.py
│   │   ├── normalize.py
│   │   └── trends_cache.py
│   ├── auth.py
│   ├── google_trends.py
│   └── spotify_rising_artists.py
├── artists_enricher.py
├── artists_scraper.py
├── proposal.md
├── readme.md
├── requirements.txt
└── LICENSE

Note: artists_scraper and artists_enricher are the 2 main codes for extraction.

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

## Technologies

Python 3.11

Spotify Web API – for playlist and artist metadata

Google Trends (via pytrends) – for regional interest over time

pandas – for data structuring

requests – for API interaction

dotenv – for managing credentials

tqdm – for progress feedback

## License

This project is licensed under the MIT License.