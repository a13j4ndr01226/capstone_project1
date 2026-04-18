"""
main.py

Script executes ETL Pipeline.
    - Extracts from Spotify API
    - Transforms/Cleans daily artist trend scores
    - Loads cleaned data to PostgreSQL Database

Each run loads 1 month of data for all rising artists. (Artists x U.S States X 1 month of data).
These are loaded to the database with a load timestamp.
"""

from src.s1_extract.artists_scraper import scrape
from src.s1_extract.artists_enricher import enricher
from src.s2_transform.pandas_job import transform
from src.s3_load.load import load

def main():
    scrape() #Scrapes Spotify Playlists for artist and their genres
    enricher() #Enriches data with Google Trend Score
    transform() #Cleans and transforms locally
    load() #Loads to PostgreSQL


if __name__ == "__main__":
    main()