"""
extract.py

Script executes spotify scraping and then enriches each artist with google trend score.

Final output is stored in data/stage2_trend_enrichment/spotify_rising_artists_with_trends_{batch_date}.csv 
"""

from src.s1_extract.artists_scraper import scrape
from src.s1_extract.artists_enricher import enricher

def main():
    scrape() #Scrapes Spotify Playlists for artist and their genres
    enricher() #Enriches data with Google Trend Score

if __name__ == "__main__":
    main()