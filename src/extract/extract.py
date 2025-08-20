from src.extract.artists_scraper import scrape
from src.extract.artists_enricher import enricher

def main():
    scrape() #Scrapes Spotify Playlists for artist and their genres
    enricher() #Enriches data with Google Trend Score

if __name__ == "__main__":
    main()