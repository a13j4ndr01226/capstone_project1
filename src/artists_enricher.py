"""
artists_enricher.py

Enriches a list of musical artists with 1 year of daily Google Trends interest scores
across multiple U.S. regions. Each enriched artist record is saved as a line in a JSONL file.

This script:
- Loads raw artist data from a JSON file
- Checks which artists have already been processed
- Fetches trend scores per region using pytrends
- Appends each enriched artist to a JSONL output file
- Supports resumption and fault tolerance for large datasets
"""

import json
import time
import random
from datetime import datetime
from pathlib import Path
from src.utils.google_trends_scraper import get_trend_score_1y_loop
from src.utils.trends_cache import load_cache, save_cache

# batch_date = datetime.now().strftime('%Y_%m_%d')
batch_date = '2025_06_18'

INPUT_FILE = Path(f"data/spotify_rising_artists_{batch_date}.json")
OUTPUT_FILE = Path(f"data/spotify_rising_with_trends_{batch_date}.jsonl")

#Add or change regions as needed. Current project focuses on the Atlantic City Area.
regions = {
        "AL": "US-AL", "AK": "US-AK", "AZ": "US-AZ", "AR": "US-AR", "CA": "US-CA", "CO": "US-CO", "CT": "US-CT", "DE": "US-DE", "FL": "US-FL", "GA": "US-GA",
        "HI": "US-HI", "ID": "US-ID", "IL": "US-IL", "IN": "US-IN", "IA": "US-IA", "KS": "US-KS", "KY": "US-KY", "LA": "US-LA", "ME": "US-ME", "MD": "US-MD",
        "MA": "US-MA", "MI": "US-MI", "MN": "US-MN", "MS": "US-MS", "MO": "US-MO", "MT": "US-MT", "NE": "US-NE", "NV": "US-NV", "NH": "US-NH", "NJ": "US-NJ",
        "NM": "US-NM", "NY": "US-NY", "NC": "US-NC", "ND": "US-ND", "OH": "US-OH", "OK": "US-OK", "OR": "US-OR", "PA": "US-PA", "RI": "US-RI", "SC": "US-SC",
        "SD": "US-SD", "TN": "US-TN", "TX": "US-TX", "UT": "US-UT", "VT": "US-VT", "VA": "US-VA", "WA": "US-WA", "WV": "US-WV", "WI": "US-WI", "WY": "US-WY"
}

def get_processed_artist_names(filepath):
    """
    Extracts a set of artist names that have already been processed and saved
    in the output JSONL file.

    Args:
        filepath (Path): Path to the JSONL file containing previously enriched artist records.

    Returns:
        set: A set of lowercased artist names.
    """
    if not filepath.exists():
        return set()

    names = set()
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            try:
                artist = json.loads(line)
                name = artist.get("artist", "").strip().lower()
                if name:
                    names.add(name)
            except json.JSONDecodeError:
                continue
    return names

def enrich_artist(artist):
    """
    Enriches a single artist record with 1 year of daily Google Trends scores for each specified region.

    Args:
        artist (dict): The artist record containing at least an 'artist' key.

    Returns:
        dict: The enriched artist record with added trend data.
    """
    name = artist.get("artist", "").strip()
    print(f"\nPROCESSING: {name}")

    for region_label, geo in regions.items():
        daily_scores = get_trend_score_1y_loop(name, geo)
        
        if daily_scores:
            artist[f"daily_trends_{region_label}"] = daily_scores
            print(f"TOTAL {region_label.upper()} = {len(daily_scores)} entries")
        
        else:
            print(f"WARNING: No data for {name} in {region_label}")
        
        time.sleep(random.uniform(10, 25))  # Throttle between regions

    return artist

def main():
    """
    Main execution function that orchestrates the enrichment process:
    - Loads artist list from a JSON input file
    - Loads cache and previously processed artists
    - Enriches each new artist with Google Trends data
    - Appends each enriched record to a JSONL output file
    - Updates the trends cache on completion
    """
    if not INPUT_FILE.exists():
        print(f"ERROR: Input file not found: {INPUT_FILE}")
        return

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        input_artists = json.load(f)

    processed_names = get_processed_artist_names(OUTPUT_FILE)
    load_cache()

    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:

        for artist in input_artists:
            name = artist.get("artist", "").strip().lower()
            
            if name in processed_names:
                print(f"Skipping already processed: {name}")
                continue

            enriched = enrich_artist(artist)

            f.write(json.dumps(enriched) + "\n")
            save_cache()
            print(f"SAVED: {name}")
            
            
            time.sleep(random.uniform(10, 15))  # Throttle between artists

    print(f"Data saved to {OUTPUT_FILE.resolve()}")

if __name__ == "__main__":
    main()
