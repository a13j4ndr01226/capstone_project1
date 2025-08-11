"""
artists_scraper.py

Main script to collect rising artist data from selected Spotify playlists and
store them in a local JSON file.

This script:
- Loads a local cache to avoid redundant Spotify API calls
- Extracts artist metadata from playlists known for rising artists
- Saves the scraped data in JSON format
- Updates the cache after execution
"""
import os
import json
import sys
from pathlib import Path
from datetime import datetime
from src.utils.json_to_csv import convert_json_to_csv
from src.utils.logger_config import logger
from src.utils.genre_cache import load_cache, save_cache
from src.utils.spotify_rising_artists import artist_by_playlistIDs


# Add src directory to Python path so modules can be imported
sys.path.append(str(Path(__file__).resolve().parent / "src"))

#These are playlists that have "on the rise" artists
playlist_dict = {
                    "If you need new music": "50ELv66L5ukDtOnbhM6dr5",
                    "Fresh Finds:EDM": "5CweKpXcP6I3p95u8zgIyb",
                    "Dreamstate So Cal 2024 Official Playlist": "1xmVnKmyX8jJrXmEGndRdY",
                    "Beyond Wonderland": "4KAqWt42J4Hrq0Bzfq3TLp",
                    "Beyond Wonderland 2025": "6LY7D0yFFV6hK0MitL0g7m",
                    "EDC 2024": "7cctoQsKcwCJKDIf6iRYg0",
                    "2025 New House and EDM Artists": "1mgdJfNwKJzpVYfQWZ61PZ",
                    "Insomniac Records": "4MrTARI697K18YFEBNVhDH",
                    "Insomniac New Music Friday": "4WVOpoFeIDUri7FiF1gRid",
                    "EDC Las Vegas 2025 Official Playlist": "6aKeNXbP3MirExlGu1tVXf",
                    "Feel a beat": "1dqp4ANVzYzkswv370d1mg",
                    "New Artist Discovery": "1ozCM0k4h6vrMlAzNaJCyy",
                    "Best New Artists": "6Q3lZzcoHYmmOp8KhTi9bN",
                    "Discover Artists/Music": "1c5DSGlZBgExVlfufN08Q2",
                    "Best Up & Coming Artist": "16QJFB0E6w1dHpF42AygUc"                    
}

# Get current date in YYYY_MM_DD format
batch_date = datetime.now().strftime('%Y_%m_%d')

# Output path
output_file = Path(f"data/spotify_rising_artists_{batch_date}.json")

def main():
    logger.info("Loading cache...")
    load_cache() 
    logger.info("Collecting artists from rising playlists...")
    
    artists = artist_by_playlistIDs(playlist_dict)

    output_file.parent.mkdir(exist_ok=True)  # Create data folder if missing
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(artists, f, indent=2)

    logger.info(f"\n Saved {len(artists)} artists to {output_file.resolve()}")
    logger.info("Saving cache...")
    save_cache()

    logger.info("Converting json to csv...")
    convert_json_to_csv() #Does not take in any parameters. Assumes json file is saved in path defined above
    logger.info("Conversion complete.")

if __name__ == "__main__":
    main()
