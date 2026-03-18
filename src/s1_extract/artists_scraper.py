"""
artists_scraper.py

Collect rising artist data from selected Spotify playlists and write it to:
  data/raw/{batch_date}/spotify_rising_artists_{batch_date}.json

Also updates the genre cache and optionally converts the JSON to CSV.
"""
import json
import sys
from pathlib import Path
from datetime import date
from typing import Dict, List, Any, Optional

# Add src directory to Python path so modules can be imported
sys.path.append(str(Path(__file__).resolve().parent / "src"))

from src.utils.json_to_csv import convert_json_to_csv
from src.utils.genre_cache import load_cache, save_cache
from utils.scrape_playlist import artist_by_playlistIDs
from src.utils.logger_config import get_logger
from src.utils.confirm_dir_exists import ensure_dir

SCRAPED_DIR = Path("data/raw")
FILE_STEM = "spotify_rising_artists"

logger = get_logger("Extract_Artist_Scraper")

#These are playlists that have "on the rise" artists
playlist_dict = {
                    "If you need new music": "50ELv66L5ukDtOnbhM6dr5",
                    "Fresh Finds:EDM": "5CweKpXcP6I3p95u8zgIyb",
                    "Insomniac Records": "4MrTARI697K18YFEBNVhDH",
                    "Insomniac New Music Friday": "4WVOpoFeIDUri7FiF1gRid",
                    "New Artist Discovery": "1ozCM0k4h6vrMlAzNaJCyy",
                    "Best New Artists": "6Q3lZzcoHYmmOp8KhTi9bN",
                    "Discover Artists/Music": "1c5DSGlZBgExVlfufN08Q2",
                    "Best Up & Coming Artist": "16QJFB0E6w1dHpF42AygUc"                    
}

def make_output_path(batch_date: str, file_stem: str) -> Path:
    dated_dir = SCRAPED_DIR / batch_date
    ensure_dir(dated_dir, logger=logger)
    return dated_dir / f"{file_stem}_{batch_date}.json"

def scrape(batch_date: Optional[str] = None, also_convert_csv: bool = True) -> Path:
    """
    Orchestrate scrape -> write JSON -> update cache -> optional CSV convert.
    Returns the path of the JSON written.
    """
    # Resolve batch_date
    if not batch_date:
        batch_date = date.today().strftime("%Y_%m_%d")

    output_file = make_output_path(batch_date, FILE_STEM)

    logger.info("Loading cache…")
    load_cache()

    logger.info("Collecting artists from rising playlists…")
    artists: List[Dict[str, Any]] = artist_by_playlistIDs(playlist_dict) or []

    if not artists:
        logger.warning("No artists were collected from playlists; writing empty list to JSON.")

    try:
        with output_file.open("w", encoding="utf-8") as f:
            json.dump(artists, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(artists)} artists to {output_file.resolve()}")
    except Exception as e:
        logger.exception(f"Failed writing JSON to {output_file}: {e}")
        raise

    logger.info("Saving cache…")
    save_cache()

    if also_convert_csv:
        try:
            convert_json_to_csv(output_file, logger=logger)
            logger.info("JSON → CSV conversion complete.")
        except Exception as e:
            logger.exception(f"Failed converting JSON to CSV for {output_file}: {e}")

    return output_file

def main() -> None:
    scrape()

if __name__ == "__main__":
    main()
