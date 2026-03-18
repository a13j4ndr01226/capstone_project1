import json
import time
import csv
import requests
from datetime import date
from pathlib import Path
from typing import Dict, List, Any

from src.utils.logger_config import get_logger
from src.utils.auth import get_auth_headers
from src.utils.confirm_dir_exists import ensure_dir

logger = get_logger("artist_scraper_test")

# -----------------------------
# Config
# -----------------------------

MARKETS = ["US", "GB","DE","FR","KR","JP","MX","CO","CA"] #U.S, U.K, Germany, France, South Korea, Japan, Mexico, Colombia, Canada
LIMIT = 50
MAX_RESULTS = 200  
SLEEP_SECONDS = 0.35
GENRE_CSV_PATH = Path("data/persisted_dims/dim_genres.csv")
RAW_DIR = Path("data/raw/artists")
batch_date = date.today().strftime("%Y_%m_%d")

# -----------------------------
# Helpers
# -----------------------------
def load_genres_from_csv(csv_path: Path) -> List[str]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Genre CSV not found: {csv_path}")

    genres: List[str] = []
    skipped = 0

    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        required_cols = {"genre_id", "genre"}
        if not required_cols.issubset(reader.fieldnames):
            raise ValueError(
                f"CSV must contain columns {required_cols}. "
                f"Found: {reader.fieldnames}"
            )

        for row in reader:
            raw_genre = row["genre"].strip()

            if not raw_genre or raw_genre.lower() in {"<na>", "na", "null"}:
                skipped += 1
                continue

            genres.append(raw_genre)

    logger.info(
        f"Loaded {len(genres)} genres from {csv_path} "
        f"(skipped {skipped} invalid rows)"
    )
    return genres


def make_output_path(batch_date: str, genre: str, market: str) -> Path:
    out_dir = RAW_DIR / batch_date / f"genre={genre}" / f"market={market}"
    ensure_dir(out_dir, logger=logger)
    return out_dir / "artists.json"

# -----------------------------
# Core Search
# -----------------------------
def search_artists_by_genre_market(
    genre: str,
    market: str,
) -> List[Dict[str, Any]]:

    headers = get_auth_headers()
    artists: List[Dict[str, Any]] = []

    query = f'genre:"{genre}"'

    for offset in range(0, MAX_RESULTS, LIMIT):
        params = {
            "q": query,
            "type": "artist",
            "market": market,
            "limit": LIMIT,
            "offset": offset,
        }

        start = time.time()
        resp = requests.get(
            "https://api.spotify.com/v1/search",
            headers=headers,
            params=params,
            timeout=10,
        )
        elapsed = round(time.time() - start, 2)

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 1))
            logger.warning(
                f"[429] genre={genre} market={market} "
                f"retry_after={retry_after}s"
            )
            time.sleep(retry_after)
            continue

        resp.raise_for_status()
        data = resp.json()

        items = data["artists"]["items"]

        logger.info(
            f"genre={genre} market={market} "
            f"offset={offset} returned={len(items)} "
            f"time={elapsed}s"
        )

        if not items:
            break

        # Store FULL artist object (raw ingestion)
        artists.extend(items)

        time.sleep(SLEEP_SECONDS)

    return artists

# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    logger.info("Starting artist ingestion (genre × market)")

    try:
        GENRES = load_genres_from_csv(GENRE_CSV_PATH)
    except Exception:
        logger.exception("Failed loading genres CSV")
        raise

    for genre in GENRES:
        for market in MARKETS:
            logger.info(f"START genre={genre} market={market}")

            artists = search_artists_by_genre_market(genre, market)

            logger.info(
                f"END genre={genre} market={market} "
                f"artists_collected={len(artists)}"
            )

            if not artists:
                logger.warning(
                    f"No artists returned for genre={genre}, market={market}"
                )
                continue

            output_file = make_output_path(batch_date, genre, market)

            try:
                with output_file.open("w", encoding="utf-8") as f:
                    json.dump(artists, f, ensure_ascii=False)

                logger.info(
                    f"Saved {len(artists)} artists → {output_file.resolve()}"
                )
            except Exception as e:
                logger.exception(
                    f"Failed writing artists for genre={genre}, market={market}: {e}"
                )
                raise
