"""
artists_enricher.py

Enrich artists with Google Trends efficiently:
- build worklist from cache misses only
- optional US "gate" (fast pre-check) before expanding to states
- small thread pool per-artist to parallelize regions safely
- frequent cache saves for resilience
"""

import json
import time
import random
import argparse
import threading
import signal
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, date
from pathlib import Path
from src.utils.jsonl_to_csv import convert_jsonl_to_csv
from src.utils.logger_config import logger
from src.utils.trends_cache import load_cache, save_cache, get_cached_score
from src.utils.google_trends_scraper import get_trend_score_last_complete_month, install_stop_event


MAX_WORKERS_PER_ARTIST = 3 # 3–6 is usually safe
USE_US_GATE = True           # quick screen before per-state expansion
US_GATE_MIN_PEAK = 50        # min peak interest to "promote" to states
SAVE_CACHE_EVERY_N_ARTISTS = 1

batch_date = datetime.now().strftime('%Y_%m_%d')
INPUT_FILE = Path(f"data/stage1_artists/spotify_rising_artists_{batch_date}.json")
OUTPUT_FILE = Path(f"data/stage2_trend_enrichment/spotify_rising_with_trends_{batch_date}.jsonl")

STOP_EVENT = threading.Event()
install_stop_event(STOP_EVENT)

def _on_sigint(signum, frame):
    logger.warning("SIGINT received — requesting stop...")
    STOP_EVENT.set()

signal.signal(signal.SIGINT, _on_sigint)

regions = {
    "AL": "US-AL", "AK": "US-AK", "AZ": "US-AZ", "AR": "US-AR", "CA": "US-CA", "CO": "US-CO", "CT": "US-CT", "DE": "US-DE", "FL": "US-FL", "GA": "US-GA",
    "HI": "US-HI", "ID": "US-ID", "IL": "US-IL", "IN": "US-IN", "IA": "US-IA", "KS": "US-KS", "KY": "US-KY", "LA": "US-LA", "ME": "US-ME", "MD": "US-MD",
    "MA": "US-MA", "MI": "US-MI", "MN": "US-MN", "MS": "US-MS", "MO": "US-MO", "MT": "US-MT", "NE": "US-NE", "NV": "US-NV", "NH": "US-NH", "NJ": "US-NJ",
    "NM": "US-NM", "NY": "US-NY", "NC": "US-NC", "ND": "US-ND", "OH": "US-OH", "OK": "US-OK", "OR": "US-OR", "PA": "US-PA", "RI": "US-RI", "SC": "US-SC",
    "SD": "US-SD", "TN": "US-TN", "TX": "US-TX", "UT": "US-UT", "VT": "US-VT", "VA": "US-VA", "WA": "US-WA", "WV": "US-WV", "WI": "US-WI", "WY": "US-WY"
}

#---------------------------------------------------Helpers------------------------------------------------------
def _last_complete_month_label() -> str:
    today = date.today()
    if today.month == 1:
        y, m = today.year - 1, 12
    else:
        y, m = today.year, today.month - 1
    return datetime(y, m, 1).strftime('%b_%Y')  # e.g., "Jul_2025"

def passes_us_gate(name: str, min_peak: int = US_GATE_MIN_PEAK) -> bool:
    """Validates minimum interest across the US. Low interest gets skipped in API call."""
    us_scores = get_trend_score_last_complete_month(name, "US")
    if not us_scores:
        return False
    return max(us_scores.values()) >= min_peak

def _region_job(artist_name: str, region_label: str, geo: str):
    """Thread worker to fetch last-complete-month daily scores for a region."""
    if STOP_EVENT.is_set():
        return region_label, None
    daily = get_trend_score_last_complete_month(artist_name, geo)
    return region_label, daily

def get_processed_artist_names(filepath: Path) -> set:
    """
    Resume safely by skipping artists already written to the JSONL output.
    """
    if not filepath.exists():
        return set()

    names = set()
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                nm = rec.get("artist", "").strip().lower()
                if nm:
                    names.add(nm)
            except json.JSONDecodeError as e:
                logger.error(f"{e}")
                continue
    return names

def parse_args():
    """
    Deterministic first 50: python -m src.artists_enricher --limit 50 --tag test
    Random 50: python -m src.artists_enricher --limit 50 --sample --tag test
    First 50 whose names start with “a”: python -m src.artists_enricher --starts-with a --limit 50 --tag test
    """
    p = argparse.ArgumentParser(description="Enrich artists with Google Trends")
    p.add_argument("--limit", type=int, default=None,
                   help="Only process this many artists (head of list unless --sample).")
    p.add_argument("--sample", action="store_true",
                   help="Randomly sample from the input (requires --limit).")
    p.add_argument("--starts-with", type=str, default=None,
                   help="Only include artists whose name (case-insensitive) starts with this string.")
    p.add_argument("--tag", type=str, default=None,
                   help="Append a tag to the output filename, e.g., --tag test")
    return p.parse_args()

#---------------------------------------------Core----------------------------------------------------------------------
def enrich_artist(artist: dict) -> dict:
    """
    Enrich a single artist with last complete month's daily scores across regions:
    - skip regions already cached
    - parallelize remaining few using a small thread pool
    """
    name = artist.get("artist", "").strip()
    logger.info(f"\nPROCESSING: {name}")
    month_label = _last_complete_month_label()

    # Optional gate: only expand to states if US shows a minimum interest
    if USE_US_GATE and not passes_us_gate(name, US_GATE_MIN_PEAK):
        logger.info(f"US gate not passed for '{name}' (peak<{US_GATE_MIN_PEAK}). Skipping state-by-state enrichment.")
        artist["daily_trends_US_only"] = True
        return artist

    # Build jobs only for cache misses; attach cached immediately
    jobs = []
    for region_label, geo in regions.items():
        cache_id = f"{name}|{geo}|{month_label}"
        cached = get_cached_score(cache_id)
        if cached is None:
            jobs.append((region_label, geo))
        else:
            artist[f"daily_trends_{region_label}"] = cached

    if not jobs:
        logger.info("All regions already cached for this artist.")
        return artist

    # Small bounded thread pool
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_ARTIST) as ex:
        futures = {ex.submit(_region_job, name, r_label, geo): r_label for r_label, geo in jobs}
        try:
            while futures and not STOP_EVENT.is_set():
                done, _ = wait(list(futures.keys()), timeout=1.0, return_when=FIRST_COMPLETED)
                for fut in done:
                    r_label = futures.pop(fut)
                    try:
                        region_label, daily_scores = fut.result()
                        if daily_scores:
                            artist[f"daily_trends_{region_label}"] = daily_scores
                            logger.info(f"TOTAL {region_label} = {len(daily_scores)} entries")
                        else:
                            logger.warning(f"No data for {name} in {region_label}")
                    except Exception as e:
                        logger.error(f"Region job failed for {name} ({r_label}): {e}")
        except KeyboardInterrupt:
            logger.warning("Interrupt caught in enrich_artist — cancelling region jobs...")
            STOP_EVENT.set()
            # cancel any pending futures
            for fut in futures:
                fut.cancel()
            # stop the executor immediately (Python 3.9+: cancel_futures available)
            ex.shutdown(wait=False, cancel_futures=True)
            return artist
    return artist

def enricher():
    args = parse_args()

    # Use the constants defined at the top so downstream (CSV converter) finds the file
    input_file = INPUT_FILE
    output_file = OUTPUT_FILE

    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return

    # Ensure output dir exists before we append
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(input_file, "r", encoding="utf-8") as f:
        input_artists = json.load(f)

    # ---------- TEST FILTERING & LIMITING ----------
    if args.starts_with:
        pref = args.starts_with.lower()
        input_artists = [a for a in input_artists
                         if a.get("artist", "").strip().lower().startswith(pref)]
        logger.info(f"Filtered by starts-with='{args.starts_with}': {len(input_artists)} artists remain.")

    if args.limit is not None:
        if args.sample:
            k = min(args.limit, len(input_artists))
            input_artists = random.sample(input_artists, k)
            logger.info(f"Random sample of {k} artists selected for test run.")
        else:
            input_artists = input_artists[:args.limit]
            logger.info(f"First {len(input_artists)} artists selected for test run.")
    # ------------------------------------------------

    processed_names = get_processed_artist_names(output_file)
    load_cache()

    saved_since_flush = 0

    try:
        with open(output_file, "a", encoding="utf-8") as out:
            for artist in input_artists:
                if STOP_EVENT.is_set():
                    break

                name = artist.get("artist", "").strip()
                if not name:
                    continue
                lname = name.lower()

                if lname in processed_names:
                    logger.info(f"Skipping already processed: {name}")
                    continue

                enriched = enrich_artist(artist)  # this function contains the thread pool
                out.write(json.dumps(enriched) + "\n")
                logger.info(f"SAVED: {name}")

                saved_since_flush += 1
                if saved_since_flush >= SAVE_CACHE_EVERY_N_ARTISTS:
                    save_cache()
                    saved_since_flush = 0

                time.sleep(random.uniform(0.2, 0.8))
    except KeyboardInterrupt:
        logger.warning("Interrupted — saving cache and exiting.")
        STOP_EVENT.set()
        return
    finally:
        save_cache()
        logger.info(f"Data saved to {output_file.resolve()}")
    
    logger.info("Converting jsonl to csv...")
    convert_jsonl_to_csv()  
    logger.info("Conversion complete.")

def main():
    enricher()

if __name__ == "__main__":
    main()
