# src/s1_extract/test_artists_enricher.py
import json, time, random, threading, signal, re
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, date
from pathlib import Path
from src.utils.jsonl_to_csv import convert_jsonl_to_csv
from src.utils.logger_config import get_logger
from src.utils.trends_cache import load_cache, save_cache, get_cached_score
from utils.scrape_google_trends import get_trend_score_last_complete_month, install_stop_event
from src.utils.confirm_dir_exists import ensure_dir

logger = get_logger("Extract_Artist_Enricher")

MAX_WORKERS_PER_ARTIST = 3
USE_US_GATE = True
US_GATE_MIN_PEAK = 50
SAVE_CACHE_EVERY_N_ARTISTS = 1

ENRICHED_DIR = Path("data/raw")
SCRAPER_STEM = "spotify_rising_artists"
FILE_STEM = "spotify_rising_with_trends"

STOP_EVENT = threading.Event()
install_stop_event(STOP_EVENT)

_DATE_RE = re.compile(r"^\d{4}_\d{2}_\d{2}$")

def _on_sigint(signum, frame):
    logger.warning("SIGINT received — requesting stop.")
    STOP_EVENT.set()

signal.signal(signal.SIGINT, _on_sigint)


regions = {
    "AL": "US-AL","AK":"US-AK","AZ":"US-AZ","AR":"US-AR","CA":"US-CA","CO":"US-CO","CT":"US-CT","DE":"US-DE","FL":"US-FL","GA":"US-GA",
    "HI":"US-HI","ID":"US-ID","IL":"US-IL","IN":"US-IN","IA":"US-IA","KS":"US-KS","KY":"US-KY","LA":"US-LA","ME":"US-ME","MD":"US-MD",
    "MA":"US-MA","MI":"US-MI","MN":"US-MN","MS":"US-MS","MO":"US-MO","MT":"US-MT","NE":"US-NE","NV":"US-NV","NH":"US-NH","NJ":"US-NJ",
    "NM":"US-NM","NY":"US-NY","NC":"US-NC","ND":"US-ND","OH":"US-OH","OK":"US-OK","OR":"US-OR","PA":"US-PA","RI":"US-RI","SC":"US-SC",
    "SD":"US-SD","TN":"US-TN","TX":"US-TX","UT":"US-UT","VT":"US-VT","VA":"US-VA","WA":"US-WA","WV":"US-WV","WI":"US-WI","WY":"US-WY"
}

def _last_complete_month_label() -> str:
    today = date.today()
    y, m = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)
    return datetime(y, m, 1).strftime("%b_%Y")

def passes_us_gate(name: str, min_peak: int = US_GATE_MIN_PEAK) -> bool:
    us_scores = get_trend_score_last_complete_month(name, "US")
    return bool(us_scores) and max(us_scores.values()) >= min_peak

def _region_job(artist_name: str, region_label: str, geo: str):
    if STOP_EVENT.is_set():
        return region_label, None
    daily = get_trend_score_last_complete_month(artist_name, geo)
    return region_label, daily

def _make_scraper_input_path(batch_date: str) -> Path:
    return ENRICHED_DIR / batch_date / f"{SCRAPER_STEM}_{batch_date}.json"

def _make_output_jsonl_path(batch_date: str) -> Path:
    dated_dir = ENRICHED_DIR / batch_date
    ensure_dir(dated_dir, logger=logger)
    return dated_dir / f"{FILE_STEM}_{batch_date}.jsonl"

def _latest_batch_with_scraper_file():
    """Pick newest data/raw/{date}/spotify_rising_artists_{date}.json"""
    if not ENRICHED_DIR.exists():
        return None
    candidates = []
    for d in ENRICHED_DIR.iterdir():
        if not d.is_dir(): 
            continue
        bd = d.name
        if not _DATE_RE.fullmatch(bd):
            continue
        cand = d / f"{SCRAPER_STEM}_{bd}.json"
        if cand.exists():
            candidates.append((bd, cand))
    if not candidates:
        return None
    candidates.sort(key=lambda t: t[0], reverse=True)  # YYYY_MM_DD sorts naturally
    return candidates[0]

def get_processed_artist_names(filepath: Path) -> set:
    if not filepath.exists():
        return set()
    names = set()
    with filepath.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                nm = rec.get("artist", "").strip().lower()
                if nm: names.add(nm)
            except json.JSONDecodeError as e:
                logger.error(f"{e}")
                continue
    return names

def enrich_artist(artist: dict) -> dict:
    name = artist.get("artist", "").strip()
    logger.info(f"\nPROCESSING: {name}")
    month_label = _last_complete_month_label()

    if USE_US_GATE and not passes_us_gate(name, US_GATE_MIN_PEAK):
        logger.info(f"US gate not passed for '{name}' (peak<{US_GATE_MIN_PEAK}). Skipping states.")
        artist["daily_trends_US_only"] = True
        return artist

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

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_ARTIST) as ex:
        futures = {ex.submit(_region_job, name, r, g): r for r, g in jobs}
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
    return artist

def enricher():
    latest = _latest_batch_with_scraper_file()
    if not latest:
        raise FileNotFoundError(
            "No valid batch found. Expecting data/raw/{YYYY_MM_DD}/spotify_rising_artists_{YYYY_MM_DD}.json"
        )
    batch_date, input_file = latest
    output_file = _make_output_jsonl_path(batch_date)

    logger.info(f"INPUT : {input_file.resolve()}")
    logger.info(f"OUTPUT: {output_file.resolve()}")

    with input_file.open("r", encoding="utf-8") as f:
        input_artists = json.load(f)

    processed_names = get_processed_artist_names(output_file)
    load_cache()

    saved_since_flush = 0
    try:
        with output_file.open("a", encoding="utf-8") as out:
            for artist in input_artists:
                if STOP_EVENT.is_set():
                    break
                name = artist.get("artist", "").strip()
                if not name:
                    continue
                if name.lower() in processed_names:
                    logger.info(f"Skipping already processed: {name}")
                    continue

                enriched = enrich_artist(artist)
                out.write(json.dumps(enriched) + "\n")
                logger.info(f"SAVED: {name}")

                saved_since_flush += 1
                if saved_since_flush >= SAVE_CACHE_EVERY_N_ARTISTS:
                    save_cache(); saved_since_flush = 0

                time.sleep(random.uniform(0.2, 0.8))
    finally:
        save_cache()
        logger.info(f"Data saved to {output_file.resolve()}")

    logger.info("Converting jsonl to csv…")
    convert_jsonl_to_csv(output_file, logger=logger)
    logger.info("Conversion complete.")

def main():
    enricher()


if __name__ == "__main__":
    main()
