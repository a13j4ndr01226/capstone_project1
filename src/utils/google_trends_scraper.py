"""
google_trends_scraper.py

Fast, safe Google Trends fetching with:
- cache-first lookups
- a global rate limiter (shared across threads)
- retries with exponential backoff + jitter
- helpers for last-complete-month and 14d windows
"""
import random
import time
import calendar
import threading
from datetime import date, datetime, timedelta
from pytrends.request import TrendReq
from src.utils.logger_config import get_logger
from src.utils.trends_cache import get_cached_score, set_cached_score

# ------------------ Tuning Defaults ------------------
MIN_INTERVAL_BETWEEN_CALL = 10.0  # seconds between any two pytrends calls
MAX_RETRIES = 2                  # max retries per request
INITIAL_BACKOFF = 90             # seconds when 429 is encountered

# ------------------ Global rate limiter ------------------
__rate_lock = threading.Lock()
__last_call_ts = 0.0

__penalty_lock = threading.Lock()
__penalty_until = 0.0         # monotonic time until which we must pause
__penalty_seconds = 300.0     # 5 minutes global cool-down on first 429


STOP_EVENT: threading.Event | None = None  # will be installed by artists_enricher
logger = get_logger("Extract_Artist_Enricher")

def install_stop_event(ev: threading.Event):
    """Call this once from artists_enricher so this module can see the same flag."""
    global STOP_EVENT
    STOP_EVENT = ev

def _sleep_with_cancel(seconds: float) -> bool:
    """Sleep up to `seconds`, but wake early if STOP_EVENT is set. Returns False if cancelled."""
    if not STOP_EVENT:
        time.sleep(seconds)
        return True
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if STOP_EVENT.is_set():
            return False
        time.sleep(0.25)  # short tick so Ctrl+C exits quickly
    return True

def _norm_key(s: str) -> str:
    return " ".join(s.strip().lower().split())  # collapse whitespace; lower

def _throttled_build_payload(pytrends, kw_list, timeframe, geo):
    """Ensure a minimum global interval between calls across all threads."""
    global __last_call_ts, __penalty_until

    # bail out fast if user requested stop
    if STOP_EVENT and STOP_EVENT.is_set():
        return None

    with __rate_lock:
        now = time.monotonic()

        # --- penalty window (circuit breaker) ---
        if now < __penalty_until:
            remaining = __penalty_until - now
            if not _sleep_with_cancel(remaining):
                return None

        # --- inter-call spacing ---
        wait = MIN_INTERVAL_BETWEEN_CALL - (now - __last_call_ts)
        if wait > 0:
            if not _sleep_with_cancel(wait):
                return None

        __last_call_ts = time.monotonic()

    # one last check right before the API call
    if STOP_EVENT and STOP_EVENT.is_set():
        return None

    pytrends.build_payload(kw_list, timeframe=timeframe, geo=geo)
    return True

def get_trend_score(artist_name: str, geo: str, month_year: str, timeframe_range: str,
                    max_retries: int = MAX_RETRIES):
    """
    Fetch interest-over-time for a single artist/region/timeframe.
    Cache key: "{artist}|{geo}|{month_year}"
    Returns: dict[YYYY-MM-DD] -> int, or None
    """
    if STOP_EVENT and STOP_EVENT.is_set():
        return None

    cache_id = f"{_norm_key(artist_name)}|{geo}|{month_year}"
    cached = get_cached_score(cache_id)

    if cached is not None:
        return cached

    for attempt in range(1, max_retries + 1):
        if STOP_EVENT and STOP_EVENT.is_set():
            return None
        try:
            pytrends = TrendReq(hl='en-US', tz=480)
            ok = _throttled_build_payload(pytrends, [artist_name], timeframe=timeframe_range, geo=geo)
            if ok is None:
                return None
            data = pytrends.interest_over_time()

            if not data.empty and artist_name in data.columns:
                trend_series = {str(d): int(v) for d, v in data[artist_name].dropna().items()}
                set_cached_score(cache_id, trend_series)
                # tiny jitter so threads don’t align
                if not _sleep_with_cancel(random.uniform(0.2, 0.6)):
                    return None
                return trend_series
            else:
                return None
        except Exception as e:
            logger.warning(
                f"Attempt {attempt}/{max_retries} failed for {artist_name} in {geo} ({month_year}): {e}"
            )

            # --- circuit breaker on 429 ---
            if "429" in str(e):
                # activate global cool-down so ALL threads pause
                global __penalty_until
                with __penalty_lock:
                    __penalty_until = max(__penalty_until, time.monotonic() + __penalty_seconds)

                if attempt < max_retries:
                    # short yield; the real wait is enforced in _throttled_build_payload via __penalty_until
                    if not _sleep_with_cancel(2.0):
                        return None
                    continue
                return None

            # non-429 errors: optional backoff or just bail
            if attempt < max_retries:
                if not _sleep_with_cancel(1.0):
                    return None
                continue
            return None

    return None
            
def get_trend_score_last_complete_month(artist_name: str, geo: str):
    """
    Fetch daily Google Trends scores for the last COMPLETE calendar month.
    e.g., if today is Aug 15, 2025 -> fetch Jul 1–31, 2025
    """
    today = date.today()
    if today.month == 1:
        year, month = today.year - 1, 12
    else:
        year, month = today.year, today.month - 1

    start_date = date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = date(year, month, last_day)

    label = start_date.strftime('%b_%Y')  # e.g., "Jul_2025"
    timeframe = f"{start_date:%Y-%m-%d} {end_date:%Y-%m-%d}"

    return get_trend_score(
        artist_name=artist_name,
        geo=geo,
        month_year=label,
        timeframe_range=timeframe,
    )

def get_trend_score_14d(name: str, geo: str):
    """
    Fetch 14 days of daily Google Trends scores ending yesterday for a given artist/region.
    """
    end_date = datetime.today().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=13)
    label = f"last14d_end_{end_date:%Y-%m-%d}"
    timeframe = f"{start_date:%Y-%m-%d} {end_date:%Y-%m-%d}"

    return get_trend_score(
        artist_name=name,
        geo=geo,
        month_year=label,
        timeframe_range=timeframe,
    )
