"""
google_trends_scraper.py

Fetches and caches 1 year of daily Google Trends interest scores
for musical artists across specified regions. Avoids API rate-limiting
by chunking requests by calendar month and adding randomized throttling.

This module includes:
- get_trend_score: retrieves and caches daily interest scores for a specific time window
- get_trend_score_1y_loop: loops over 1 year, month-by-month, and merges all trend data
"""
import random
import time
import calendar
from datetime import datetime, timedelta
from pytrends.request import TrendReq
from src.utils.trends_cache import get_cached_score, set_cached_score

pytrends = TrendReq(hl='en-US', tz=480)  # Pacific time, timestamps irrelevant for daily data

from datetime import datetime, timedelta

def get_trend_score(artist_name, geo, month_year, timeframe_range, max_retries=2):
    """
    Fetches interest-over-time data for a given artist from Google Trends for a specific
    time window and geographic region. Uses caching to avoid redundant API requests.

    Args:
        artist_name (str): The artist name to query.
        geo (str): The region code (e.g., 'US-PA').
        month_year (str): A custom label used for caching (e.g., 'Jan_2025').
        timeframe_range (str): The timeframe string for the API (e.g., '2023-01-01 2023-01-31').
        max_retries (int, optional): Number of retry attempts on failure (default is 2).

    Returns:
        dict or None: Dictionary of daily scores {YYYY-MM-DD: interest_score}, or None if it fails.
    """
    cache_id = f"{artist_name}|{geo}|{month_year}" #unique identifier
    cached = get_cached_score(cache_id)                 #checks if it's already stored
    
    if cached is not None:
        return cached
    
    for attempt in range(1, max_retries + 1):
        try:
            pytrends.build_payload([artist_name], timeframe=timeframe_range, geo=geo)
            data = pytrends.interest_over_time()

            if not data.empty and artist_name in data.columns:
                trend_series = {
                    str(date): int(score)
                    for date, score in data[artist_name].dropna().items()
                }
                #Save to cache and sleep before returning
                set_cached_score(cache_id, trend_series)

                time.sleep(random.uniform(5, 10))
                return trend_series 

            else:
                trend_series = None

        except Exception as e:
                print(f"WARNING: Attempt {attempt}/{max_retries} failed for {artist_name} in {geo} ({month_year}): {e}")

                if "429" in str(e) and attempt < max_retries:
                    wait_time = 60 * attempt  # Exponential backoff
                    print(f"Sleeping for {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
                else:
                    print(f"Error: Giving up on {artist_name} in {geo} ({month_year}) after {attempt} attempts.")
                    return None

    return None

def get_trend_score_1y_loop(artist_name, geo):
    """
    Orchestrates chunked Google Trends API requests to collect 1 year of daily trend scores.
    Splits the request into calendar-month chunks to stay within API limits.

    Args:
        artist_name (str): The artist to query.
        geo (str): The geographic region code (e.g., 'US-NY').

    Returns:
        dict: A dictionary with keys as date strings (YYYY-MM-DD) and values as daily interest scores.
    """
    
    today = datetime.today()

    if today.month == 1:
        year = today.year - 1
        month = 12
    else:
        year = today.year
        month = today.month - 1
    
    last_day = calendar.monthrange(year, month)[1]
    end_date = datetime(year, month, last_day)
    start_date = end_date - timedelta(days=364)
    # chunk_size = 30  # Google allows up to ~270 days of daily granularity

    combined_scores = {}

    chunk_start = start_date
    chunk_index = 0

    while chunk_start <= end_date:
        chunk_last_day = calendar.monthrange(chunk_start.year, chunk_start.month)[1]
        chunk_end = min(datetime(chunk_start.year,chunk_start.month,chunk_last_day), end_date)
        # chunk_end = min(chunk_start + timedelta(days=chunk_size), end_date) 
        label = chunk_start.strftime('%b_%Y')
        tf_range = f"{chunk_start.strftime('%Y-%m-%d')} {chunk_end.strftime('%Y-%m-%d')}" #this exact range is used in pytrends

        print(f"{geo} Chunk {chunk_index + 1}: {tf_range}")

        chunk_scores = get_trend_score(artist_name, geo, label, tf_range)
        
        if chunk_scores:
            combined_scores.update(chunk_scores)

        chunk_start = chunk_end + timedelta(days=1)
        chunk_index += 1
        time.sleep(random.uniform(10, 25))

    return combined_scores


