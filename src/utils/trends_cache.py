"""
trend_cache.py

This module reduces redundant API calls by saving previously fetched trend scores
in a local JSON file (`data/cache/trend_score_cache.json`).

Functions:
- load_cache : Load the cache from disk into memory.
- save_cache : Save the in-memory cache to disk.
- get_cached_score : Retrieve a cached score by cache_id.
- set_cached_score : Store a new score under a given cache_id.
"""

import json
from pathlib import Path

CACHE_FILE = Path("data/cache/trend_score_cache.json") #economy
trend_cache = {}

def load_cache():
    """
    Loads trend scores from the cache file into memory.
    If the cache file exists, its contents are read and stored
    in the global `trend_cache` dictionary.
    """
    global trend_cache
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            trend_cache = json.load(f)

def save_cache():
    """
    Saves the current contents.
    Ensures the cache directory exists before writing. Results are
    written in indented JSON format for readability.
    """
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(trend_cache, f, indent=2)

def get_cached_score(cache_id):
    """
    Retrieves a cached trend score for a given cache_id.

    Parameters
    cache_id : str
        Unique identifier combining artist name, region, month and year.

    Returns
    dict or None
        Cached trend data if found, otherwise None.
    """
    return trend_cache.get(cache_id)

def set_cached_score(cache_id, score):
    """
    Stores a trend score in the cache under the specified key.

    Parameters
    key : str
        Unique cache key
    score : dict
        Dictionary of daily scores 
    """
    trend_cache[cache_id] = score
