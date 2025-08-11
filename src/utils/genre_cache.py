"""
genre_cache.py

Stores genre data associated with Spotify artist IDs. 
This helps reduce redundant API calls
"""

import json
from pathlib import Path

CACHE_DIR = Path("data/cache")
CACHE_FILE = CACHE_DIR / "artist_genre_cache.json"

# In-memory cache
genre_cache = {}

def load_cache():
    """
    Loads the artist genre cache from disk into memory.
    """
    global genre_cache
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            genre_cache = json.load(f)
    else:
        genre_cache = {}

def save_cache():
    """
    Saves the in-memory genre cache to disk.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(genre_cache, f, indent=2)

def get_cached_genres(artist_id):
    """
    Returns cached genres for the given artist_id if available.

    Args:
        artist_id (str): Spotify artist ID.

    Returns:
        list or None: Genre list or None if not cached.
    """
    return genre_cache.get(artist_id)

def set_cached_genres(artist_id, genres):
    """
    Stores genres in cache for a given artist_id.

    Args:
        artist_id (str): Spotify artist ID.
        genres (list): Genre list to cache.
    """
    genre_cache[artist_id] = genres
