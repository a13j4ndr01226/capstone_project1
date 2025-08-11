"""
get_genre.py

"""
import time
import random
import requests
from src.utils.logger_config import logger
from src.utils.genre_cache import get_cached_genres, set_cached_genres

def get_artist_genres(artist_id, headers,max_retries=3):
    """
    Fetches genres for a given artist from Spotify API or cache.

    Args:
        artist_id (str): Spotify artist ID
        headers (dict): Auth headers
        max_retries (int): Max number of retry attempts on 429 errors

    Returns:
        list (str): List of genres associated with the artist
    """
    time.sleep(random.uniform(1.5, 2.5)) #rate limiting delay

    cached = get_cached_genres(artist_id) 
    if cached is not None: #If the artist ID exists in the cache, it returns the cached genres immediately and does not make an API call
        return cached

    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    
    #If the artist ID is not in the cache, the function makes a Spotify API request to GET
    for attempt in range(1, max_retries + 1):
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            genres = response.json().get("genres", [])
            set_cached_genres(artist_id, genres)
            return genres

        elif response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            logger.warning(f"Status Code 429. Rate limit hit for artist {artist_id}. Sleeping for {retry_after} seconds...")
            time.sleep(retry_after)
        else:
            logger.error(f"Failed to fetch genres for artist {artist_id}. "
                  f"STATUS: {response.status_code}, RESPONSE: {response.text}")
            break  # stop retrying on non-429 errors

    return []






