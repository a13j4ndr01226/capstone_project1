"""
spotify_rising_artists.py

Contains functions to search for specific "on the rise" Spotify created playlists,
extract artist data from those playlists, and deduplicate artists across playlists.

"""

import requests
import time
from datetime import datetime
from src.utils.logger_config import logger
from src.utils.get_genre import get_artist_genres
from src.utils.auth import get_auth_headers
from src.utils.dedup_artists import deduplicate_artists
from src.utils.genre_cache import get_cached_genres, set_cached_genres

def scrape_spotify_created_playlists(playlist_id, playlist_name, headers, max_retries=3):
    """
    Extracts artist IDs and names from a spotify curated playlist (no genres)

    Args:
        playlist_id (str): The Spotify playlist ID
        playlist_name (str): Playlist name for tagging
        headers (dict): Authorization headers for the Spotify API
        max_retries (int): Max number of retry attempts on 429 errors

    Returns:
        list of dict: Each dict includes artist name, ID, and playlist source
    """
    base_url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    limit = 100
    offset = 0 
    all_items = []    
    retries = 0

    while True:
        url = f"{base_url}?limit={limit}&offset={offset}"
        
        while retries <= max_retries:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                break
            
            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logger.error(f"Status Code 429. Sleeping for {retry_after} seconds...")
                time.sleep(retry_after)
                retries += 1
            
            else:
                raise logger.error(
                    f"Failed to fetch playlist {playlist_name}."
                    f"Status: {response.status_code}. Response: {response.text}"
                )
        
        else:
            raise logger.error(f"ERROR: Exceeds max retries on playlist {playlist_name} due to repeated 429s.")
        
        data = response.json()
        items = data.get("items",[])
        all_items.extend(items)

        if data.get("next"):
            offset += limit
        else:
            break

    seen = set()
    artists = []
    #adds timestamp for tracking purposes
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 

    for item in all_items:
        try:
            artist = item['track']['artists'][0] #Pulls the first listed artist
            artist_id = artist["id"]
            if artist_id in seen:
                continue

            seen.add(artist_id)

            artists.append({
                "artist": artist["name"],
                "id": artist["id"],
                "scrape_date": timestamp
            })

        except (TypeError, KeyError): #Skip over any tracks that are missing artist info or are formatted oddly
            continue
    
    logger.info(f"Tracks pulled from '{playlist_name}': {len(all_items)}")

    return artists

def artist_by_playlistIDs(playlist_dict):
    """
    Pulls artists directly from known Spotify playlist IDs. 

    Args:
        playlist_dict (dict): Dict of {playlist_name: playlist_id}

    Returns:
        list of dict: Flattened list of all artists found, tagged with source playlist.
    """
    # This function calls the access token using Client Credentials Flow
    headers = get_auth_headers() 

    all_artists = []

    for playlist_name, playlist_id in playlist_dict.items():
        logger.info(f"Scraping '{playlist_name}' (ID: {playlist_id})")
        
        try:
            artists = scrape_spotify_created_playlists(playlist_id, playlist_name, headers)
            logger.info(f"Found {len(artists)} artists in '{playlist_name}'")
            
            all_artists.extend(artists)
        
        except Exception as e:
            logger.error(f"Failed to scrape '{playlist_name}': {e}")
        
        time.sleep(1)

    logger.info(f"Total collected before deduplication: {len(all_artists)}")

    deduped_artists = deduplicate_artists(all_artists)
    logger.info(f"Total after deduplication: {len(deduped_artists)}")

    #Add genres using cache-first approach 
    cache_hits = 0 
    cache_misses = 0
    enriched = []

    for a in deduped_artists:
        artist_id = a["id"]
        genres = get_cached_genres(artist_id)
        
        if genres is not None:
            cache_hits += 1
        
        else:
            genres = get_artist_genres(artist_id, headers)
            set_cached_genres(artist_id, genres)
            cache_misses += 1

        enriched.append({**a, "genres": genres})

    logger.info(f"Artists already in genre cache: {cache_hits}")
    logger.info(f"Artists requiring API calls: {cache_misses}")

    return enriched