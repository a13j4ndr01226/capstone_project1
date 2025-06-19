import time
from src.utils.get_genre import get_artist_genres

def add_genres(unique_artists_dict, headers):
    """
    Enriches each unique artist with genre data from the Spotify API.
    Uses caching to avoid duplicate API calls and includes throttling
    to prevent hitting rate limits.

    Parameters
    ----------
    unique_artists_dict : dict
        Dictionary where keys are artist IDs and values are:
        {
            'artist': str,
            'id': str,
            'locations': set of str
        }

    headers : dict
        Dictionary of Spotify API headers. Must include:
        - 'Authorization': Bearer token

    Returns
    -------
    list of dict
        Each enriched artist record contains:
        - 'artist': str
        - 'id': str
        - 'genres': list of str
        - 'locations': list of str
    """
    enriched = []
    genre_cache = {}

    for artist_id, info in unique_artists_dict.items():
        if artist_id in genre_cache:
            genres = genre_cache[artist_id]
        else:
            genres = get_artist_genres(artist_id, headers)
            genre_cache[artist_id] = genres

        enriched.append({
            "artist": info["artist"],
            "id": artist_id,
            "genres": genres,
            "locations": list(info["locations"])
        })

        time.sleep(0.2)  # Light delay to prevent hitting API limits

    return enriched
