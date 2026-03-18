from datetime import datetime

def deduplicate_artists(artist_list):
    """
    Deduplicates a list of artist entries and aggregates all locations
    where each artist was found.

    Parameters
    ----------
    artist_list : list of dict
        List of artist entries from scraped playlists.
        Each entry must include:
        - 'artist': str (artist name)
        - 'id': str (Spotify artist ID)
        - 'genres: list of str

    Returns
    -------
    list of dict
        Deduplicated list of artist entries.
        Each dictionary includes:
        - 'artist': str (artist name)
        - 'id': str (artist ID)
        - 'genres': list of str
    """
    unique_artists = {}

    #adds timestamp for tracking purposes
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 

    for entry in artist_list:
        aid = entry["id"]
        unique_artists[aid] = {
            "artist": entry["artist"],
            "id": aid,
            "genres": entry.get("genres", []),
            "scrape_date": timestamp
        }

    return list(unique_artists.values())
