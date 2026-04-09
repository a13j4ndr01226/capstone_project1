from src.utils.dedup_artists import deduplicate_artists


def test_deduplicate_artists_removes_duplicate_id():
    artist_list = [
        {"artist": "Artist A", "id": "123", "genres": ["pop"]},
        {"artist": "Artist A", "id": "123", "genres": ["pop"]},
        {"artist": "Artist B", "id": "456", "genres": ["rock"]}
    ]

    result = deduplicate_artists(artist_list)
    assert len(result) == 2
    
    ids = {artist["id"] for artist in result}
    assert ids == {"123", "456"}


def test_deduplicate_artists_adds_scrape_date():
    artist_list = [
        {"artist": "Artist A", "id": "123", "genres": ["pop"]}
    ]

    result = deduplicate_artists(artist_list)

    assert "scrape_date" in result[0]
    assert isinstance(result[0]["scrape_date"], str)


def test_deduplicate_artists_defaults_missing_genres_to_empty_list():
    artist_list = [
        {"artist": "Artist A", "id": "123"}
    ]

    result = deduplicate_artists(artist_list)

    assert result[0]["genres"] == []