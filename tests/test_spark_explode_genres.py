from datetime import date
from src.s2_transform.spark_azure_job import explode_genres


def test_explode_genres_splits_semicolon_and_normalizes_case(spark):
    rows = [
        {
            "artist": "Artist A",
            "id": "123",
            "genres": "Pop; Rock",
            "location": "NV",
            "date": date(2026, 2, 5),
            "trend_score": 50.0
        }
    ]

    df = spark.createDataFrame(rows)
    out_df, metrics = explode_genres(df)

    out = out_df.collect()
    genres = sorted([row["genres"] for row in out])

    assert len(out) == 2
    assert genres == ["pop", "rock"]
    assert metrics["genre_delim_replaced"] == 0
    assert metrics["dropped_empty_genre"] == 0


def test_explode_genres_replaces_pipe_and_comma_delimiters(spark):
    rows = [
        {
            "artist": "Artist A",
            "id": "123",
            "genres": "Pop|Rock,EDM",
            "location": "NV",
            "date": date(2026, 2, 5),
            "trend_score": 50.0
        }
    ]

    df = spark.createDataFrame(rows)
    out_df, metrics = explode_genres(df)

    out = out_df.collect()
    genres = sorted([row["genres"] for row in out])

    assert genres == ["edm", "pop", "rock"]
    assert metrics["genre_delim_replaced"] == 1


def test_explode_genres_parses_list_like_strings(spark):
    rows = [
        {
            "artist": "Artist A",
            "id": "123",
            "genres": "['Pop', 'Rock']",
            "location": "NV",
            "date": date(2026, 2, 5),
            "trend_score": 50.0
        }
    ]

    df = spark.createDataFrame(rows)
    out_df, _ = explode_genres(df)

    out = out_df.collect()
    genres = sorted([row["genres"] for row in out])

    assert genres == ["pop", "rock"]


def test_explode_genres_drops_empty_tokens(spark):
    rows = [
        {
            "artist": "Artist A",
            "id": "123",
            "genres": "Pop;;Rock;",
            "location": "NV",
            "date": date(2026, 2, 5),
            "trend_score": 50.0
        }
    ]

    df = spark.createDataFrame(rows)
    out_df, metrics = explode_genres(df)

    out = out_df.collect()
    genres = sorted([row["genres"] for row in out])

    assert genres == ["pop", "rock"]
    assert metrics["dropped_empty_genre"] >= 1