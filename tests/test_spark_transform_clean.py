from datetime import date
from src.s2_transform.spark_azure_job import clean_and_validate


def test_clean_and_validate(spark):
    rows = [
        {
            "artist": "  Artist A ",
            "id": " 123 ",
            "genres": None,
            "location": " nv ",
            "date": "2026_02_05",
            "trend_score": "150"
        },
        {
            "artist": "Artist B",
            "id": "456",
            "genres": "Pop",
            "location": "ca",
            "date": "bad-date",
            "trend_score": None
        }
    ]

    df = spark.createDataFrame(rows)

    out_df, metrics = clean_and_validate(df)
    out = out_df.collect()

    assert len(out) == 1

    row = out[0]
    assert row["artist"] == "Artist A" # Checks for string trimming
    assert row["id"] == "123" 
    assert row["genres"] == "Unknown" # Flags nulls
    assert row["location"] == "NV" # Upper case state 
    assert row["date"] == date(2026, 2, 5) # date parsing
    assert row["trend_score"] == 100.0 # Score max out at 100

    assert metrics["rows_in"] == 2 # Row count
    assert metrics["bad_dates"] == 1 # Bad date count
    assert metrics["score_out_of_range"] == 2 # Null scores count
    assert metrics["dropped_missing_id_loc_date"] == 1 # Dropped row count