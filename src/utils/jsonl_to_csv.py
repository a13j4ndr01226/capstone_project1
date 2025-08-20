"""
jsonl_to_csv.py

Converts the enriched JSONL file (spotify_rising_with_trends_<BATCHDATE>.jsonl)
into a CSV with the required columns.

- Input:  data/stage2_trend_enrichment/spotify_rising_with_trends_<BATCHDATE>.jsonl
- Output: data/stage2_trend_enrichment/spotify_rising_with_trends_<BATCHDATE>.csv

Output columns:
  artist, id, genres, location, date, trend_score
"""

import pandas as pd
import json
import re
from datetime import datetime
from src.utils.logger_config import logger


def convert_jsonl_to_csv():
    batch_date = datetime.now().strftime('%Y_%m_%d')
    input_path = f"data/stage2_trend_enrichment/spotify_rising_with_trends_{batch_date}.jsonl"
    output_path = f"data/stage2_trend_enrichment/spotify_rising_with_trends_{batch_date}.csv"

    # Load JSONL
    data = []
    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))

    if not data:
        logger.warning(f"No data found in {input_path}")
        return

    # Normalize + melt
    df = pd.json_normalize(data)
    id_vars = ['artist', 'id', 'genres']
    trend_cols = [col for col in df.columns if col.startswith("daily_trends_")]
    melted = df.melt(id_vars=id_vars, value_vars=trend_cols,
                     var_name="location_date", value_name="trend_score")
    melted = melted.dropna(subset=["trend_score"])

    # Extract location and date
    def extract_location_date(value):
        match = re.match(r'daily_trends_([A-Za-z]+)[._](\d{4}-\d{2}-\d{2})', value)
        return (match.group(1).upper(), match.group(2)) if match else (None, None)

    melted[['location', 'date']] = melted['location_date'].apply(
        lambda x: pd.Series(extract_location_date(x))
    )
    melted['date'] = pd.to_datetime(melted['date'])

    # Keep required columns
    final_df = melted[['artist', 'id', 'genres', 'location', 'date', 'trend_score']]

    # Save CSV
    final_df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path} with shape: {final_df.shape}")


if __name__ == "__main__":
    convert_jsonl_to_csv()
