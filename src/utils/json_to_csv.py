"""
json_to_csv.py

Convert a JSON file containing Spotify artist metadata into a clean CSV format.

Usage:
- Called from artists_scraper.py with an explicit JSON path
- Or run standalone with a path argument

Input:
  spotify_rising_artists_{batch_date}.json

Output:
  spotify_rising_artists_{batch_date}.csv (same directory)
"""

import json
import pandas as pd
from pathlib import Path
import argparse
from typing import Union, Optional

from src.utils.logger_config import get_logger


def convert_json_to_csv(input_path: Union[str, Path], logger: Optional[object] = None) -> Path:
    """
    Convert a Spotify artist JSON file into a CSV.

    Args:
        input_path (str | Path): Path to the input JSON file.
        logger (logging.Logger, optional): Logger instance to use. 
                                           If None, creates a default logger.

    Returns:
        Path: Path to the generated CSV file.
    """
    if logger is None:
        logger = get_logger("Convert_JSON_to_CSV")

    input_path = Path(input_path)
    output_path = input_path.with_suffix(".csv")

    logger.info(f"Loading JSON file: {input_path}")
    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        logger.error(f"No data found in {input_path}")
        raise ValueError(f"No data found in {input_path}")

    logger.info(f"Converting {len(data)} records into DataFrame")
    df = pd.DataFrame(data)

    # Convert genre list to semicolon-separated string
    if "genres" in df.columns:
        df["genres"] = df["genres"].apply(lambda g: "; ".join(g) if isinstance(g, list) else "")
        logger.debug("Transformed 'genres' field into semicolon-separated strings")

    # Save as CSV
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"Saved {len(df)} artist records to {output_path.resolve()}")

    return output_path


def main():
    parser = argparse.ArgumentParser(description="Convert Spotify artist JSON to CSV.")
    parser.add_argument("input_json", help="Path to input JSON file")
    args = parser.parse_args()

    logger = get_logger("Convert_JSON_to_CSV_CLI")
    convert_json_to_csv(args.input_json, logger=logger)


if __name__ == "__main__":
    main()
