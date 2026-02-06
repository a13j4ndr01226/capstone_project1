
"""
jsonl_to_csv.py

Convert an enriched JSONL file (one JSON object per line) into a clean CSV.

- Accepts both flattened daily trend columns like:
    daily_trends_CA.2025-08-01 00:00:00
    daily_trends_NV_2025-08-01
  and nested dicts like:
    "daily_trends_CA": {"2025-08-01 00:00:00": 45, ...}

- Tolerates an optional time suffix after YYYY-MM-DD (space or 'T').
- Filters out non-date trend columns automatically.

Output columns: artist, id, genres, location, date, trend_score
"""
import argparse
import json
import re
from typing import Union, Optional
from pathlib import Path

import pandas as pd

try:
    from src.utils.logger_config import get_logger
except Exception:
    # Fallback logger if project logger isn't available in this context
    import logging
    def get_logger(name="jsonl_to_csv"):
        logger = logging.getLogger(name)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger


# Regex: location separator '.' or '_' + YYYY-MM-DD with optional time suffix
LOC_DATE_RE = re.compile(
    r"^daily_trends_([A-Za-z]+)[._](\d{4}-\d{2}-\d{2})(?:[ T]\d{2}:\d{2}:\d{2})?$"
)

def _safe_list_join(x):
    if isinstance(x, list):
        return "; ".join(map(str, x))
    return x if x is not None else ""

def convert_jsonl_to_csv(input_path: Union[str, Path], logger: Optional[object] = None) -> Path:
    """
    Normalize -> melt -> (expand nested dicts) -> extract (location, date) and write CSV next to the JSONL.
    Preserves the older script's tolerance for optional time suffixes in dates.
    """
    if logger is None:
        logger = get_logger("Convert_JSONL_to_CSV")

    input_path = Path(input_path)
    output_path = input_path.with_suffix(".csv")

    logger.info(f"Loading JSONL file: {input_path}")
    data = []
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                data.append(json.loads(line))

    # Always write a CSV (possibly empty with headers) to keep downstream steps stable
    if not data:
        logger.warning(f"No data found in {input_path}")
        pd.DataFrame(columns=["artist", "id", "genres", "location", "date", "trend_score"]).to_csv(
            output_path, index=False, encoding="utf-8"
        )
        logger.info(f"Saved empty CSV to {output_path}")
        return output_path

    # Normalize JSONL to a flat frame
    df = pd.json_normalize(data)

    # Ensure id vars exist
    id_vars = ["artist", "id", "genres"]
    for col in id_vars:
        if col not in df.columns:
            df[col] = None

    # Collect trend columns
    trend_cols = [c for c in df.columns if c.startswith("daily_trends_")]
    if not trend_cols:
        logger.warning("No 'daily_trends_*' columns found after normalization.")
        pd.DataFrame(columns=["artist", "id", "genres", "location", "date", "trend_score"]).to_csv(
            output_path, index=False, encoding="utf-8"
        )
        logger.info(f"Saved empty CSV to {output_path}")
        return output_path

    # Melt trend columns into long form
    melted = df.melt(
        id_vars=id_vars,
        value_vars=trend_cols,
        var_name="location_date",
        value_name="trend_score",
    ).dropna(subset=["trend_score"])

    # Separate rows where trend_score is a nested dict (needs expansion) vs scalar
    is_dict = melted["trend_score"].apply(lambda v: isinstance(v, dict))

    # Expand nested dicts: one row per date_key -> value
    if is_dict.any():
        dict_rows = melted[is_dict]
        expanded_rows = []
        for _, row in dict_rows.iterrows():
            locdate = str(row["location_date"])
            # Prefer the original separator if present; default to '.'
            sep = "." if "." in locdate else ("_" if "_" in locdate else ".")
            for date_key, value in row["trend_score"].items():
                new_row = row.copy()
                new_row["trend_score"] = value
                # Synthesize a fully qualified "location_date" with the date key
                new_row["__locdate_full"] = f"{locdate}{sep}{date_key}"
                expanded_rows.append(new_row)
        dict_expanded = pd.DataFrame(expanded_rows) if expanded_rows else pd.DataFrame(columns=melted.columns.tolist() + ["__locdate_full"])
    else:
        dict_expanded = pd.DataFrame(columns=melted.columns.tolist() + ["__locdate_full"])

    # Non-dict rows: keep as-is; they already have the full "location_date"
    non_dict_rows = melted[~is_dict].copy()
    if not non_dict_rows.empty:
        non_dict_rows["__locdate_full"] = non_dict_rows["location_date"].astype(str)

    # Combine
    melted2 = pd.concat([non_dict_rows, dict_expanded], ignore_index=True)

    # Filter to rows where "__locdate_full" matches our tolerant regex (drop oddball flags/columns)
    matched_mask = melted2["__locdate_full"].apply(lambda s: bool(LOC_DATE_RE.match(s)))
    filtered = melted2[matched_mask].copy()

    # Extract location and date
    def _extract(colname: str):
        m = LOC_DATE_RE.match(colname)
        return (m.group(1).upper(), m.group(2)) if m else (None, None)

    filtered[["location", "date"]] = filtered["__locdate_full"].apply(lambda s: pd.Series(_extract(s)))

    # Normalize genres to string
    if filtered["genres"].apply(lambda g: isinstance(g, list)).any():
        filtered["genres"] = filtered["genres"].apply(_safe_list_join)

    # Finalize types and columns
    filtered["date"] = pd.to_datetime(filtered["date"], errors="coerce")
    final_df = filtered[["artist", "id", "genres", "location", "date", "trend_score"]].copy()

    # Write CSV
    final_df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"Saved to {output_path} with shape: {final_df.shape}")

    return output_path


def main():
    parser = argparse.ArgumentParser(description="Convert JSONL (enriched artists) to CSV with tolerant date parsing.")
    parser.add_argument("input_jsonl", help="Path to input JSONL file")
    args = parser.parse_args()

    logger = get_logger("Convert_JSONL_to_CSV_CLI")
    convert_jsonl_to_csv(args.input_jsonl, logger=logger)


if __name__ == "__main__":
    main()
