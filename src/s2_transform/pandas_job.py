"""
Automated TRANSFORM:

1) Auto-detect latest spotify_rising_with_trends_YYYY_MM_DD.csv
2) Clean & standardize rows (**EXPLODED by genre**)
3) Write a single cleaned CSV to data/transformed/{YYYY_MM_DD}/:
      spotify_rising_cleaned_{YYYY_MM_DD}.csv
4) Persist dimensions by calling the reusable module **USING THE CLEANED FILE**:
      dim_artists.csv, dim_genres.csv, dim_locations.csv
"""
import os
import re
import ast
import csv
from pathlib import Path
import pandas as pd
from src.utils.logger_config import get_logger
from src.s2_transform.dim_persist import persist_dimensions_for_input
from src.utils.find_latest_file import find_latest_raw_nested
from src.utils.confirm_dir_exists import ensure_dir


logger = get_logger("Transform")

# ---------- local helpers ----------
def _std(x):
    if pd.isna(x):
        return None
    return str(x).strip()

def standardize_string(x):
    return _std(x)

def standardize_state(x):
    s = _std(x)
    return s.upper() if s is not None else None

def clean_and_validate_chunk(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Returns:
        (clean_df, metrics_dict)
    metrics_dict keys:
      rows_in, bad_dates, score_out_of_range, dropped_missing_id_loc_date
    """
    metrics = {"rows_in": len(df), "bad_dates": 0, "score_out_of_range": 0, "dropped_missing_id_loc_date": 0}

    df["artist"]   = df["artist"].map(standardize_string)
    df["id"]       = df["id"].map(standardize_string)
    df["location"] = df["location"].map(standardize_state)
    df["genres"]   = df["genres"].map(standardize_string)

    # Many of our inputs carry dates like 'YYYY_MM_DD' → normalize underscores to hyphens first.
    date_str = df["date"].astype(str).str.replace("_", "-", regex=False).str.strip()
    parsed = pd.to_datetime(date_str, errors="coerce")  # let pandas infer
    metrics["bad_dates"] = int(parsed.isna().sum())
    df["date"] = parsed.dt.date

    # trend_score range metrics
    ts = pd.to_numeric(df["trend_score"], errors="coerce")
    out_of_range = (ts < 0.0) | (ts > 100.0)
    metrics["score_out_of_range"] = int(out_of_range.fillna(True).sum())
    ts = ts.clip(lower=0.0, upper=100.0).fillna(0.0)
    df["trend_score"] = ts

    # drop NA id/location/date
    before = len(df)
    df = df[df["id"].notna() & df["location"].notna() & df["date"].notna()]
    metrics["dropped_missing_id_loc_date"] = int(before - len(df))

    df["genres"] = df["genres"].fillna("Unknown")

    for col in ["artist", "id", "genres", "location"]:
        df[col] = df[col].astype(str).str.strip()

    return df, metrics

# ---------- runner ----------
def transform():
    RAW_ROOT = Path("data/raw")
    latest, batch_date = find_latest_raw_nested(RAW_ROOT, expected_template="spotify_rising_with_trends_{date}.csv", logger=logger)

    # --- ONE-OFF OVERRIDE (optional) ---
    # If TRANSFORM_ONE_OFF_INPUT is set, use that file instead of auto-detecting.
    override = os.getenv("TRANSFORM_ONE_OFF_INPUT")
    if override:
        override_path = Path(override)
        if not override_path.exists():
            raise FileNotFoundError(f"Override file not found: {override_path}")
        latest = override_path
        # derive batch_date from filename: spotify_rising_with_trends_YYYY_MM_DD.csv
        m = re.search(r"spotify_rising_with_trends_(\d{4}_\d{2}_\d{2})\.csv$", override_path.name, flags=re.I)
        if not m:
            raise ValueError(f"Cannot extract batch_date from filename: {override_path.name}")
        batch_date = m.group(1)
        logger.info(f"ONE-OFF override active. Using: {latest} (batch_date={batch_date})")
    # --- end override ---

    if latest is None:
        raise FileNotFoundError(f"No raw files found under {RAW_ROOT}/{{YYYY_MM_DD}}/")

    # --- NEW: batch-dated output directory ---
    outdir = Path("data/transformed") / batch_date
    ensure_dir(outdir, logger=logger)

    cleaned_out = outdir / f"spotify_rising_cleaned_{batch_date}.csv"

    with open(cleaned_out, "w", encoding="utf-8") as f:
        f.write("artist,id,genres,location,date,trend_score\n")
    logger.info(f"Initialized (overwrote) cleaned file: {cleaned_out}")

    rows_seen = 0
    rows_written = 0
    total_metrics = {"rows_in": 0, "bad_dates": 0, "score_out_of_range": 0, "dropped_missing_id_loc_date": 0,
                     "genre_delim_replaced": 0, "rows_after_explode": 0}

    for i, chunk in enumerate(pd.read_csv(
        latest,
        chunksize=500_000,
        dtype={"artist": "string", "id": "string", "genres": "string", "location": "string"},
        low_memory=True
    ), start=1):
        rows_seen += len(chunk)
        logger.info(f"[Chunk {i}] Starting clean (rows={len(chunk):,})")

        # 1) base cleaning + metrics
        df, m = clean_and_validate_chunk(chunk)
        for k, v in m.items():
            total_metrics[k] += v

        if df.empty:
            logger.info(f"[Chunk {i}] Empty after base cleaning, skipping.")
            continue

        # 2) robust genre explode with metrics
        # normalize alternate delimiters to ';'
        before_norm = df["genres"].astype(str)
        after_norm = before_norm.str.replace(r"[|,]", ";", regex=True).str.strip()
        replaced = int((before_norm != after_norm).sum())
        total_metrics["genre_delim_replaced"] += replaced
        df["genres"] = after_norm

        def _parse_genres(cell: str):
            """
            Accepts:
              - "['midwest emo', 'emo']"
              - '["indie pop"]'
              - 'midwest emo; emo'
              - 'midwest emo'
            Returns: list[str]
            """
            s = (cell or "").strip()
            # Try to parse Python/JSON-like list first (handles: ['midwest emo'])
            if s.startswith("[") and s.endswith("]"):
                try:
                    parsed = ast.literal_eval(s)
                    if isinstance(parsed, (list, tuple)):
                        return [str(x) for x in parsed]
                except Exception:
                    pass  # fall through to delimiter split

            # Fallback: split on ';' (already normalized) or treat as single value
            return [p for p in s.split(";")] if ";" in s else [s]

        df["genres"] = df["genres"].map(_parse_genres)
        df = df.explode("genres", ignore_index=True)

        # Final token clean
        df["genres"] = (
            df["genres"]
            .astype(str)
            .str.replace(r"^[\s\"'\[\]\(\)]+|[\s\"'\[\]\(\)]+$", "", regex=True)
            .str.strip()
            .str.lower()
        )

        # Drop empties (keep "unknown")
        before_drop = len(df)
        df = df[df["genres"].ne("")]
        total_metrics["rows_after_explode"] += len(df)
        dropped_empty_genre = before_drop - len(df)

        if dropped_empty_genre:
            logger.info(f"[Chunk {i}] Dropped {dropped_empty_genre:,} rows with empty genre after explode")

        if df.empty:
            logger.info(f"[Chunk {i}] Empty after genre explode, skipping.")
            continue

        # 3) append to cleaned output
        df.to_csv(cleaned_out, mode="a", header=False, index=False, quoting=csv.QUOTE_MINIMAL)
        rows_written += len(df)
        logger.info(f"[Chunk {i}] Wrote {len(df):,} cleaned rows")

    logger.info(
        "Transform summary: "
        f"seen={rows_seen:,}, written={rows_written:,}, "
        f"bad_dates={total_metrics['bad_dates']:,}, "
        f"score_out_of_range={total_metrics['score_out_of_range']:,}, "
        f"dropped_missing_id_loc_date={total_metrics['dropped_missing_id_loc_date']:,}, "
        f"genre_delim_replaced={total_metrics['genre_delim_replaced']:,}, "
        f"rows_after_explode={total_metrics['rows_after_explode']:,}"
    )
    logger.info(f"Wrote cleaned: {cleaned_out.resolve()}")

    # 4) Persist dims FROM THE CLEANED FILE 
    logger.info("Starting dimension persistence from cleaned output...")
    persist_dimensions_for_input(cleaned_out)
    logger.info("Transform complete. Dims persisted from cleaned output.")
    

if __name__ == "__main__":
    transform()
