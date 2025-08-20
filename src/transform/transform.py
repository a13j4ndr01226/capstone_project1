"""
transform.py 

Automated TRANSFORM (no user input):

1) Auto-detect latest stage2 input in data/stage2_trend_enrichment/
   file pattern: spotify_rising_with_trends_YYYY_MM_DD.csv
2) Clean & standardize rows (UNEXPLODED)
3) Write a single cleaned CSV to data/transformed/:
      spotify_rising_cleaned_{YYYY_MM_DD}.csv
4) Persist dimensions by calling the reusable module:
      dim_artists.csv, dim_genres.csv, dim_locations.csv

"""

import re
import csv
from pathlib import Path
import pandas as pd
from src.utils.logger_config import logger
from src.transform.dim_persist import persist_dims

# ---------- local helpers (keeps this script self-sufficient) ----------
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _std(x):
    if pd.isna(x):
        return None
    return str(x).strip()

def standardize_string(x):
    return _std(x)

def standardize_genre(x):
    s = _std(x)
    return s.lower() if s is not None else None

def standardize_state(x):
    s = _std(x)
    return s.upper() if s is not None else None

def clean_and_validate_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df["artist"] = df["artist"].map(standardize_string)
    df["id"] = df["id"].map(standardize_string)
    df["location"] = df["location"].map(standardize_state)
    df["genres"] = df["genres"].map(standardize_string)

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["trend_score"] = pd.to_numeric(df["trend_score"], errors="coerce").fillna(0.0)
    df = df[(df["trend_score"] >= 0.0) & (df["trend_score"] <= 100.0)]

    df = df[df["id"].notna() & df["location"].notna() & df["date"].notna()]
    df["genres"] = df["genres"].fillna("Unknown")

    for col in ["artist", "id", "genres", "location"]:
        df[col] = df[col].astype(str).str.strip()

    return df

def find_latest_stage2_file(root: Path) -> Path | None:
    """Find the latest spotify_rising_with_trends_{YYYY_MM_DD}.csv by date in filename."""
    pat = re.compile(r"spotify_rising_with_trends_(\d{4}_\d{2}_\d{2})\.csv$", re.I)
    if not root.exists():
        return None
    candidates = []
    for p in root.iterdir():
        if p.is_file():
            m = pat.match(p.name)
            if m:
                candidates.append((m.group(1), p))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])  # YYYY_MM_DD sorts correctly
    return candidates[-1][1]

def extract_batch_date(path: Path) -> str:
    m = re.search(r"spotify_rising_with_trends_(\d{4}_\d{2}_\d{2})\.csv$", path.name, flags=re.I)
    return m.group(1) if m else "unknown"

# ---------- runner ----------
def run():
    stage2_dir = Path("data/stage2_trend_enrichment")
    latest = find_latest_stage2_file(stage2_dir)
    if latest is None:
        raise FileNotFoundError(f"No stage2 files found in {stage2_dir}")

    batch_date = extract_batch_date(latest)
    outdir = Path("data/transformed")
    ensure_dir(outdir)

    cleaned_out = outdir / f"spotify_rising_cleaned_{batch_date}.csv"
    # initialize header if missing (non-destructive)
    if not cleaned_out.exists():
        cleaned_out.write_text("artist,id,genres,location,date,trend_score\n", encoding="utf-8")

    total_rows = 0
    for i, chunk in enumerate(pd.read_csv(
        latest,
        chunksize=500_000,
        dtype={"artist": "string", "id": "string", "genres": "string", "location": "string"},
        low_memory=True
    ), start=1):
        total_rows += len(chunk)
        logger.info(f"Cleaning chunk {i:,} (rows={len(chunk):,})")
        df = clean_and_validate_chunk(chunk)
        if df.empty:
            continue
        df.to_csv(cleaned_out, mode="a", header=False, index=False, quoting=csv.QUOTE_MINIMAL)

    logger.info(f"Wrote cleaned: {cleaned_out.resolve()} ({total_rows:,} rows)")

    # Persist dims for future runs using the same input file
    persist_dims()

    logger.info("Transform complete. Dims persisted.")

if __name__ == "__main__":
    run()