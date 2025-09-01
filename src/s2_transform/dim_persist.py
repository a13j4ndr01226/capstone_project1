"""
dim_persist.py

Reusable, lightweight persistence module for dimension CSV caches (artists, genres, locations).

Usage:
    from src.s2_transform.dim_persist import persist_dimensions_for_input

    # From transform pipeline
    persist_dimensions_for_input("data/transformed/spotify_rising_cleaned_2025_08_27.csv")

    # Standalone (auto-detects latest RAW batch, then targets matching CLEANED file)
    python -m src.s2_transform.dim_persist
"""

from __future__ import annotations
import re
import csv
from pathlib import Path
from typing import Dict, Optional
import pandas as pd
import numpy as np
from src.utils.logger_config import get_logger
from src.utils.find_latest_file import find_latest_raw_nested 

TRANSFORMED_DIR = Path("data/transformed")
RAW_ROOT       = Path("data/raw")
PERSISTED_DIR  = Path("data/persisted_dims")
logger = get_logger("Transform_Dim_Persistence")

# ---------- helpers ----------

def ensure_dir(p: Path) -> None:
    """Ensure directory `p` exists; create recursively if missing."""
    if p.exists():
        logger.info(f"Folder exists: {p.resolve()}")
    else:
        p.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created folder: {p.resolve()}")

def init_csv_if_missing(path: Path, headers):
    """Create CSV with header if missing. Non-destructive if already exists."""
    ensure_dir(path.parent)
    if not path.exists():
        path.write_text(",".join(headers) + "\n", encoding="utf-8")
        logger.info(f"Initialized dim CSV with header: {path.resolve()}")
    else:
        logger.info(f"Dim CSV present (will append if new rows): {path.resolve()}")

def _std(s: Optional[str]) -> Optional[str]:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    return str(s).strip()

def std_name(x):  # artist
    return _std(x)

def std_spotify_id(x):
    return _std(x)

def std_state(x):
    s = _std(x)
    return s.upper() if s else None

def std_genres_string(x):
    # Cleaned file should already be single token, but normalize defensively
    s = _std(x)
    if s is None:
        return None
    return re.sub(r"[|,]", ";", s)

def std_genre(x):
    s = _std(x)
    return s.lower() if s else None

# ---------- in-memory keyspace ----------

class KeySpace:
    """In-memory surrogate key manager for artists, genres, and locations."""
    def __init__(self):
        self.artist_key: Dict[str, int] = {}
        self.artist_label: Dict[int, str] = {}
        self.genre_key: Dict[str, int] = {}
        self.location_key: Dict[str, int] = {}
        self._artist_seq = 0
        self._genre_seq = 0
        self._location_seq = 0

    def _next_artist_id(self) -> int:
        self._artist_seq += 1
        return self._artist_seq

    def _next_genre_id(self) -> int:
        self._genre_seq += 1
        return self._genre_seq

    def _next_location_id(self) -> int:
        self._location_seq += 1
        return self._location_seq

    def intern_artist(self, spotify_id: str, artist_name: Optional[str]) -> int:
        if spotify_id in self.artist_key:
            a_id = self.artist_key[spotify_id]
            if artist_name:
                self.artist_label[a_id] = artist_name
            return a_id
        a_id = self._next_artist_id()
        self.artist_key[spotify_id] = a_id
        self.artist_label[a_id] = artist_name or ""
        return a_id

    def intern_genre(self, genre: str) -> int:
        if genre in self.genre_key:
            return self.genre_key[genre]
        g_id = self._next_genre_id()
        self.genre_key[genre] = g_id
        return g_id

    def intern_location(self, state_code: str) -> int:
        if state_code in self.location_key:
            return self.location_key[state_code]
        l_id = self._next_location_id()
        self.location_key[state_code] = l_id
        return l_id

# ---------- dim loaders ----------

def load_dim_artists(path: Path, keys: KeySpace) -> int:
    """Load existing artists into KeySpace and return max artist_id."""
    if not path.exists():
        logger.info(f"No existing dim_artists.csv (will start fresh): {path.resolve()}")
        return 0
    df = pd.read_csv(path, dtype={"artist_id": "Int64", "spotify_id": "string", "artist_name": "string"})
    before = len(df)
    df = df.dropna(subset=["artist_id", "spotify_id"])
    dropped = before - len(df)
    if dropped:
        logger.warning(f"dim_artists.csv: dropped {dropped:,} invalid rows (missing id/spotify_id)")
    for _, row in df.iterrows():
        aid = int(row["artist_id"])
        sid = _std(row["spotify_id"])
        nm  = _std(row.get("artist_name"))
        if not sid:
            continue
        keys.artist_key[sid] = aid
        keys.artist_label[aid] = nm or ""
        if aid > keys._artist_seq:
            keys._artist_seq = aid
    logger.info(f"Loaded existing artists: {len(df):,}; max artist_id: {keys._artist_seq}")
    return int(keys._artist_seq)

def load_dim_genres(path: Path, keys: KeySpace) -> int:
    """Load existing genres into KeySpace and return max genre_id."""
    if not path.exists():
        logger.info(f"No existing dim_genres.csv (will start fresh): {path.resolve()}")
        return 0
    df = pd.read_csv(path, dtype={"genre_id": "Int64", "genre": "string"})
    before = len(df)
    df = df.dropna(subset=["genre_id", "genre"])
    dropped = before - len(df)
    if dropped:
        logger.warning(f"dim_genres.csv: dropped {dropped:,} invalid rows (missing id/genre)")
    for _, row in df.iterrows():
        gid = int(row["genre_id"])
        g   = std_genre(row["genre"])
        if not g:
            continue
        keys.genre_key[g] = gid
        if gid > keys._genre_seq:
            keys._genre_seq = gid
    logger.info(f"Loaded existing genres: {len(df):,}; max genre_id: {keys._genre_seq}")
    return int(keys._genre_seq)

def load_dim_locations(path: Path, keys: KeySpace) -> int:
    """Load existing locations into KeySpace and return max location_id."""
    if not path.exists():
        logger.info(f"No existing dim_locations.csv (will start fresh): {path.resolve()}")
        return 0
    df = pd.read_csv(path, dtype={"location_id": "Int64", "state_code": "string"})
    before = len(df)
    df = df.dropna(subset=["location_id", "state_code"])
    dropped = before - len(df)
    if dropped:
        logger.warning(f"dim_locations.csv: dropped {dropped:,} invalid rows (missing id/state_code)")
    for _, row in df.iterrows():
        lid = int(row["location_id"])
        st  = std_state(row["state_code"])
        if not st:
            continue
        keys.location_key[st] = lid
        if lid > keys._location_seq:
            keys._location_seq = lid
    logger.info(f"Loaded existing locations: {len(df):,}; max location_id: {keys._location_seq}")
    return int(keys._location_seq)

def append_csv(path: Path, df: pd.DataFrame, label: str):
    """Append DataFrame rows to CSV at `path`. Skips if df empty. Logs row count."""
    if df is None or df.empty:
        logger.info(f"No NEW {label} rows to append.")
        return
    prev_count = 0
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            prev_count = max(sum(1 for _ in f) - 1, 0)
    df.to_csv(path, mode="a", header=False, index=False, quoting=csv.QUOTE_MINIMAL)
    logger.info(f"Appended {len(df):,} NEW {label} rows to {path.resolve()} (previous count ~{prev_count:,})")

def incremental_finalize_dimensions(keys: KeySpace, outdir: Path,
                                    existing_max_artist: int,
                                    existing_max_genre: int,
                                    existing_max_loc: int) -> None:
    """Compare new IDs in KeySpace with existing max IDs and append only NEW rows."""
    dim_artists_path   = outdir / "dim_artists.csv"
    dim_genres_path    = outdir / "dim_genres.csv"
    dim_locations_path = outdir / "dim_locations.csv"

    init_csv_if_missing(dim_artists_path, ["artist_id", "spotify_id", "artist_name"])
    init_csv_if_missing(dim_genres_path,  ["genre_id", "genre"])
    init_csv_if_missing(dim_locations_path, ["location_id", "state_code"])

    # artists
    inv_artist = {aid: sid for sid, aid in keys.artist_key.items()}
    new_artist_ids = [aid for aid in sorted(inv_artist.keys()) if aid > existing_max_artist]
    if new_artist_ids:
        dim_artists = pd.DataFrame({
            "artist_id": new_artist_ids,
            "spotify_id": [inv_artist[aid] for aid in new_artist_ids],
            "artist_name": [keys.artist_label.get(aid, "") for aid in new_artist_ids],
        })
        append_csv(dim_artists_path, dim_artists, "artist")
    else:
        logger.info("No NEW artists to persist.")

    # genres
    inv_genre = {gid: g for g, gid in keys.genre_key.items()}
    new_genre_ids = [gid for gid in sorted(inv_genre.keys()) if gid > existing_max_genre]
    if new_genre_ids:
        dim_genres = pd.DataFrame({
            "genre_id": new_genre_ids,
            "genre": [inv_genre[gid] for gid in new_genre_ids],
        })
        append_csv(dim_genres_path, dim_genres, "genre")
    else:
        logger.info("No NEW genres to persist.")

    # locations
    inv_loc = {lid: s for s, lid in keys.location_key.items()}
    new_loc_ids = [lid for lid in sorted(inv_loc.keys()) if lid > existing_max_loc]
    if new_loc_ids:
        dim_locations = pd.DataFrame({
            "location_id": new_loc_ids,
            "state_code": [inv_loc[lid] for lid in new_loc_ids],
        })
        append_csv(dim_locations_path, dim_locations, "location")
    else:
        logger.info("No NEW locations to persist.")

# ---------- public API ----------

def persist_dimensions_for_input(input_path: Path, outdir: Path = PERSISTED_DIR, chunksize: int = 500_000) -> None:
    """Main entrypoint."""
    input_path = Path(input_path)
    logger.info(f"Persist dims FROM cleaned file: {input_path.resolve()}")
    if not input_path.exists():
        raise FileNotFoundError(f"Cleaned input file not found: {input_path.resolve()}")

    ensure_dir(outdir)

    dim_artists_path   = outdir / "dim_artists.csv"
    dim_genres_path    = outdir / "dim_genres.csv"
    dim_locations_path = outdir / "dim_locations.csv"
    init_csv_if_missing(dim_artists_path, ["artist_id", "spotify_id", "artist_name"])
    init_csv_if_missing(dim_genres_path,  ["genre_id", "genre"])
    init_csv_if_missing(dim_locations_path, ["location_id", "state_code"])

    keys = KeySpace()
    existing_max_artist = load_dim_artists(dim_artists_path, keys)
    existing_max_genre  = load_dim_genres(dim_genres_path, keys)
    existing_max_loc    = load_dim_locations(dim_locations_path, keys)

    total_scanned = 0
    total_after_drops = 0
    for chunk in pd.read_csv(
        input_path,
        chunksize=chunksize,
        dtype={"artist": "string", "id": "string", "genres": "string", "location": "string"},
        low_memory=True
    ):
        scanned = len(chunk)
        total_scanned += scanned

        # Defensive standardization
        chunk["artist"]   = chunk["artist"].map(std_name)
        chunk["id"]       = chunk["id"].map(std_spotify_id)
        chunk["location"] = chunk["location"].map(std_state)
        chunk["genres"]   = chunk["genres"].map(std_genres_string).fillna("Unknown")

        # drop rows missing essentials
        before = len(chunk)
        chunk = chunk[chunk["id"].notna() & chunk["location"].notna()]
        dropped = before - len(chunk)
        total_after_drops += len(chunk)
        if dropped:
            logger.warning(f"dim scan: dropped {dropped:,} rows missing id/location (chunk size {before:,})")

        if chunk.empty:
            continue

        # artists + locations
        for sid, nm, st in zip(chunk["id"], chunk["artist"], chunk["location"]):
            keys.intern_artist(str(sid), nm)
            keys.intern_location(str(st))

        # genres (split defensively)
        genre_lists = chunk["genres"].str.split(";")
        lists = [g if isinstance(g, list) else [] for g in genre_lists.values]
        if lists:
            flat = pd.Series(np.concatenate(lists))
            flat = (
                flat.map(std_genre)
                    .replace("", pd.NA)
                    .dropna()
                    .unique()
                    .tolist()
            )
            for g in flat:
                keys.intern_genre(g)

    logger.info(f"Scanned cleaned rows: {total_scanned:,}; rows after defensive drops: {total_after_drops:,}")

    incremental_finalize_dimensions(
        keys, outdir,
        existing_max_artist=existing_max_artist,
        existing_max_genre=existing_max_genre,
        existing_max_loc=existing_max_loc
    )
    logger.info("Dimension persistence complete.")

# ---------- standalone, zero-input ----------

def persist_dims():
    """
    Standalone runner:
      1) Use latest RAW batch date via find_latest_raw_nested() to derive the target cleaned file name.
      2) Require that exact cleaned file to exist; error if missing.
    """
    raw_path, batch_date = find_latest_raw_nested(RAW_ROOT, logger=logger)  # e.g., YYYY_MM_DD
    if not batch_date:
        raise FileNotFoundError(
            f"No RAW batches found under {RAW_ROOT.resolve()} "
            f"(expected dated subfolders like YYYY_MM_DD)."
        )

    target_cleaned = TRANSFORMED_DIR / f"spotify_rising_cleaned_{batch_date}.csv"
    if not target_cleaned.exists():
        raise FileNotFoundError(
            f"Matching cleaned file for RAW batch {batch_date} not found.\n"
            f"Expected: {target_cleaned.resolve()}\n"
            f"Hint: run your transform step that writes cleaned CSVs to {TRANSFORMED_DIR.resolve()}."
        )

    ensure_dir(PERSISTED_DIR)
    persist_dimensions_for_input(target_cleaned, PERSISTED_DIR)

if __name__ == "__main__":
    persist_dims()
