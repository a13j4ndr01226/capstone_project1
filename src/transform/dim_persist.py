"""
Reusable, lightweight persistence module for dimension CSV caches (artists, genres, locations).
- Importable API: persist_dimensions_for_input(input_path, outdir=PERSISTED_DIR)
- Standalone auto-run (NO CLI args): detects latest stage2 file and persists dims.

What it does (PERSIST ONLY):
- Loads existing dim_* CSVs from data/persisted_dims/ to continue surrogate key sequences.
- Scans the stage2 input to discover NEW artists/genres/locations.
- Appends only NEW rows to dim_artists.csv, dim_genres.csv, dim_locations.csv.

"""

from __future__ import annotations
import re
import csv
from pathlib import Path
from typing import Dict, Optional
import pandas as pd
import numpy as np

STAGE2_DIR = Path("data/stage2_trend_enrichment")
PERSISTED_DIR = Path("data/persisted_dims")


# ---------- helpers ----------
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def init_csv_if_missing(path: Path, headers):
    """Create CSV header if file is missing (non-destructive)."""
    ensure_dir(path.parent)
    if not path.exists():
        path.write_text(",".join(headers) + "\n", encoding="utf-8")

def find_latest_stage2_file(root: Path = STAGE2_DIR):
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
    candidates.sort(key=lambda x: x[0])
    return candidates[-1][1]

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
    return _std(x)

def std_genre(x):
    s = _std(x)
    return s.lower() if s else None


# ---------- in-memory keyspace ----------
class KeySpace:
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
    if not path.exists():
        return 0
    df = pd.read_csv(path, dtype={"artist_id": "Int64", "spotify_id": "string", "artist_name": "string"})
    df = df.dropna(subset=["artist_id", "spotify_id"])
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
    return int(keys._artist_seq)

def load_dim_genres(path: Path, keys: KeySpace) -> int:
    if not path.exists():
        return 0
    df = pd.read_csv(path, dtype={"genre_id": "Int64", "genre": "string"})
    df = df.dropna(subset=["genre_id", "genre"])
    for _, row in df.iterrows():
        gid = int(row["genre_id"])
        g   = std_genre(row["genre"])
        if not g:
            continue
        keys.genre_key[g] = gid
        if gid > keys._genre_seq:
            keys._genre_seq = gid
    return int(keys._genre_seq)

def load_dim_locations(path: Path, keys: KeySpace) -> int:
    if not path.exists():
        return 0
    df = pd.read_csv(path, dtype={"location_id": "Int64", "state_code": "string"})
    df = df.dropna(subset=["location_id", "state_code"])
    for _, row in df.iterrows():
        lid = int(row["location_id"])
        st  = std_state(row["state_code"])
        if not st:
            continue
        keys.location_key[st] = lid
        if lid > keys._location_seq:
            keys._location_seq = lid
    return int(keys._location_seq)

def append_csv(path: Path, df: pd.DataFrame):
    if df is None or df.empty:
        return
    df.to_csv(path, mode="a", header=False, index=False, quoting=csv.QUOTE_MINIMAL)

def incremental_finalize_dimensions(keys: KeySpace, outdir: Path,
                                    existing_max_artist: int,
                                    existing_max_genre: int,
                                    existing_max_loc: int) -> None:
    dim_artists_path = outdir / "dim_artists.csv"
    dim_genres_path  = outdir / "dim_genres.csv"
    dim_locations_path = outdir / "dim_locations.csv"

    init_csv_if_missing(dim_artists_path, ["artist_id", "spotify_id", "artist_name"])
    init_csv_if_missing(dim_genres_path, ["genre_id", "genre"])
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
        append_csv(dim_artists_path, dim_artists)

    # genres
    inv_genre = {gid: g for g, gid in keys.genre_key.items()}
    new_genre_ids = [gid for gid in sorted(inv_genre.keys()) if gid > existing_max_genre]
    if new_genre_ids:
        dim_genres = pd.DataFrame({
            "genre_id": new_genre_ids,
            "genre": [inv_genre[gid] for gid in new_genre_ids],
        })
        append_csv(dim_genres_path, dim_genres)

    # locations
    inv_loc = {lid: s for s, lid in keys.location_key.items()}
    new_loc_ids = [lid for lid in sorted(inv_loc.keys()) if lid > existing_max_loc]
    if new_loc_ids:
        dim_locations = pd.DataFrame({
            "location_id": new_loc_ids,
            "state_code": [inv_loc[lid] for lid in new_loc_ids],
        })
        append_csv(dim_locations_path, dim_locations)


# ---------- public API ----------
def persist_dimensions_for_input(input_path: Path, outdir: Path = PERSISTED_DIR, chunksize: int = 500_000) -> None:
    """Persist dim_artists, dim_genres, dim_locations for the given stage2 input file."""
    ensure_dir(outdir)

    dim_artists_path = outdir / "dim_artists.csv"
    dim_genres_path  = outdir / "dim_genres.csv"
    dim_locations_path = outdir / "dim_locations.csv"
    init_csv_if_missing(dim_artists_path, ["artist_id", "spotify_id", "artist_name"])
    init_csv_if_missing(dim_genres_path, ["genre_id", "genre"])
    init_csv_if_missing(dim_locations_path, ["location_id", "state_code"])

    keys = KeySpace()
    existing_max_artist = load_dim_artists(dim_artists_path, keys)
    existing_max_genre  = load_dim_genres(dim_genres_path, keys)
    existing_max_loc    = load_dim_locations(dim_locations_path, keys)

    # scan input -> intern natural keys
    for chunk in pd.read_csv(
        input_path,
        chunksize=chunksize,
        dtype={"artist": "string", "id": "string", "genres": "string", "location": "string"},
        low_memory=True
    ):
        chunk["artist"] = chunk["artist"].map(std_name)
        chunk["id"] = chunk["id"].map(std_spotify_id)
        chunk["location"] = chunk["location"].map(std_state)
        chunk["genres"] = chunk["genres"].map(std_genres_string).fillna("Unknown")

        # drop rows missing essentials
        chunk = chunk[chunk["id"].notna() & chunk["location"].notna()]
        if chunk.empty:
            continue

        # artists + locations
        for sid, nm, st in zip(chunk["id"], chunk["artist"], chunk["location"]):
            keys.intern_artist(sid, nm)
            keys.intern_location(st)

        # genres (collect distincts)
        genre_lists = chunk["genres"].str.split(";")
        lists = [g if isinstance(g, list) else [] for g in genre_lists.values]
        if len(lists) == 0:
            flat = []
        else:
            flat = pd.Series(np.concatenate(lists))
        if len(flat) > 0:
            flat = flat.map(std_genre).replace("", pd.NA).dropna().unique().tolist()
            for g in flat:
                keys.intern_genre(g)

    # append only NEW rows
    incremental_finalize_dimensions(keys, outdir,
                                    existing_max_artist=existing_max_artist,
                                    existing_max_genre=existing_max_genre,
                                    existing_max_loc=existing_max_loc)


# ---------- standalone, zero-input ----------
def persist_dims():
    latest = find_latest_stage2_file(STAGE2_DIR)
    if latest is None:
        raise FileNotFoundError(f"No stage2 files found in {STAGE2_DIR}")
    ensure_dir(PERSISTED_DIR)
    persist_dimensions_for_input(latest, PERSISTED_DIR)

if __name__ == "__main__":
    persist_dims()
