"""
load_artists_trends.py

Load the CLEANED/TRANSFORMED CSV into Postgres.

Input file (already exploded by genre):
  data/transformed/YYYY_MM_DD/spotify_rising_cleaned_{batch_date}.csv
Columns expected in the cleaned CSV:
  artist,id,genres,location,date,trend_score

Destination table:
  {PG_SCHEMA}.{PG_TABLE}  
Columns in destination:
  artist, id, genre, state_code, state_name, date, trend_score, load_timestamp
"""

import os
import io
import sys
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from src.utils.logger_config import get_logger
from src.utils.find_latest_file import find_latest_raw_nested

logger = get_logger("Load")

# ----------------- Config -----------------
DOTENV_PATH = Path(__file__).resolve().parent.parent.parent / "config" / ".env"
if not DOTENV_PATH.exists():
    raise FileNotFoundError(f".env not found at {DOTENV_PATH}")
load_dotenv(dotenv_path=DOTENV_PATH)

# fail fast if anything critical is missing
required = ["POSTGRES_HOST","POSTGRES_PORT","POSTGRES_DB","POSTGRES_USER","POSTGRES_PASSWORD"]
missing = [k for k in required if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing env vars in {DOTENV_PATH}: {', '.join(missing)}")

# Schema & table (override via env if you like)
PG_SCHEMA = os.getenv("POSTGRES_SCHEMA_STAGING")
PG_TABLE  = os.getenv("POSTGRES_TABLE_STAGING")

# Connection
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = int(os.getenv("POSTGRES_PORT") or 5432)
DB_NAME = os.getenv("POSTGRES_DB") 

CONN_STR = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# USPS state mapping (add territories if needed)
US_STATES = {
    "AL": "Alabama","AK": "Alaska","AZ": "Arizona","AR": "Arkansas","CA": "California",
    "CO": "Colorado","CT": "Connecticut","DE": "Delaware","FL": "Florida","GA": "Georgia",
    "HI": "Hawaii","ID": "Idaho","IL": "Illinois","IN": "Indiana","IA": "Iowa",
    "KS": "Kansas","KY": "Kentucky","LA": "Louisiana","ME": "Maine","MD": "Maryland",
    "MA": "Massachusetts","MI": "Michigan","MN": "Minnesota","MS": "Mississippi","MO": "Missouri",
    "MT": "Montana","NE": "Nebraska","NV": "Nevada","NH": "New Hampshire","NJ": "New Jersey",
    "NM": "New Mexico","NY": "New York","NC": "North Carolina","ND": "North Dakota","OH": "Ohio",
    "OK": "Oklahoma","OR": "Oregon","PA": "Pennsylvania","RI": "Rhode Island","SC": "South Carolina",
    "SD": "South Dakota","TN": "Tennessee","TX": "Texas","UT": "Utah","VT": "Vermont",
    "VA": "Virginia","WA": "Washington","WV": "West Virginia","WI": "Wisconsin","WY": "Wyoming",
    "DC": "District of Columbia"
}

DDL_SQL = f"""
CREATE SCHEMA IF NOT EXISTS "{PG_SCHEMA}";

CREATE TABLE IF NOT EXISTS "{PG_SCHEMA}"."{PG_TABLE}" (
    artist          TEXT        NOT NULL,
    id              TEXT        NOT NULL,
    genre           TEXT        NOT NULL,
    state_code      CHAR(2)     NOT NULL,
    state_name      TEXT,
    date            DATE        NOT NULL,
    trend_score     DOUBLE PRECISION,
    load_timestamp  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_artist_trend_id_date_state
    ON "{PG_SCHEMA}"."{PG_TABLE}" (id, date, state_code);

CREATE INDEX IF NOT EXISTS ix_artist_trend_genre
    ON "{PG_SCHEMA}"."{PG_TABLE}" (genre);

CREATE INDEX IF NOT EXISTS ix_artist_trend_state_date
    ON "{PG_SCHEMA}"."{PG_TABLE}" (state_code, date);
"""

def ensure_schema_and_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(DDL_SQL))
    logger.info(f"Ensured schema/table: {PG_SCHEMA}.{PG_TABLE}")

def prepare_dataframe(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    df = pd.read_csv(
        csv_path,
        dtype={
            "artist": "string",
            "id": "string",
            "genres": "string",
            "location": "string",
        },
        parse_dates=["date"],
        keep_default_na=False
    )

    # Harmonize columns: genres->genre, location->state_code
    if "genres" not in df.columns:
        raise ValueError("Input file is missing 'genres' column")
    if "location" not in df.columns:
        raise ValueError("Input file is missing 'location' (state code) column")

    df = df.rename(columns={"genres": "genre", "location": "state_code"})

    # Enforce one-genre-per-row (defensive; transform step should already do this)
    # If any semicolons slip in, split/explode them here.
    needs_explode = df["genre"].astype(str).str.contains(";").any()
    if needs_explode:
        df["genre"] = df["genre"].astype(str).str.replace(r"[|,]", ";", regex=True).str.split(";")
        df = df.explode("genre", ignore_index=True)

    # Normalize fields
    df["genre"] = df["genre"].astype(str).str.strip().str.lower()
    df["state_code"] = df["state_code"].astype(str).str.strip().str.upper()
    # map state name
    df["state_name"] = df["state_code"].map(US_STATES).astype("string")

    # Ensure date is date (not datetime)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # trend_score numeric
    if "trend_score" in df.columns:
        df["trend_score"] = pd.to_numeric(df["trend_score"], errors="coerce")

    # Basic row filter: require essentials
    df = df[df["id"].notna() & df["genre"].notna() & df["state_code"].notna() & df["date"].notna()]
    df = df[df["genre"] != ""]

    # Final column order for COPY (omit load_timestamp to use DEFAULT NOW())
    df = df[["artist", "id", "genre", "state_code", "state_name", "date", "trend_score"]]

    # Final assertion: no multi-value genres remain
    assert not df["genre"].astype(str).str.contains(";").any(), "Genres not fully exploded"

    return df

def copy_df_to_postgres(engine: Engine, df: pd.DataFrame) -> int:
    # Write to an in-memory CSV buffer with the exact columns (header included)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Use raw connection to call COPY for speed (no context manager for raw_connection)
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        try:
            cols = '(artist, id, genre, state_code, state_name, date, trend_score)'
            copy_sql = f'''
                COPY "{PG_SCHEMA}"."{PG_TABLE}" {cols}
                FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
            '''
            cur.copy_expert(copy_sql, csv_buffer)
        finally:
            cur.close()
        raw_conn.commit()
    finally:
        raw_conn.close()

    return len(df)

def main():
    # Find the latest CLEANED file produced by transform
    CLEANED_ROOT = Path("data/transformed")
    latest_cleaned, batch_date = find_latest_raw_nested(
                                            CLEANED_ROOT, 
                                            expected_template="spotify_rising_cleaned_{date}.csv", 
                                            logger=logger)

    if latest_cleaned is None:
        raise FileNotFoundError(f"No cleaned files found in {CLEANED_ROOT}")

    logger.info(
        f"Batch date: {batch_date} | "
        f"Target DB={DB_NAME} on {DB_HOST}:{DB_PORT} | "
        f"Table={PG_SCHEMA}.{PG_TABLE} | Source={latest_cleaned.name}"
    )

    engine = create_engine(CONN_STR)
    ensure_schema_and_table(engine)

    df = prepare_dataframe(latest_cleaned)

    if df.empty:
        logger.warning("No rows to load after prepare_dataframe(). Skipping COPY.")
        return

    n = copy_df_to_postgres(engine, df)
    logger.info(f"Loaded {n:,} rows into {PG_SCHEMA}.{PG_TABLE}")

def load():
    try:
        main()
    except Exception as e:
        logger.exception(f"Load failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    load()