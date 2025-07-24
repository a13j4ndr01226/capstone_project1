"""
connect.py

Loads a CSV file and imports it into a PostgreSQL database table using credentials
from a .env file located in the `config/` directory.
"""
import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# === Load .env ===
dotenv_path = Path(__file__).resolve().parent.parent / "config" / ".env"
load_dotenv(dotenv_path=dotenv_path)

# === File path and table name ===
batch_date = '2025_06_18'
input_path = Path(f"data/stage2_trend_enrichment/spotify_rising_with_trends_{batch_date}.csv")
table_name = f"spotify_rising_with_trends_enriched_{batch_date}"

# === PostgreSQL config ===
db_config = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT") or 5432),
    "database": os.getenv("POSTGRES_DB")
}

# === Build PostgreSQL connection string ===
# Format: postgresql+psycopg2://<user>:<password>@<host>:<port>/<database>
connection_string = (
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
    f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# === Create SQLAlchemy engine ===
engine = create_engine(connection_string)

def import_to_postgres(csv_path: Path, table_name: str, engine):
    try:
        df = pd.read_csv(csv_path, parse_dates=["date"], dtype={
            "artist": str,
            "id": str,
            "genres": str,
            "location": str,
            "trend_score": float
        })
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        print(f"Imported {len(df)} rows into '{table_name}' table.")

    except FileNotFoundError:
        print(f"File not found: {csv_path}")
    except Exception as e:
        print(f"Failed to import: {e}")

def main():
    import_to_postgres(input_path, table_name, engine)

if __name__ == "__main__":
    main()