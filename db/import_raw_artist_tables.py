"""
connect.py

Loads a CSV file and imports it into a MySQL database table using credentials
from a .env file located in the `config/` directory.
"""
import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# Set the path to the .env file in the config/ directory
dotenv_path = Path(__file__).resolve().parent.parent / "config" / ".env"
load_dotenv(dotenv_path=dotenv_path)

# === Debug (optional - remove when confident) ===
# print("DEBUG - MYSQL_HOST:", os.getenv("MYSQL_HOST"))
# print("DEBUG - Using .env path:", dotenv_path)

batch_date = datetime.now().strftime('%Y_%m_%d')
# batch_date = '2025_07_12'
input_path = Path(f"data/stage1_artists/spotify_rising_artists_{batch_date}.csv") #Verify this is the most up to date path
table_name = rf"spotify_rising_artists_raw_{batch_date}"

# === Database config from .env ===
db_config = {
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "host": os.getenv("MYSQL_HOST"), 
    "port": int(os.getenv("MYSQL_PORT") or 3306),
    "database": os.getenv("MYSQL_DB")
}

# === CONNECT TO DATABASE ===
# Format: mysql+mysqlconnector://<user>:<password>@<host>:<port>/<database>
connection_string = (
    f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}"
    f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

engine = create_engine(connection_string)


def import_to_mysql(csv_path: Path, table_name: str, engine):
    try:
        df = pd.read_csv(input_path)
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)     # If table exists: 'replace' will drop it, 'append' will add to it
        print(f"Imported {len(df)} rows into '{table_name}' table.")

    except FileNotFoundError:
        print(f"File not found: {csv_path}")
    
    except Exception as e:
        print(f"Failed to import: {e}")

def main():
    import_to_mysql(input_path, table_name, engine)

if __name__ == "__main__":
    main()
