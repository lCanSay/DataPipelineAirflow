import sqlite3
import pandas as pd
from pathlib import Path

CLEANED_CSV = "data/cleaned.csv"
DB_PATH = "data/gog.db"
TABLE_NAME = "games"

SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    product_id TEXT PRIMARY KEY,
    title TEXT,
    url TEXT,
    page INTEGER,
    base_price REAL,
    final_price REAL,
    discount_percent INTEGER,
    discount_amount REAL,
    is_discounted INTEGER
);
"""

def create_db(db_path=DB_PATH):
    Path("data").mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    conn.close()
    print(f"[loader] created DB and table at {db_path}")

def load_csv_to_db(csv_path=CLEANED_CSV, db_path=DB_PATH, table=TABLE_NAME):
    df = pd.read_csv(csv_path)

    conn = sqlite3.connect(db_path)
    df.to_sql(table, conn, if_exists="replace", index=False)
    conn.close()
    print(f"[loader] inserted {len(df)} rows into {db_path}::{table}")

if __name__ == "__main__":
    create_db()
    load_csv_to_db()
