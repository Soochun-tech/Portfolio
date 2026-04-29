"""
Load raw datasets (NYC Taxi parquet + Olist CSV) into MySQL `raw_*` tables.

Usage:
    # Activate venv and ensure .env is configured first.
    python scripts/load_to_mysql.py --dataset nyc_taxi
    python scripts/load_to_mysql.py --dataset olist
    python scripts/load_to_mysql.py --dataset all

Notes:
    * Loads in append mode by default. Use --truncate to reset target tables first.
    * NYC Taxi parquet is loaded month-by-month with chunked inserts to avoid OOM.
    * Olist CSV is small (10万 rows / table) and loads in one shot.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from tqdm import tqdm

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data" / "raw"


def get_engine():
    load_dotenv(ROOT / ".env")
    user = os.environ.get("MYSQL_USER", "analyst")
    pw = os.environ.get("MYSQL_PASSWORD", "analystpw")
    host = os.environ.get("MYSQL_HOST", "localhost")
    port = os.environ.get("MYSQL_PORT", "3306")
    db = os.environ.get("MYSQL_DATABASE", "portfolio")
    # Use mysql-connector-python driver — natively supports caching_sha2_password
    # (MySQL 8.0+ default auth plugin) without needing the `cryptography` package.
    # Use mysql-connector-python driver — natively supports caching_sha2_password
    # (MySQL 8.0+ default auth plugin) without needing the `cryptography` package.
    url = f"mysql+mysqlconnector://{user}:{pw}@{host}:{port}/{db}?charset=utf8mb4"
    # `allow_local_infile=True` enables the fast LOAD DATA LOCAL INFILE path
    # used for the NYC Taxi loader. Must be passed via connect_args for
    # mysql-connector-python (URL query param doesn't get coerced to bool).
    # Server side also needs `local_infile=1`.
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"allow_local_infile": True},
    )


def load_nyc_taxi(engine, truncate: bool = False) -> None:
    """Fast loader using MySQL LOAD DATA LOCAL INFILE.

    Why: pandas `to_sql` with method='multi' generates massive INSERT statements
    that MySQL has to parse, and the round-trip overhead per chunk is huge.
    LOAD DATA LOCAL INFILE bypasses the SQL parser and streams CSV directly into
    InnoDB — typically 20-50x faster for million-row loads.

    Requires:
      * Server: `SET GLOBAL local_infile = 1;` (run once in Workbench)
      * Client: `allow_local_infile=true` in connection string (set in get_engine)
    """
    parquet_files = sorted((DATA_DIR / "nyc_taxi").glob("yellow_tripdata_*.parquet"))
    if not parquet_files:
        print("[!] No NYC Taxi parquet files found. Run scripts/download_nyc_taxi.sh first.")
        return

    table = "raw_yellow_taxi"
    if truncate:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table}"))
        print(f"[i] Truncated {table}")

    # Get target column order from MySQL — CSV must match this order
    with engine.begin() as conn:
        result = conn.execute(text(f"SHOW COLUMNS FROM {table}"))
        target_cols = [row[0] for row in result]

    for pq in parquet_files:
        print(f"[+] Loading {pq.name} via LOAD DATA LOCAL INFILE")
        df = pd.read_parquet(pq)
        df.columns = [c.lower() for c in df.columns]

        # Keep only columns present in target table, in the right order.
        # Missing columns get filled with NULL.
        for col in target_cols:
            if col not in df.columns:
                df[col] = pd.NA
        df = df[target_cols]

        # Write to temp CSV (NA -> \N which MySQL reads as NULL)
        tmp_csv = DATA_DIR / "nyc_taxi" / f".tmp_{pq.stem}.csv"
        df.to_csv(tmp_csv, index=False, header=False, na_rep=r"\N")
        size_mb = tmp_csv.stat().st_size / 1024 / 1024
        print(f"  -> wrote temp CSV ({size_mb:.1f} MB, {len(df):,} rows)")

        # LOAD DATA LOCAL INFILE — the fast path
        # Use forward slashes in path even on Windows; MySQL accepts both.
        path_for_sql = tmp_csv.as_posix().replace("'", "''")
        sql = (
            f"LOAD DATA LOCAL INFILE '{path_for_sql}' "
            f"INTO TABLE {table} "
            f"FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\' "
            f"LINES TERMINATED BY '\\n' "
            f"({', '.join('`' + c + '`' for c in target_cols)})"
        )
        with engine.begin() as conn:
            conn.execute(text(sql))
        print(f"  -> loaded {len(df):,} rows into {table}")

        # Clean up temp file
        tmp_csv.unlink()

    # Zone lookup — small enough that to_sql is fine
    zone_csv = DATA_DIR / "nyc_taxi" / "taxi_zone_lookup.csv"
    if zone_csv.exists():
        zdf = pd.read_csv(zone_csv)
        zdf.columns = [c.lower() for c in zdf.columns]
        zdf.to_sql("raw_taxi_zone_lookup", engine, if_exists="replace", index=False)
        print(f"[+] Loaded zone lookup: {len(zdf)} rows")


# Olist filename -> table name mapping
OLIST_FILES = {
    "olist_orders_dataset.csv": "raw_olist_orders",
    "olist_order_items_dataset.csv": "raw_olist_order_items",
    "olist_order_payments_dataset.csv": "raw_olist_order_payments",
    "olist_order_reviews_dataset.csv": "raw_olist_order_reviews",
    "olist_customers_dataset.csv": "raw_olist_customers",
    "olist_sellers_dataset.csv": "raw_olist_sellers",
    "olist_products_dataset.csv": "raw_olist_products",
    "olist_geolocation_dataset.csv": "raw_olist_geolocation",
    "product_category_name_translation.csv": "raw_olist_category_translation",
}


def load_olist(engine, truncate: bool = False) -> None:
    olist_dir = DATA_DIR / "olist"
    found = list(olist_dir.glob("*.csv"))
    if not found:
        print("[!] No Olist CSV files found. Download from "
              "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce and unzip into data/raw/olist/")
        return

    for fname, table in OLIST_FILES.items():
        path = olist_dir / fname
        if not path.exists():
            print(f"  [skip] {fname} not found")
            continue
        df = pd.read_csv(path)
        df.columns = [c.lower() for c in df.columns]
        mode = "replace" if truncate else "append"
        df.to_sql(table, engine, if_exists=mode, index=False, method="multi", chunksize=5_000)
        print(f"  [+] {fname} -> {table} ({len(df):,} rows, mode={mode})")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dataset", choices=["nyc_taxi", "olist", "all"], required=True)
    ap.add_argument("--truncate", action="store_true", help="Truncate target tables before loading")
    args = ap.parse_args()

    engine = get_engine()

    if args.dataset in ("nyc_taxi", "all"):
        load_nyc_taxi(engine, truncate=args.truncate)
    if args.dataset in ("olist", "all"):
        load_olist(engine, truncate=args.truncate)

    return 0


if __name__ == "__main__":
    sys.exit(main())
