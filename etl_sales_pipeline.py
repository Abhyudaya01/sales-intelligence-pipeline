#!/usr/bin/env python3
"""ETL pipeline for cleaning sales data and loading it into SQLite."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd


DB_PATH = Path("sales_pipeline.db")
TABLE_NAME = "sales_cleaned"

REQUIRED_COLUMNS = [
    "Order ID",
    "Order Date",
    "Ship Date",
    "Customer Segment",
    "Product Category",
    "Sales",
    "Profit",
    "Quantity",
    "Discount",
    "Region",
]

RENAME_MAP = {
    "Order ID": "order_id",
    "Order Date": "order_date",
    "Ship Date": "ship_date",
    "Customer Segment": "customer_segment",
    "Product Category": "product_category",
    "Sales": "sales",
    "Profit": "profit",
    "Quantity": "quantity",
    "Discount": "discount",
    "Region": "region",
}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def extract(csv_path: Path) -> pd.DataFrame:
    logging.info("Starting extract step from %s", csv_path)
    df = pd.read_csv(csv_path)
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    extracted_df = df[REQUIRED_COLUMNS].copy()
    logging.info("Extracted %s rows and %s columns", *extracted_df.shape)
    return extracted_df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting transform step")

    transformed_df = df.copy()
    transformed_df["Order Date"] = pd.to_datetime(
        transformed_df["Order Date"], errors="coerce"
    )
    transformed_df["Ship Date"] = pd.to_datetime(
        transformed_df["Ship Date"], errors="coerce"
    )

    string_columns = transformed_df.select_dtypes(include="object").columns
    for column in string_columns:
        transformed_df[column] = transformed_df[column].apply(
            lambda value: value.strip() if isinstance(value, str) else value
        )

    transformed_df = transformed_df.rename(columns=RENAME_MAP)
    logging.info("Renamed columns to snake_case")
    return transformed_df


def load(df: pd.DataFrame, db_path: Path) -> None:
    logging.info("Starting load step into %s (%s)", db_path, TABLE_NAME)
    with sqlite3.connect(db_path) as connection:
        df.to_sql(TABLE_NAME, connection, if_exists="replace", index=False)
    logging.info("Loaded %s rows into table %s", len(df), TABLE_NAME)


def run_etl(csv_path: Path) -> None:
    extracted_df = extract(csv_path)
    transformed_df = transform(extracted_df)
    load(transformed_df, DB_PATH)
    logging.info("ETL pipeline completed successfully")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean a raw sales CSV and load it into SQLite."
    )
    parser.add_argument("csv_path", type=Path, help="Path to the raw sales CSV file")
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()

    if not args.csv_path.exists():
        logging.error("Input CSV not found: %s", args.csv_path)
        sys.exit(1)

    try:
        run_etl(args.csv_path)
    except Exception as exc:
        logging.exception("ETL pipeline failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
