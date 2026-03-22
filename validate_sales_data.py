#!/usr/bin/env python3
"""Validate cleaned sales data and persist a validated table to SQLite."""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd


DB_PATH = Path("sales_pipeline.db")
SOURCE_TABLE = "sales_cleaned"
TARGET_TABLE = "sales_validated"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_data(db_path: Path) -> pd.DataFrame:
    logging.info("Connecting to SQLite database: %s", db_path)
    with sqlite3.connect(db_path) as connection:
        df = pd.read_sql_query(f"SELECT * FROM {SOURCE_TABLE}", connection)
    logging.info("Loaded %s rows from table %s", len(df), SOURCE_TABLE)
    return df


def build_quality_report(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Generating data quality report")
    report = pd.DataFrame(
        {
            "column_name": df.columns,
            "null_count": df.isnull().sum().values,
            "null_percentage": (df.isnull().mean() * 100).round(2).values,
            "data_type": df.dtypes.astype(str).values,
        }
    )
    return report


def validate_data(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    logging.info("Running data validation checks")

    working_df = df.copy()
    working_df["order_date"] = pd.to_datetime(working_df["order_date"], errors="coerce")
    working_df["ship_date"] = pd.to_datetime(working_df["ship_date"], errors="coerce")

    null_mask = working_df.isnull().any(axis=1)
    duplicate_mask = working_df.duplicated(subset=["order_id"], keep="first")
    negative_mask = (working_df["sales"] < 0) | (working_df["profit"] < 0)
    invalid_date_mask = working_df["order_date"] > working_df["ship_date"]

    invalid_mask = null_mask | duplicate_mask | negative_mask | invalid_date_mask

    stats = {
        "rows_with_nulls": int(null_mask.sum()),
        "duplicate_order_ids": int(duplicate_mask.sum()),
        "negative_sales_or_profit": int(negative_mask.sum()),
        "invalid_date_ranges": int(invalid_date_mask.sum()),
        "total_invalid_rows": int(invalid_mask.sum()),
    }

    validated_df = working_df.loc[~invalid_mask].copy()
    return validated_df, stats


def save_validated_data(df: pd.DataFrame, db_path: Path) -> None:
    logging.info("Saving validated dataset to table %s", TARGET_TABLE)
    with sqlite3.connect(db_path) as connection:
        df.to_sql(TARGET_TABLE, connection, if_exists="replace", index=False)
    logging.info("Saved %s validated rows", len(df))


def main() -> None:
    configure_logging()

    if not DB_PATH.exists():
        logging.error("Database not found: %s", DB_PATH)
        sys.exit(1)

    try:
        sales_df = load_data(DB_PATH)
        quality_report = build_quality_report(sales_df)
        validated_df, stats = validate_data(sales_df)

        dropped_rows = len(sales_df) - len(validated_df)
        logging.info("Rows with null values: %s", stats["rows_with_nulls"])
        logging.info("Rows with duplicate order IDs: %s", stats["duplicate_order_ids"])
        logging.info(
            "Rows with negative sales or profit: %s",
            stats["negative_sales_or_profit"],
        )
        logging.info("Rows with invalid date ranges: %s", stats["invalid_date_ranges"])
        logging.info("Dropped %s invalid rows", dropped_rows)

        save_validated_data(validated_df, DB_PATH)

        print("\nData Quality Report:")
        print(quality_report.to_string(index=False))
        print(f"\nValidated row count: {len(validated_df)}")
    except Exception as exc:
        logging.exception("Data validation failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
