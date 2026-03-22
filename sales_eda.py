#!/usr/bin/env python3
"""EDA script for the sales_validated SQLite table."""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


DB_PATH = Path("sales_pipeline.db")
TABLE_NAME = "sales_validated"
OUTPUT_DIR = Path("outputs")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_data(db_path: Path) -> pd.DataFrame:
    logging.info("Loading data from %s (%s)", db_path, TABLE_NAME)
    with sqlite3.connect(db_path) as connection:
        df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", connection)

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce")
    return df


def save_monthly_revenue_trend(df: pd.DataFrame, output_dir: Path) -> None:
    logging.info("Creating monthly revenue trend plot")
    monthly_revenue = (
        df.assign(year_month=df["order_date"].dt.to_period("M").astype(str))
        .groupby("year_month", as_index=False)["sales"]
        .sum()
        .sort_values("year_month")
    )

    plt.figure(figsize=(12, 6))
    sns.lineplot(data=monthly_revenue, x="year_month", y="sales", marker="o")
    plt.title("Monthly Revenue Trend")
    plt.xlabel("Year-Month")
    plt.ylabel("Revenue")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(output_dir / "monthly_revenue_trend.png", dpi=300)
    plt.close()


def save_profit_margin_distribution(df: pd.DataFrame, output_dir: Path) -> None:
    logging.info("Creating profit margin distribution plot")
    margin_df = df.loc[df["sales"] != 0, ["sales", "profit"]].copy()
    margin_df["profit_margin_pct"] = (margin_df["profit"] / margin_df["sales"]) * 100

    plt.figure(figsize=(10, 6))
    sns.histplot(margin_df["profit_margin_pct"], bins=30, kde=True)
    plt.title("Profit Margin Distribution")
    plt.xlabel("Profit Margin %")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(output_dir / "profit_margin_distribution.png", dpi=300)
    plt.close()


def save_correlation_heatmap(df: pd.DataFrame, output_dir: Path) -> None:
    logging.info("Creating correlation heatmap")
    corr_df = df[["sales", "profit", "quantity", "discount"]].corr(numeric_only=True)

    plt.figure(figsize=(8, 6))
    sns.heatmap(corr_df, annot=True, cmap="YlGnBu", fmt=".2f")
    plt.title("Correlation Heatmap")
    plt.tight_layout()
    plt.savefig(output_dir / "correlation_heatmap.png", dpi=300)
    plt.close()


def save_revenue_by_region_segment(df: pd.DataFrame, output_dir: Path) -> None:
    logging.info("Creating grouped revenue bar chart")
    grouped_df = (
        df.groupby(["region", "customer_segment"], as_index=False)["sales"]
        .sum()
        .rename(columns={"sales": "revenue"})
    )

    plt.figure(figsize=(12, 6))
    sns.barplot(data=grouped_df, x="region", y="revenue", hue="customer_segment")
    plt.title("Revenue by Region and Customer Segment")
    plt.xlabel("Region")
    plt.ylabel("Revenue")
    plt.tight_layout()
    plt.savefig(output_dir / "revenue_by_region_customer_segment.png", dpi=300)
    plt.close()


def print_top_sales_anomalies(df: pd.DataFrame) -> None:
    logging.info("Identifying top 3 sales anomalies using IQR")
    q1 = df["sales"].quantile(0.25)
    q3 = df["sales"].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    outliers = df.loc[
        (df["sales"] < lower_bound) | (df["sales"] > upper_bound),
        ["order_id", "order_date", "region", "customer_segment", "sales", "profit"],
    ].sort_values("sales", ascending=False)

    top_outliers = outliers.head(3)
    print("\nTop 3 Sales Anomalies (IQR Method):")
    if top_outliers.empty:
        print("No sales outliers detected.")
        return

    print(top_outliers.to_string(index=False))


def main() -> None:
    configure_logging()

    if not DB_PATH.exists():
        logging.error("Database not found: %s", DB_PATH)
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    try:
        sales_df = load_data(DB_PATH)
        save_monthly_revenue_trend(sales_df, OUTPUT_DIR)
        save_profit_margin_distribution(sales_df, OUTPUT_DIR)
        save_correlation_heatmap(sales_df, OUTPUT_DIR)
        save_revenue_by_region_segment(sales_df, OUTPUT_DIR)
        print_top_sales_anomalies(sales_df)
        logging.info("EDA completed successfully. Plots saved to %s", OUTPUT_DIR)
    except Exception as exc:
        logging.exception("EDA script failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
