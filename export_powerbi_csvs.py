#!/usr/bin/env python3
"""Export aggregated sales datasets from SQLite to dated CSV files."""

from __future__ import annotations

import logging
import sqlite3
import sys
from datetime import date
from pathlib import Path

import pandas as pd


DB_PATH = Path("sales_pipeline.db")
TABLE_NAME = "sales_validated"
EXPORT_DIR = Path("exports")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_sales_data(db_path: Path) -> pd.DataFrame:
    logging.info("Loading %s from %s", TABLE_NAME, db_path)
    with sqlite3.connect(db_path) as connection:
        df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", connection)

    if df.empty:
        raise ValueError(f"Table {TABLE_NAME!r} is empty.")

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    return df


def ensure_required_columns(df: pd.DataFrame) -> None:
    required_columns = {
        "order_id",
        "order_date",
        "customer_segment",
        "product_category",
        "sales",
        "profit",
        "discount",
        "region",
    }
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns in {TABLE_NAME}: {missing}")


def monthly_revenue_trend(df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        df.assign(Month=df["order_date"].dt.to_period("M").astype(str))
        .groupby("Month", as_index=False)
        .agg(Total_Sales=("sales", "sum"), Total_Profit=("profit", "sum"))
        .sort_values("Month")
    )
    monthly["Profit_Margin"] = (
        monthly["Total_Profit"].div(monthly["Total_Sales"]).replace([pd.NA, float("inf"), -float("inf")], 0) * 100
    ).fillna(0).round(2)
    monthly["Total_Sales"] = monthly["Total_Sales"].round(2)
    monthly["Total_Profit"] = monthly["Total_Profit"].round(2)
    return monthly[["Month", "Total_Sales", "Total_Profit", "Profit_Margin"]]


def region_kpi(df: pd.DataFrame) -> pd.DataFrame:
    region = (
        df.groupby("region", as_index=False)
        .agg(
            Revenue=("sales", "sum"),
            Profit=("profit", "sum"),
            Orders=("order_id", pd.Series.nunique),
        )
        .rename(columns={"region": "Region"})
    )
    region["AOV"] = (region["Revenue"] / region["Orders"]).round(2)
    region["Revenue"] = region["Revenue"].round(2)
    region["Profit"] = region["Profit"].round(2)
    return region[["Region", "Revenue", "Profit", "Orders", "AOV"]]


def product_performance(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    if "sub_category" not in working.columns:
        logging.warning(
            "Column 'sub_category' not found. Using 'Unknown' for Sub_Category export."
        )
        working["sub_category"] = "Unknown"

    product = (
        working.groupby(["product_category", "sub_category"], as_index=False)
        .agg(Revenue=("sales", "sum"), Profit=("profit", "sum"))
        .rename(
            columns={
                "product_category": "Category",
                "sub_category": "Sub_Category",
            }
        )
    )
    product["Profit_Margin"] = (
        product["Profit"].div(product["Revenue"]).replace([pd.NA, float("inf"), -float("inf")], 0) * 100
    ).fillna(0).round(2)
    product["Revenue"] = product["Revenue"].round(2)
    return product[["Category", "Sub_Category", "Revenue", "Profit_Margin"]]


def customer_segment(df: pd.DataFrame) -> pd.DataFrame:
    segment = (
        df.groupby("customer_segment", as_index=False)
        .agg(
            Revenue=("sales", "sum"),
            Orders=("order_id", pd.Series.nunique),
        )
        .rename(columns={"customer_segment": "Segment"})
    )
    segment["Avg_Order_Value"] = (segment["Revenue"] / segment["Orders"]).round(2)
    segment["Revenue"] = segment["Revenue"].round(2)
    return segment[["Segment", "Revenue", "Orders", "Avg_Order_Value"]]


def discount_impact(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    working["Discount_Bucket"] = pd.cut(
        working["discount"],
        bins=[-0.001, 0.10, 0.20, 0.40, float("inf")],
        labels=["0-10%", "10-20%", "20-40%", "40%+"],
    )
    working["profit_margin"] = (
        working["profit"].div(working["sales"]).replace([pd.NA, float("inf"), -float("inf")], 0) * 100
    ).fillna(0)

    discount = (
        working.groupby("Discount_Bucket", observed=False, as_index=False)
        .agg(
            Avg_Profit_Margin=("profit_margin", "mean"),
            Order_Count=("order_id", pd.Series.nunique),
        )
    )
    discount["Avg_Profit_Margin"] = discount["Avg_Profit_Margin"].round(2)
    return discount[["Discount_Bucket", "Avg_Profit_Margin", "Order_Count"]]


def export_dataframe(df: pd.DataFrame, base_name: str, export_dir: Path, suffix: str) -> None:
    file_path = export_dir / f"{base_name}_{suffix}.csv"
    df.to_csv(file_path, index=False)
    logging.info("Exported %s", file_path)


def main() -> None:
    configure_logging()

    if not DB_PATH.exists():
        logging.error("Database not found: %s", DB_PATH)
        sys.exit(1)

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    today_suffix = date.today().isoformat()

    try:
        sales_df = load_sales_data(DB_PATH)
        ensure_required_columns(sales_df)

        export_dataframe(
            monthly_revenue_trend(sales_df),
            "monthly_revenue_trend",
            EXPORT_DIR,
            today_suffix,
        )
        export_dataframe(region_kpi(sales_df), "region_kpi", EXPORT_DIR, today_suffix)
        export_dataframe(
            product_performance(sales_df),
            "product_performance",
            EXPORT_DIR,
            today_suffix,
        )
        export_dataframe(
            customer_segment(sales_df),
            "customer_segment",
            EXPORT_DIR,
            today_suffix,
        )
        export_dataframe(
            discount_impact(sales_df),
            "discount_impact",
            EXPORT_DIR,
            today_suffix,
        )
        logging.info("All Power BI export files were created successfully")
    except Exception as exc:
        logging.exception("Export failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
