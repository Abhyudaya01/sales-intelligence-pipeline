#!/usr/bin/env python3
"""SQLite KPI queries for the sales_validated table."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


DB_PATH = Path("sales_pipeline.db")
TABLE_NAME = "sales_validated"


def _get_connection(db_path: Path | str = DB_PATH) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def _read_sql(query: str, db_path: Path | str = DB_PATH) -> pd.DataFrame:
    with _get_connection(db_path) as connection:
        return pd.read_sql_query(query, connection)


def _get_table_columns(db_path: Path | str = DB_PATH) -> set[str]:
    query = f"PRAGMA table_info({TABLE_NAME})"
    schema_df = _read_sql(query, db_path)
    return set(schema_df["name"].tolist())


def get_revenue_profit_margin_by_region(
    db_path: Path | str = DB_PATH,
) -> pd.DataFrame:
    query = f"""
        SELECT
            region,
            ROUND(SUM(sales), 2) AS total_revenue,
            ROUND(SUM(profit), 2) AS total_profit,
            ROUND(
                CASE
                    WHEN SUM(sales) = 0 THEN 0
                    ELSE (SUM(profit) * 100.0) / SUM(sales)
                END,
                2
            ) AS profit_margin_pct
        FROM {TABLE_NAME}
        GROUP BY region
        ORDER BY total_revenue DESC
    """
    return _read_sql(query, db_path)


def get_monthly_revenue_trend(db_path: Path | str = DB_PATH) -> pd.DataFrame:
    query = f"""
        SELECT
            strftime('%Y-%m', order_date) AS year_month,
            ROUND(SUM(sales), 2) AS monthly_revenue
        FROM {TABLE_NAME}
        GROUP BY strftime('%Y-%m', order_date)
        ORDER BY year_month
    """
    return _read_sql(query, db_path)


def get_top_10_products_by_revenue(db_path: Path | str = DB_PATH) -> pd.DataFrame:
    columns = _get_table_columns(db_path)
    if "product_name" not in columns:
        raise ValueError(
            "The sales_validated table does not contain a 'product_name' column. "
            "Update the ETL pipeline to preserve product-level data before running "
            "the Top 10 Products KPI."
        )

    query = f"""
        SELECT
            product_name,
            ROUND(SUM(sales), 2) AS total_revenue
        FROM {TABLE_NAME}
        GROUP BY product_name
        ORDER BY total_revenue DESC
        LIMIT 10
    """
    return _read_sql(query, db_path)


def get_sales_profit_by_customer_segment(
    db_path: Path | str = DB_PATH,
) -> pd.DataFrame:
    query = f"""
        SELECT
            customer_segment,
            ROUND(SUM(sales), 2) AS total_sales,
            ROUND(SUM(profit), 2) AS total_profit
        FROM {TABLE_NAME}
        GROUP BY customer_segment
        ORDER BY total_sales DESC
    """
    return _read_sql(query, db_path)


def get_aov_by_product_category(db_path: Path | str = DB_PATH) -> pd.DataFrame:
    query = f"""
        SELECT
            product_category,
            ROUND(
                CASE
                    WHEN COUNT(DISTINCT order_id) = 0 THEN 0
                    ELSE SUM(sales) * 1.0 / COUNT(DISTINCT order_id)
                END,
                2
            ) AS average_order_value
        FROM {TABLE_NAME}
        GROUP BY product_category
        ORDER BY average_order_value DESC
    """
    return _read_sql(query, db_path)


def get_discount_pass_fail_rate(db_path: Path | str = DB_PATH) -> pd.DataFrame:
    query = f"""
        SELECT
            discount_flag,
            order_count,
            ROUND(order_count * 100.0 / total_orders.total_count, 2) AS percentage
        FROM (
            SELECT
                CASE
                    WHEN discount > 0.4 THEN 'high_discount'
                    ELSE 'pass'
                END AS discount_flag,
                COUNT(*) AS order_count
            FROM {TABLE_NAME}
            GROUP BY
                CASE
                    WHEN discount > 0.4 THEN 'high_discount'
                    ELSE 'pass'
                END
        ) AS flagged_orders
        CROSS JOIN (
            SELECT COUNT(*) AS total_count
            FROM {TABLE_NAME}
        ) AS total_orders
        ORDER BY percentage DESC
    """
    return _read_sql(query, db_path)

