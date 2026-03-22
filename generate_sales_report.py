#!/usr/bin/env python3
"""Generate a one-page PDF business summary report from sales data."""

from __future__ import annotations

import logging
import sqlite3
import sys
from calendar import month_name
from datetime import date
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


DB_PATH = Path("sales_pipeline.db")
TABLE_NAME = "sales_validated"
OUTPUT_DIR = Path("outputs")
CHART_PATH = OUTPUT_DIR / "monthly_revenue_trend.png"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_sales_data(db_path: Path) -> pd.DataFrame:
    logging.info("Loading data from %s (%s)", db_path, TABLE_NAME)
    with sqlite3.connect(db_path) as connection:
        df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", connection)

    if df.empty:
        raise ValueError(f"Table {TABLE_NAME!r} is empty.")

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    return df


def create_monthly_revenue_chart(df: pd.DataFrame, chart_path: Path) -> None:
    logging.info("Creating monthly revenue chart at %s", chart_path)
    monthly = (
        df.assign(year_month=df["order_date"].dt.to_period("M").astype(str))
        .groupby("year_month", as_index=False)["sales"]
        .sum()
        .sort_values("year_month")
    )

    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(9, 3.6))
    sns.lineplot(data=monthly, x="year_month", y="sales", marker="o", color="#2f6b4f")
    plt.title("Monthly Revenue Trend")
    plt.xlabel("Month")
    plt.ylabel("Revenue")
    plt.xticks(rotation=45, ha="right", fontsize=8)
    plt.tight_layout()
    chart_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(chart_path, dpi=250)
    plt.close()


def compute_key_metrics(df: pd.DataFrame) -> dict[str, str]:
    total_revenue = df["sales"].sum()
    total_profit = df["profit"].sum()
    profit_margin = (total_profit / total_revenue * 100) if total_revenue else 0

    yearly = (
        df.dropna(subset=["order_date"])
        .assign(year=df["order_date"].dt.year)
        .groupby("year", as_index=False)["sales"]
        .sum()
        .sort_values("year")
    )
    if len(yearly) >= 2 and yearly.iloc[-2]["sales"] != 0:
        yoy_growth_value = (
            (yearly.iloc[-1]["sales"] - yearly.iloc[-2]["sales"])
            / yearly.iloc[-2]["sales"]
            * 100
        )
        yoy_growth = f"{yoy_growth_value:.2f}%"
    else:
        yoy_growth = "N/A"

    region_revenue = df.groupby("region", as_index=False)["sales"].sum()
    top_region = (
        region_revenue.sort_values("sales", ascending=False).iloc[0]["region"]
        if not region_revenue.empty
        else "N/A"
    )

    return {
        "Total Revenue": f"${total_revenue:,.2f}",
        "Profit Margin %": f"{profit_margin:.2f}%",
        "YoY Growth": yoy_growth,
        "Top Region": str(top_region),
    }


def generate_insights(df: pd.DataFrame) -> list[str]:
    insights: list[str] = []

    region_summary = df.groupby("region", as_index=False)["sales"].sum()
    if not region_summary.empty:
        top_region = region_summary.sort_values("sales", ascending=False).iloc[0]
        share = (top_region["sales"] / df["sales"].sum() * 100) if df["sales"].sum() else 0
        insights.append(
            f"{top_region['region']} leads all regions with ${top_region['sales']:,.0f} in revenue, contributing {share:.1f}% of total sales."
        )

    segment_summary = (
        df.groupby("customer_segment", as_index=False)
        .agg(revenue=("sales", "sum"), profit=("profit", "sum"))
        .sort_values("revenue", ascending=False)
    )
    if not segment_summary.empty:
        top_segment = segment_summary.iloc[0]
        insights.append(
            f"{top_segment['customer_segment']} is the highest-grossing segment at ${top_segment['revenue']:,.0f}, with ${top_segment['profit']:,.0f} in profit."
        )

    discount_mask = df["discount"] > 0.4
    high_discount_margin = (
        df.loc[discount_mask, "profit"].sum() / df.loc[discount_mask, "sales"].sum() * 100
        if discount_mask.any() and df.loc[discount_mask, "sales"].sum() != 0
        else 0
    )
    normal_discount_margin = (
        df.loc[~discount_mask, "profit"].sum() / df.loc[~discount_mask, "sales"].sum() * 100
        if (~discount_mask).any() and df.loc[~discount_mask, "sales"].sum() != 0
        else 0
    )
    insights.append(
        f"Orders with discounts above 40% deliver a {high_discount_margin:.1f}% margin versus {normal_discount_margin:.1f}% for the rest of the portfolio."
    )

    return insights[:3]


def generate_recommendations(df: pd.DataFrame) -> list[str]:
    recommendations: list[str] = []

    high_discount = df.loc[df["discount"] > 0.4]
    if not high_discount.empty:
        high_margin = (
            high_discount["profit"].sum() / high_discount["sales"].sum() * 100
            if high_discount["sales"].sum() != 0
            else 0
        )
        recommendations.append(
            f"Reduce or approve discounts above 40% more selectively, since this bucket is generating only {high_margin:.1f}% profit margin."
        )
    else:
        recommendations.append(
            "Keep current discount guardrails in place because no orders exceeded the 40% high-discount threshold."
        )

    segment_summary = (
        df.groupby("customer_segment", as_index=False)
        .agg(revenue=("sales", "sum"), profit=("profit", "sum"))
    )
    if not segment_summary.empty:
        segment_summary["margin"] = (
            segment_summary["profit"] / segment_summary["revenue"] * 100
        ).fillna(0)
        weakest_segment = segment_summary.sort_values("margin").iloc[0]
        strongest_segment = segment_summary.sort_values("revenue", ascending=False).iloc[0]
        recommendations.append(
            f"Focus segment-specific pricing and retention plans on {weakest_segment['customer_segment']}, while scaling the playbook that is already driving revenue in {strongest_segment['customer_segment']}."
        )

    return recommendations[:2]


def build_pdf(
    pdf_path: Path,
    report_title: str,
    metrics: dict[str, str],
    insights: list[str],
    recommendations: list[str],
    chart_path: Path,
) -> None:
    logging.info("Building PDF report at %s", pdf_path)
    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=letter,
        leftMargin=0.45 * inch,
        rightMargin=0.45 * inch,
        topMargin=0.4 * inch,
        bottomMargin=0.35 * inch,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Heading1"],
        fontSize=17,
        leading=20,
        textColor=colors.HexColor("#1f3b2d"),
        spaceAfter=8,
    )
    section_style = ParagraphStyle(
        "SectionTitle",
        parent=styles["Heading2"],
        fontSize=11,
        leading=13,
        textColor=colors.HexColor("#2f6b4f"),
        spaceBefore=4,
        spaceAfter=4,
    )
    body_style = ParagraphStyle(
        "BodyCopy",
        parent=styles["BodyText"],
        fontSize=8.4,
        leading=10.2,
        textColor=colors.black,
        spaceAfter=2,
    )

    story = [Paragraph(report_title, title_style), Spacer(1, 0.04 * inch)]

    story.append(Paragraph("Key Metrics", section_style))
    metric_rows = [["Metric", "Value"]] + [[key, value] for key, value in metrics.items()]
    metric_table = Table(metric_rows, colWidths=[2.1 * inch, 1.55 * inch])
    metric_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dbe8df")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#1f3b2d")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#9bb7a7")),
                ("FONTSIZE", (0, 0), (-1, -1), 8.3),
                ("LEADING", (0, 0), (-1, -1), 9),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(metric_table)
    story.append(Spacer(1, 0.06 * inch))

    story.append(Paragraph("Top 3 Insights", section_style))
    for insight in insights:
        story.append(Paragraph(f"• {insight}", body_style))

    story.append(Spacer(1, 0.04 * inch))
    story.append(Paragraph("Recommendations", section_style))
    for recommendation in recommendations:
        story.append(Paragraph(f"• {recommendation}", body_style))

    story.append(Spacer(1, 0.08 * inch))
    story.append(Image(str(chart_path), width=6.8 * inch, height=2.7 * inch))
    doc.build(story)


def main() -> None:
    configure_logging()

    if not DB_PATH.exists():
        logging.error("Database not found: %s", DB_PATH)
        sys.exit(1)

    try:
        sales_df = load_sales_data(DB_PATH)
        if not CHART_PATH.exists():
            create_monthly_revenue_chart(sales_df, CHART_PATH)

        today = date.today()
        report_title = (
            f"Sales Performance Analysis Report - {month_name[today.month]} {today.year}"
        )
        pdf_name = f"sales_report_{today.isoformat()}.pdf"
        pdf_path = OUTPUT_DIR / pdf_name

        metrics = compute_key_metrics(sales_df)
        insights = generate_insights(sales_df)
        recommendations = generate_recommendations(sales_df)
        build_pdf(pdf_path, report_title, metrics, insights, recommendations, CHART_PATH)
        logging.info("Sales summary report created successfully: %s", pdf_path)
    except Exception as exc:
        logging.exception("Report generation failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
