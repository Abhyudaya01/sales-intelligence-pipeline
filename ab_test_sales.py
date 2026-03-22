#!/usr/bin/env python3
"""Run an A/B test on sales between Consumer and Corporate segments."""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy import stats


DB_PATH = Path("sales_pipeline.db")
TABLE_NAME = "sales_validated"
OUTPUT_PATH = Path("ab_test_result.png")
GROUP_A = "Consumer"
GROUP_B = "Corporate"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_groups(db_path: Path) -> tuple[pd.Series, pd.Series]:
    logging.info("Loading sales data from %s (%s)", db_path, TABLE_NAME)
    query = f"""
        SELECT customer_segment, sales
        FROM {TABLE_NAME}
        WHERE customer_segment IN (?, ?)
          AND sales IS NOT NULL
    """
    with sqlite3.connect(db_path) as connection:
        df = pd.read_sql_query(query, connection, params=(GROUP_A, GROUP_B))

    group_a = df.loc[df["customer_segment"] == GROUP_A, "sales"].astype(float)
    group_b = df.loc[df["customer_segment"] == GROUP_B, "sales"].astype(float)

    if len(group_a) < 2 or len(group_b) < 2:
        raise ValueError("Both groups must contain at least 2 sales records.")

    return group_a, group_b


def cohens_d(group_a: pd.Series, group_b: pd.Series) -> float:
    mean_diff = group_a.mean() - group_b.mean()
    var_a = group_a.var(ddof=1)
    var_b = group_b.var(ddof=1)
    pooled_std = np.sqrt(
        ((len(group_a) - 1) * var_a + (len(group_b) - 1) * var_b)
        / (len(group_a) + len(group_b) - 2)
    )
    if pooled_std == 0:
        return 0.0
    return mean_diff / pooled_std


def welch_confidence_interval(
    group_a: pd.Series, group_b: pd.Series, confidence: float = 0.95
) -> tuple[float, float]:
    mean_diff = group_a.mean() - group_b.mean()
    var_a = group_a.var(ddof=1) / len(group_a)
    var_b = group_b.var(ddof=1) / len(group_b)
    standard_error = np.sqrt(var_a + var_b)

    numerator = (var_a + var_b) ** 2
    denominator = (var_a**2 / (len(group_a) - 1)) + (var_b**2 / (len(group_b) - 1))
    degrees_of_freedom = numerator / denominator

    alpha = 1 - confidence
    critical_value = stats.t.ppf(1 - alpha / 2, degrees_of_freedom)
    margin = critical_value * standard_error
    return mean_diff - margin, mean_diff + margin


def run_ab_test(group_a: pd.Series, group_b: pd.Series) -> dict[str, float]:
    logging.info("Running Welch two-sample t-test for %s vs %s", GROUP_A, GROUP_B)
    t_statistic, p_value = stats.ttest_ind(group_a, group_b, equal_var=False)
    ci_low, ci_high = welch_confidence_interval(group_a, group_b)

    return {
        "group_a_mean": float(group_a.mean()),
        "group_b_mean": float(group_b.mean()),
        "t_statistic": float(t_statistic),
        "p_value": float(p_value),
        "cohens_d": float(cohens_d(group_a, group_b)),
        "ci_low": float(ci_low),
        "ci_high": float(ci_high),
    }


def plot_distributions(group_a: pd.Series, group_b: pd.Series, output_path: Path) -> None:
    logging.info("Saving A/B test histogram to %s", output_path)
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(10, 6))

    sns.histplot(group_a, bins=30, stat="density", alpha=0.45, label=GROUP_A, color="#1f77b4")
    sns.histplot(group_b, bins=30, stat="density", alpha=0.45, label=GROUP_B, color="#ff7f0e")

    plt.axvline(group_a.mean(), color="#1f77b4", linestyle="--", linewidth=2, label=f"{GROUP_A} Mean")
    plt.axvline(group_b.mean(), color="#ff7f0e", linestyle="--", linewidth=2, label=f"{GROUP_B} Mean")

    plt.title("A/B Test: Sales Distribution by Customer Segment")
    plt.xlabel("Sales")
    plt.ylabel("Density")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    plt.close()


def print_summary(results: dict[str, float]) -> None:
    is_significant = results["p_value"] < 0.05
    print(f"Group A ({GROUP_A}) mean sales: {results['group_a_mean']:.2f}")
    print(f"Group B ({GROUP_B}) mean sales: {results['group_b_mean']:.2f}")
    print(f"t-statistic: {results['t_statistic']:.4f}")
    print(f"p-value: {results['p_value']:.6f}")
    print(f"Cohen's d: {results['cohens_d']:.4f}")
    print(
        "95% confidence interval for mean difference "
        f"({GROUP_A} - {GROUP_B}): [{results['ci_low']:.4f}, {results['ci_high']:.4f}]"
    )
    print(
        "Is the difference statistically significant? "
        f"{'Yes' if is_significant else 'No'}"
    )


def main() -> None:
    configure_logging()

    if not DB_PATH.exists():
        logging.error("Database not found: %s", DB_PATH)
        sys.exit(1)

    try:
        group_a, group_b = load_groups(DB_PATH)
        results = run_ab_test(group_a, group_b)
        plot_distributions(group_a, group_b, OUTPUT_PATH)
        print_summary(results)
        logging.info("A/B test completed successfully")
    except Exception as exc:
        logging.exception("A/B test failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
