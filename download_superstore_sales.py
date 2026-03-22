#!/usr/bin/env python3
"""Download a Superstore Sales dataset from Kaggle and inspect the CSV."""

from __future__ import annotations

import os
import shutil
import sys
import zipfile
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd

load_dotenv()

try:
    from kaggle.api.kaggle_api_extended import KaggleApi
except ImportError:
    print(
        "Error: The 'kaggle' package is not installed.\n"
        "Install it with: pip install kaggle",
        file=sys.stderr,
    )
    sys.exit(1)


SEARCH_TERM = "Superstore Sales"
OUTPUT_DIR = Path("data")
OUTPUT_CSV = OUTPUT_DIR / "superstore_sales.csv"
MAX_DATASET_CANDIDATES = 10
CSV_ENCODINGS = ["utf-8", "utf-8-sig", "cp1252", "latin1"]

REQUIRED_COLUMN_ALIASES = {
    "Order ID": ["Order ID"],
    "Order Date": ["Order Date"],
    "Ship Date": ["Ship Date"],
    "Customer Segment": ["Customer Segment", "Segment"],
    "Product Category": ["Product Category", "Category"],
    "Sales": ["Sales"],
    "Profit": ["Profit"],
    "Quantity": ["Quantity"],
    "Discount": ["Discount"],
    "Region": ["Region"],
}


def kaggle_credentials_present() -> bool:
    """Check common Kaggle API credential locations."""
    env_has_credentials = bool(
        os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY")
    )
    json_path = Path.home() / ".kaggle" / "kaggle.json"
    return env_has_credentials or json_path.exists()


def list_dataset_candidates(api: KaggleApi) -> list[str]:
    """Return likely Kaggle dataset slugs for Superstore Sales."""
    datasets = api.dataset_list(search=SEARCH_TERM)
    if not datasets:
        raise RuntimeError(f"No Kaggle datasets found for search term: {SEARCH_TERM!r}")

    preferred = [
        ds
        for ds in datasets
        if "superstore" in ds.title.lower() and "sales" in ds.title.lower()
    ]
    ordered = preferred if preferred else datasets
    return [dataset.ref for dataset in ordered[:MAX_DATASET_CANDIDATES]]


def extract_csv_from_zip(zip_path: Path, extract_dir: Path) -> Path:
    """Extract CSV files and return the most likely main dataset file."""
    with zipfile.ZipFile(zip_path, "r") as zip_file:
        zip_file.extractall(extract_dir)

    csv_files = list(extract_dir.rglob("*.csv"))
    if not csv_files:
        raise RuntimeError("Downloaded dataset did not contain any CSV files.")

    prioritized = [
        path
        for path in csv_files
        if "superstore" in path.name.lower() or "sales" in path.name.lower()
    ]
    if prioritized:
        return max(prioritized, key=lambda path: path.stat().st_size)
    return max(csv_files, key=lambda path: path.stat().st_size)


def read_csv_with_fallback(csv_path: Path) -> pd.DataFrame:
    """Read a CSV using a small set of common fallback encodings."""
    last_error: UnicodeDecodeError | None = None
    for encoding in CSV_ENCODINGS:
        try:
            return pd.read_csv(csv_path, encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
            continue

    raise RuntimeError(
        f"Unable to decode CSV {csv_path} using encodings: {', '.join(CSV_ENCODINGS)}"
    ) from last_error


def build_column_rename_map(df: pd.DataFrame) -> dict[str, str]:
    """Map dataset-specific column names to the canonical ETL schema."""
    rename_map: dict[str, str] = {}
    for canonical_name, aliases in REQUIRED_COLUMN_ALIASES.items():
        matched_column = next((alias for alias in aliases if alias in df.columns), None)
        if matched_column is None:
            raise ValueError(f"Missing required column for {canonical_name!r}")
        rename_map[matched_column] = canonical_name
    return rename_map


def prepare_dataset_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize columns to the schema required by the ETL pipeline."""
    rename_map = build_column_rename_map(df)
    prepared_df = df.rename(columns=rename_map)
    ordered_columns = list(REQUIRED_COLUMN_ALIASES.keys())
    return prepared_df[ordered_columns].copy()


def try_download_dataset(
    api: KaggleApi, dataset_slug: str, download_root: Path
) -> pd.DataFrame | None:
    """Download one Kaggle dataset candidate and return a validated DataFrame."""
    slug_dir = download_root / dataset_slug.replace("/", "__")
    if slug_dir.exists():
        shutil.rmtree(slug_dir)
    slug_dir.mkdir(parents=True, exist_ok=True)

    api.dataset_download_files(
        dataset_slug,
        path=str(slug_dir),
        unzip=False,
        quiet=False,
    )

    zip_path = slug_dir / f"{dataset_slug.split('/')[-1]}.zip"
    if not zip_path.exists():
        zip_candidates = list(slug_dir.glob("*.zip"))
        if not zip_candidates:
            raise RuntimeError(
                f"Kaggle download completed for {dataset_slug}, but no ZIP file was found."
            )
        zip_path = max(zip_candidates, key=lambda path: path.stat().st_mtime)

    extracted_csv = extract_csv_from_zip(zip_path, slug_dir)
    raw_df = read_csv_with_fallback(extracted_csv)
    try:
        prepared_df = prepare_dataset_frame(raw_df)
    except ValueError as exc:
        print(f"Skipping dataset {dataset_slug}: {exc}")
        return None

    print(f"Selected dataset {dataset_slug} with compatible schema.")
    return prepared_df


def main() -> None:
    if not kaggle_credentials_present():
        print(
            "Error: Kaggle API credentials not found.\n"
            "Set KAGGLE_USERNAME and KAGGLE_KEY environment variables, or place\n"
            "your kaggle.json file at ~/.kaggle/kaggle.json.",
            file=sys.stderr,
        )
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    download_dir = OUTPUT_DIR / "kaggle_download"
    download_dir.mkdir(parents=True, exist_ok=True)

    try:
        api = KaggleApi()
        api.authenticate()

        dataset_candidates = list_dataset_candidates(api)
        df = None
        for dataset_slug in dataset_candidates:
            print(f"Checking dataset: {dataset_slug}")
            df = try_download_dataset(api, dataset_slug, download_dir)
            if df is not None:
                break

        if df is None:
            raise RuntimeError(
                "No compatible Kaggle dataset was found with the full required schema: "
                "Order ID, Order Date, Ship Date, Customer Segment/Segment, "
                "Product Category/Category, Sales, Profit, Quantity, Discount, Region."
            )

        df.to_csv(OUTPUT_CSV, index=False)

        print(f"\nSaved CSV to: {OUTPUT_CSV.resolve()}")
        print(f"Shape: {df.shape}")
        print("\nColumn names:")
        print(df.columns.tolist())
        print("\nData types:")
        print(df.dtypes)
        print("\nFirst 5 rows:")
        print(df.head())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
