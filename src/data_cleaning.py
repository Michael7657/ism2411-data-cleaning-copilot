"""
data_cleaning.py

This script loads the raw sales data, cleans it using several steps
and saves the cleaned version into the processed folder.
"""

import pandas as pd
# ------------------------------------------------------------
# Copilot-assisted function #1
# Purpose: Load the raw CSV file into a pandas DataFrame.
# Why: Centralizes loading logic and keeps the main pipeline clean.
# ------------------------------------------------------------
def load_data(file_path: str):
    """Load a CSV file and return a DataFrame."""
    return pd.read_csv(file_path)


# ------------------------------------------------------------
# Copilot-assisted function #2
# Purpose: Standardize column names by lowering case, trimming
# whitespace, and replacing spaces with underscores.
# Why: Makes columns consistent and easier to work with.
# ------------------------------------------------------------
def clean_column_names(df: pd.DataFrame):
    """Standardize column names to lowercase_with_underscores."""
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
    )
    return df


# ------------------------------------------------------------
# Function #3 (you can modify Copilot’s suggestion)
# Purpose: Strip whitespace from text columns.
# Why: Prevents issues where " Apple " and "Apple" look different.
# ------------------------------------------------------------
def strip_whitespace(df: pd.DataFrame):
    """Strip leading/trailing whitespace from all string columns."""
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].str.strip()
    return df


# ------------------------------------------------------------
# Function #4 (you may generate with Copilot)
# Purpose: Handle missing values for price and quantity.
# Why: Missing numeric values break calculations; decision must be consistent.
# ------------------------------------------------------------
def handle_missing_values(df: pd.DataFrame):
    """Drop rows where price or quantity is missing."""
    df = df.dropna(subset=["price", "qty"])
    return df


# ------------------------------------------------------------
# Function #5
# Purpose: Remove invalid rows such as negative price or negative quantity.
# Why: Negative values are not logically valid and indicate errors.
# ------------------------------------------------------------
def remove_invalid_rows(df):
    # Convert price and qty to numeric, coercing errors to NaN
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    df = df.dropna(subset=["price", "qty"])
    df = df[(df["price"] >= 0) & (df["qty"] >= 0)]
    return df


# ------------------------------------------------------------
# Main pipeline
# This block runs when you execute: python src/data_cleaning.py
# ------------------------------------------------------------
if __name__ == "__main__":
    raw_path = "data/raw/sales_data_raw.csv"
    cleaned_path = "data/processed/sales_data_clean.csv"

    # Load and clean data step-by-step
    df_raw = load_data(raw_path)
    df_clean = clean_column_names(df_raw)
    df_clean = strip_whitespace(df_clean)
    df_clean = handle_missing_values(df_clean)
    df_clean = remove_invalid_rows(df_clean)
    # Save cleaned data
    df_clean.to_csv(cleaned_path, index=False)
    # Preview output
    print("Cleaning complete. First few rows:")
    print(df_clean.head())
