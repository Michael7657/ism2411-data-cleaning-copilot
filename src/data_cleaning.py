"""
data_cleaning.py

This script loads the raw sales data, cleans it using several steps
and saves the cleaned version into the processed folder.
"""

import pandas as pd

# What: Load raw CSV file into a pandas.
# Why: Centralizes logic and keeps the main pipeline clean.
# ------------------------------------------------------------
def load_data(file_path: str):
    """Load a CSV file and return a DataFrame."""
    return pd.read_csv(file_path)

# What: Standardize column names.
# Why: Makes columns consistent and easier use.
def clean_column_names(df: pd.DataFrame):
    """Standardize column names to lowercase_with_underscores."""
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
    )
    return df


# What: Strip whitespace from text columns.
# Why: Prevents issues with spacing.
def strip_whitespace(df: pd.DataFrame):
    """Strip leading/trailing whitespace from all string columns."""
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].str.strip()
    return df

# What: Handle missing values for price and quantity.
# Why: Get consistent and correct calculations.
def handle_missing_values(df: pd.DataFrame):
    """Drop rows where price or quantity is missing."""
    df = df.dropna(subset=["price", "quantity"])
    return df

# What: Remove invalid rows.
# Why: Negative values indicate errors.
def remove_invalid_rows(df: pd.DataFrame):
    """Remove rows with negative or zero prices/quantities."""
    df = df[(df["price"] >= 0) & (df["quantity"] >= 0)]
    return df

# Main pipeline
# This block runs when you execute: python src/data_cleaning.py
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
