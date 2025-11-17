"""
Preprocessing module for stock price data.

This module provides functions for:
- Gap filling (forward fill, backward fill)
- Data type organization (date conversion, numeric types)
- Basic data cleaning and validation
"""

import pandas as pd
import numpy as np
from typing import Optional, Tuple, List


def preprocess_data(df: pd.DataFrame, 
                   fill_method: str = 'both',
                   drop_duplicates: bool = True,
                   sort_by_date: bool = True) -> pd.DataFrame:
    """
    Preprocess stock price data with gap filling and data type organization.
    
    Args:
        df: DataFrame with stock price data
        fill_method: Gap filling method - 'forward', 'backward', 'both', or 'none'
        drop_duplicates: Whether to drop duplicate rows based on date
        sort_by_date: Whether to sort by date
    
    Returns:
        Preprocessed DataFrame
    """
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # Convert date column to datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Sort by date if requested
    if sort_by_date and 'date' in df.columns:
        df = df.sort_values(by='date').reset_index(drop=True)
    
    # Drop duplicate dates (keep first occurrence)
    if drop_duplicates and 'date' in df.columns:
        df = df.drop_duplicates(subset=['date'], keep='first').reset_index(drop=True)
    
    # Ensure numeric columns are numeric
    numeric_columns = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Gap filling for numeric columns
    if fill_method in ['forward', 'both']:
        # Forward fill: use previous valid value
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].ffill()
    
    if fill_method in ['backward', 'both']:
        # Backward fill: use next valid value (for leading NaN values)
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].bfill()
    
    # Validate data: ensure high >= low, high >= open, high >= close, etc.
    if all(col in df.columns for col in ['high', 'low', 'open', 'close']):
        # Fix invalid price relationships
        df['high'] = df[['high', 'open', 'close', 'low']].max(axis=1)
        df['low'] = df[['high', 'open', 'close', 'low']].min(axis=1)
    
    # Ensure volume is non-negative
    if 'volume' in df.columns:
        df['volume'] = df['volume'].clip(lower=0)
    
    return df


def validate_data(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate the preprocessed data for common issues.
    
    Args:
        df: DataFrame to validate
    
    Returns:
        Tuple of (is_valid, list_of_issues)
    """
    issues = []
    
    # Check required columns
    required_columns = ['date', 'code', 'open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        issues.append(f"Missing required columns: {missing_columns}")
    
    # Check for NaN values in critical columns
    critical_columns = ['date', 'open', 'high', 'low', 'close']
    for col in critical_columns:
        if col in df.columns:
            nan_count = df[col].isna().sum()
            if nan_count > 0:
                issues.append(f"Column '{col}' has {nan_count} NaN values")
    
    # Check date column
    if 'date' in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            issues.append("Date column is not datetime type")
        
        # Check for duplicate dates
        duplicate_dates = df['date'].duplicated().sum()
        if duplicate_dates > 0:
            issues.append(f"Found {duplicate_dates} duplicate dates")
    
    # Check price relationships
    if all(col in df.columns for col in ['high', 'low', 'open', 'close']):
        invalid_high = (df['high'] < df[['open', 'close', 'low']].max(axis=1)).sum()
        invalid_low = (df['low'] > df[['open', 'close', 'high']].min(axis=1)).sum()
        
        if invalid_high > 0:
            issues.append(f"Found {invalid_high} rows where high < max(open, close, low)")
        if invalid_low > 0:
            issues.append(f"Found {invalid_low} rows where low > min(open, close, high)")
    
    # Check for negative prices
    price_columns = ['open', 'high', 'low', 'close']
    for col in price_columns:
        if col in df.columns:
            negative_count = (df[col] < 0).sum()
            if negative_count > 0:
                issues.append(f"Column '{col}' has {negative_count} negative values")
    
    is_valid = len(issues) == 0
    return is_valid, issues


if __name__ == "__main__":
    # Example usage
    import sys
    from data_loader import load_stock_data
    
    if len(sys.argv) >= 4:
        code = sys.argv[1]
        start_date = sys.argv[2]
        end_date = sys.argv[3]
        csv_path = sys.argv[4] if len(sys.argv) > 4 else None
        
        # Load data
        df = load_stock_data(code, start_date, end_date, csv_path)
        
        print(f"\nBefore preprocessing:")
        print(f"Shape: {df.shape}")
        print(f"NaN counts:\n{df.isna().sum()}")
        
        # Preprocess
        df_processed = preprocess_data(df)
        
        print(f"\nAfter preprocessing:")
        print(f"Shape: {df_processed.shape}")
        print(f"NaN counts:\n{df_processed.isna().sum()}")
        
        # Validate
        is_valid, issues = validate_data(df_processed)
        if is_valid:
            print("\n✓ Data validation passed")
        else:
            print(f"\n✗ Data validation found issues:")
            for issue in issues:
                print(f"  - {issue}")
        
        print(f"\nSample data:")
        print(df_processed.head())
    else:
        print("Usage: python preprocess.py <code> <start_date> <end_date> [csv_path]")
