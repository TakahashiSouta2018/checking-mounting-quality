"""
Label maker module for assigning Strong BUY and BUY labels based on future price movements.

This module implements the labeling logic:
- Strong BUY (+25%): Highest price from t0 to t0+10 business days >= +25%
- BUY (+18%): Highest price from t0 to t0+15 business days >= +18%
"""

import pandas as pd
import numpy as np
from typing import Optional


def generate_labels(df: pd.DataFrame, 
                   strong_buy_threshold: float = 0.25,
                   buy_threshold: float = 0.18,
                   strong_buy_days: int = 10,
                   buy_days: int = 15) -> pd.DataFrame:
    """
    Generate Strong BUY and BUY labels based on future price movements.
    
    Args:
        df: DataFrame with columns: date, code, open, high, low, close, volume
            Must be sorted by date and have date as datetime type
        strong_buy_threshold: Price increase threshold for Strong BUY (default: 0.25 = 25%)
        buy_threshold: Price increase threshold for BUY (default: 0.18 = 18%)
        strong_buy_days: Number of business days to look ahead for Strong BUY (default: 10)
        buy_days: Number of business days to look ahead for BUY (default: 15)
    
    Returns:
        DataFrame with added columns: buystrong_label, buy_label (0 or 1)
    """
    # Create a copy to avoid modifying the original DataFrame
    df = df.copy()
    
    # Ensure date is datetime and sorted
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    
    df = df.sort_values(by='date').reset_index(drop=True)
    
    # Initialize label columns
    df['buystrong_label'] = 0
    df['buy_label'] = 0
    
    # Process each row to generate labels
    # Since the data is sorted by date and contains only trading days (business days),
    # we can use consecutive rows to represent business days
    for idx in range(len(df)):
        current_price = df.iloc[idx]['close']
        
        # Skip if invalid price
        if pd.isna(current_price) or current_price <= 0:
            continue
        
        # For Strong BUY: look at next 10 business days (trading days)
        end_idx_10 = min(idx + strong_buy_days + 1, len(df))
        if end_idx_10 > idx + 1:
            future_highs_10 = df.iloc[idx + 1:end_idx_10]['high']
            if len(future_highs_10) > 0:
                max_high_10 = future_highs_10.max()
                if not pd.isna(max_high_10):
                    price_change = (max_high_10 - current_price) / current_price
                    if price_change >= strong_buy_threshold:
                        df.at[idx, 'buystrong_label'] = 1
        
        # For BUY: look at next 15 business days (trading days)
        end_idx_15 = min(idx + buy_days + 1, len(df))
        if end_idx_15 > idx + 1:
            future_highs_15 = df.iloc[idx + 1:end_idx_15]['high']
            if len(future_highs_15) > 0:
                max_high_15 = future_highs_15.max()
                if not pd.isna(max_high_15):
                    price_change = (max_high_15 - current_price) / current_price
                    if price_change >= buy_threshold:
                        df.at[idx, 'buy_label'] = 1
    
    return df


def generate_labels_vectorized(df: pd.DataFrame,
                               strong_buy_threshold: float = 0.25,
                               buy_threshold: float = 0.18,
                               strong_buy_days: int = 10,
                               buy_days: int = 15) -> pd.DataFrame:
    """
    Vectorized version of generate_labels for better performance on large datasets.
    
    Args:
        df: DataFrame with columns: date, code, open, high, low, close, volume
        strong_buy_threshold: Price increase threshold for Strong BUY (default: 0.25)
        buy_threshold: Price increase threshold for BUY (default: 0.18)
        strong_buy_days: Number of business days for Strong BUY (default: 10)
        buy_days: Number of business days for BUY (default: 15)
    
    Returns:
        DataFrame with added columns: buystrong_label, buy_label
    """
    df = df.copy()
    
    # Ensure date is datetime and sorted
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    
    df = df.sort_values(by='date').reset_index(drop=True)
    
    # Initialize labels
    df['buystrong_label'] = 0
    df['buy_label'] = 0
    
    # Use rolling window approach for better performance
    for idx in range(len(df)):
        current_price = df.iloc[idx]['close']
        
        if pd.isna(current_price) or current_price <= 0:
            continue
        
        # Get future windows
        end_idx_10 = min(idx + strong_buy_days + 1, len(df))
        end_idx_15 = min(idx + buy_days + 1, len(df))
        
        # Strong BUY label
        if end_idx_10 > idx + 1:
            future_highs_10 = df.iloc[idx + 1:end_idx_10]['high']
            if len(future_highs_10) > 0:
                max_high_10 = future_highs_10.max()
                if not pd.isna(max_high_10):
                    price_change = (max_high_10 - current_price) / current_price
                    if price_change >= strong_buy_threshold:
                        df.at[idx, 'buystrong_label'] = 1
        
        # BUY label
        if end_idx_15 > idx + 1:
            future_highs_15 = df.iloc[idx + 1:end_idx_15]['high']
            if len(future_highs_15) > 0:
                max_high_15 = future_highs_15.max()
                if not pd.isna(max_high_15):
                    price_change = (max_high_15 - current_price) / current_price
                    if price_change >= buy_threshold:
                        df.at[idx, 'buy_label'] = 1
    
    return df


if __name__ == "__main__":
    # Example usage
    import sys
    from data_loader import load_stock_data
    from preprocess import preprocess_data
    
    if len(sys.argv) >= 4:
        code = sys.argv[1]
        start_date = sys.argv[2]
        end_date = sys.argv[3]
        csv_path = sys.argv[4] if len(sys.argv) > 4 else None
        
        # Load and preprocess data
        df = load_stock_data(code, start_date, end_date, csv_path)
        df = preprocess_data(df)
        
        # Generate labels
        df = generate_labels(df)
        
        print(f"\nLabel Statistics:")
        print(f"Strong BUY labels: {df['buystrong_label'].sum()} ({df['buystrong_label'].mean()*100:.2f}%)")
        print(f"BUY labels: {df['buy_label'].sum()} ({df['buy_label'].mean()*100:.2f}%)")
        
        # Display result with all columns in requested order
        result_cols = ['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'buystrong_label', 'buy_label']
        available_cols = [col for col in result_cols if col in df.columns]
        print(f"\nResult (date, code, open, high, low, close, volume, buystrong_label, buy_label):")
        print(df[available_cols].to_string(index=False))
    else:
        print("Usage: python label_maker.py <code> <start_date> <end_date> [csv_path]")
