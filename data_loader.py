"""
Data loader module for fetching stock price data from J-Quants API or CSV files.

This module provides functions to load stock price data (opening, high, low, closing prices, and volume)
from either J-Quants API or CSV files, and returns a standardized DataFrame format.
"""

import os
import pandas as pd
import requests
from dotenv import load_dotenv
from typing import Optional

load_dotenv()


def load_data_from_csv(csv_path: str, code: Optional[str] = None) -> pd.DataFrame:
    """
    Load stock price data from a CSV file.
    
    Args:
        csv_path: Path to the CSV file
        code: Stock code (if not in CSV, will be extracted from filename or set to None)
    
    Returns:
        DataFrame with columns: date, code, open, high, low, close, volume
    """
    df = pd.read_csv(csv_path)
    
    # Standardize column names (handle various possible column name formats)
    column_mapping = {
        'Date': 'date', 'DATE': 'date', 'date': 'date',
        'Open': 'open', 'OPEN': 'open', 'open': 'open', 'open_price': 'open',
        'High': 'high', 'HIGH': 'high', 'high': 'high', 'high_price': 'high',
        'Low': 'low', 'LOW': 'low', 'low': 'low', 'low_price': 'low',
        'Close': 'close', 'CLOSE': 'close', 'close': 'close', 'close_price': 'close',
        'Volume': 'volume', 'VOLUME': 'volume', 'volume': 'volume', 'trading_volume': 'volume',
        'Code': 'code', 'CODE': 'code', 'code': 'code', 'stock_code': 'code'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Ensure required columns exist
    required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in CSV: {missing_columns}")
    
    # Add code column if not present
    if 'code' not in df.columns:
        if code:
            df['code'] = code
        else:
            # Try to extract from filename or set to None
            df['code'] = None
    
    # Select and reorder columns
    df = df[['date', 'code', 'open', 'high', 'low', 'close', 'volume']].copy()
    
    return df


def fetch_data_from_api(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch stock price data from J-Quants API.
    
    Args:
        code: Stock code (e.g., "7203")
        start_date: Start date in format "YYYY-MM-DD"
        end_date: End date in format "YYYY-MM-DD"
    
    Returns:
        DataFrame with columns: date, code, open, high, low, close, volume
    
    Raises:
        ValueError: If API key is missing
        Exception: If API request fails
    """
    base_url = "https://api.jquants.com/v1/prices/daily_quotes"
    api_key = os.getenv("JQUANTS_API_KEY")
    if not api_key:
        raise ValueError("J-Quants API key missing. Please set JQUANTS_API_KEY in environment variables or .env file.")
    
    params = {
        "code": code,
        "date_from": start_date,
        "date_to": end_date
    }
    
    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    
    try:
        response = requests.get(base_url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Handle different possible response structures
        if 'daily_quotes' in data:
            df = pd.DataFrame(data['daily_quotes'])
        elif 'data' in data:
            df = pd.DataFrame(data['data'])
        else:
            df = pd.DataFrame(data)
        
        if df.empty:
            raise ValueError(f"No data returned for code {code} from {start_date} to {end_date}")
        
        # Standardize column names
        column_mapping = {
            "Date": "date", "date": "date",
            "Open": "open", "open": "open", "open_price": "open", "OpeningPrice": "open",
            "High": "high", "high": "high", "high_price": "high", "HighPrice": "high",
            "Low": "low", "low": "low", "low_price": "low", "LowPrice": "low",
            "Close": "close", "close": "close", "close_price": "close", "ClosingPrice": "close",
            "Volume": "volume", "volume": "volume", "trading_volume": "volume", "TradingVolume": "volume"
        }
        
        df = df.rename(columns=column_mapping)
        
        # Add code column
        df['code'] = code
        
        # Select and reorder columns
        df = df[['date', 'code', 'open', 'high', 'low', 'close', 'volume']].copy()
        
        return df
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching data from J-Quants API: {str(e)}")


def load_stock_data(code: str, start_date: str, end_date: str, 
                    csv_path: Optional[str] = None) -> pd.DataFrame:
    """
    Main function to load stock data from either CSV file or J-Quants API.
    
    Args:
        code: Stock code (e.g., "7203")
        start_date: Start date in format "YYYY-MM-DD"
        end_date: End date in format "YYYY-MM-DD"
        csv_path: Optional path to CSV file. If provided, loads from CSV instead of API.
    
    Returns:
        DataFrame with columns: date, code, open, high, low, close, volume
    """
    if csv_path and os.path.exists(csv_path):
        print(f"Loading data from CSV: {csv_path}")
        df = load_data_from_csv(csv_path, code=code)
    else:
        print(f"Fetching data from J-Quants API for code {code} from {start_date} to {end_date}")
        df = fetch_data_from_api(code, start_date, end_date)
    
    # Ensure date column is datetime type
    df['date'] = pd.to_datetime(df['date'])
    
    # Sort by date
    df = df.sort_values(by='date').reset_index(drop=True)
    
    return df


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) >= 4:
        code = sys.argv[1]
        start_date = sys.argv[2]
        end_date = sys.argv[3]
        csv_path = sys.argv[4] if len(sys.argv) > 4 else None
        
        df = load_stock_data(code, start_date, end_date, csv_path)
        print(f"\nLoaded {len(df)} records")
        print(df.head())
        print(f"\nDate range: {df['date'].min()} to {df['date'].max()}")
    else:
        print("Usage: python data_loader.py <code> <start_date> <end_date> [csv_path]")
        print("Example: python data_loader.py 7203 2024-01-01 2024-06-30")
