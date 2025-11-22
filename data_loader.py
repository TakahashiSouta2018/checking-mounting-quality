"""
Data loader module for fetching stock price data from J-Quants API or CSV files.

This module provides functions to load stock price data (opening, high, low, closing prices, and volume)
from either J-Quants API or CSV files, and returns a standardized DataFrame format.
"""

import os
import pandas as pd
import requests
from dotenv import load_dotenv
from typing import Optional, Dict, Any

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


class JQuantsAuthError(Exception):
    """Custom exception for J-Quants authentication issues."""


def _request_json(url: str, payload: Dict[str, Any], timeout: int = 30, use_json: bool = True) -> Dict[str, Any]:
    """Helper to POST JSON payloads and return parsed responses."""
    headers = {"Content-Type": "application/json"} if use_json else {}
    if use_json:
        response = requests.post(url, json=payload, headers=headers, timeout=timeout)
    else:
        # Try as form data
        response = requests.post(url, data=payload, headers=headers, timeout=timeout)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        # Try to get error details from response
        error_details = ""
        try:
            error_body = response.json()
            error_details = f" Response: {error_body}"
        except:
            error_details = f" Response text: {response.text[:200]}"
        raise requests.exceptions.HTTPError(f"{exc}{error_details}") from exc
    try:
        return response.json()
    except ValueError as exc:
        raise JQuantsAuthError(f"Invalid JSON response from {url}") from exc


def _obtain_refresh_token(email: Optional[str], password: Optional[str]) -> str:
    """
    Request a new refresh token using email/password if one is not provided.
    """
    if not email or not password:
        raise JQuantsAuthError(
            "J-Quants refresh token missing. Provide JQUANTS_REFRESH_TOKEN or "
            "both JQUANTS_EMAIL and JQUANTS_PASSWORD in your environment."
        )

    auth_user_url = "https://api.jquants.com/v1/token/auth_user"
    # J-Quants API expects "mailaddress" not "email" based on API error messages
    data = _request_json(auth_user_url, {"mailaddress": email, "password": password})
    refresh_token = data.get("refreshToken") or data.get("refreshtoken")
    if not refresh_token:
        raise JQuantsAuthError("Unable to obtain refresh token from auth_user endpoint.")
    return refresh_token


def get_jquants_id_token() -> str:
    """
    Issue an idToken from RefreshToken to comply with the current J-Quants spec.

    Returns:
        idToken string to be used as Bearer token.
    
    Raises:
        JQuantsAuthError: If refresh token is missing or invalid
    """
    email = os.getenv("JQUANTS_EMAIL") or os.getenv("JQUANTS_EMAILADDRESS")
    password = os.getenv("JQUANTS_PASSWORD")
    refresh_token = os.getenv("JQUANTS_REFRESH_TOKEN")

    # Strategy: Try refresh token first, fall back to email/password if it fails
    use_refresh_token = refresh_token and refresh_token.strip() != ""
    
    if use_refresh_token:
        # Clean and validate refresh token
        refresh_token = refresh_token.strip().strip('"').strip("'").strip()
        refresh_token = refresh_token.replace('\n', '').replace('\r', '')
        
        if not refresh_token:
            use_refresh_token = False
        elif len(refresh_token) < 10:
            use_refresh_token = False
    
    # If no valid refresh token, try to obtain one from email/password
    if not use_refresh_token:
        if not email or not password:
            raise JQuantsAuthError(
                "J-Quants authentication failed. Please provide either:\n"
                "  - JQUANTS_REFRESH_TOKEN in .env file, OR\n"
                "  - Both JQUANTS_EMAIL and JQUANTS_PASSWORD in .env file"
            )
        try:
            refresh_token = _obtain_refresh_token(email, password)
            use_refresh_token = True
        except Exception as e:
            raise JQuantsAuthError(
                f"Failed to obtain refresh token from email/password: {e}\n"
                "Please check your JQUANTS_EMAIL and JQUANTS_PASSWORD in .env file."
            ) from e


    auth_refresh_url = "https://api.jquants.com/v1/token/auth_refresh"
    
    # Validate refresh token
    if not isinstance(refresh_token, str) or len(refresh_token) == 0:
        raise JQuantsAuthError(
            f"Invalid refresh token format. Token type: {type(refresh_token)}, "
            f"Length: {len(refresh_token) if refresh_token else 0}"
        )
    
    # J-Quants API accepts refresh token as query parameter
    try:
        response = requests.post(
            f"{auth_refresh_url}?refreshtoken={refresh_token}",
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        id_token = data.get("idToken") or data.get("idtoken")
        if id_token:
            return id_token
        raise JQuantsAuthError(
            f"Response received but no idToken found. Response keys: {list(data.keys())}"
        )
    except requests.exceptions.HTTPError as exc:
        # Get error details for better debugging
        error_details = ""
        try:
            error_body = exc.response.json()
            error_details = f" Response: {error_body}"
        except:
            if hasattr(exc, 'response'):
                error_details = f" Response text: {exc.response.text[:200]}"
        raise JQuantsAuthError(f"Unable to issue idToken from refresh token: {exc}{error_details}") from exc
    except requests.RequestException as exc:
        raise JQuantsAuthError(f"Unable to issue idToken from refresh token: {exc}") from exc
        # If refresh token from .env fails, try getting a fresh one from email/password
        error_str = str(exc)
        if "'refreshtoken' is required" in error_str or "400" in error_str:
            # The stored refresh token might be invalid/expired, try email/password as fallback
            if email and password:
                try:
                    # Get a fresh refresh token
                    fresh_refresh_token = _obtain_refresh_token(email, password)
                    # Try again with the fresh token
                    fresh_payload = {"refreshtoken": fresh_refresh_token}
                    headers = {"Content-Type": "application/json"}
                    response = requests.post(
                        auth_refresh_url,
                        json=fresh_payload,
                        headers=headers,
                        timeout=30
                    )
                    response.raise_for_status()
                    data = response.json()
                    id_token = data.get("idToken") or data.get("idtoken")
                    if id_token:
                        return id_token
                except Exception as fallback_error:
                    raise JQuantsAuthError(
                        f"Unable to issue idToken. Refresh token from .env failed: {exc}\n"
                        f"Fallback to email/password also failed: {fallback_error}"
                    ) from exc
        raise JQuantsAuthError(f"Unable to issue idToken from refresh token: {exc}") from exc
    except requests.RequestException as exc:
        raise JQuantsAuthError(f"Unable to issue idToken from refresh token: {exc}") from exc


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
    try:
        id_token = get_jquants_id_token()
    except JQuantsAuthError as exc:
        raise ValueError(f"Authentication failed: {exc}") from exc
    
    params = {
        "code": code,
        "date_from": start_date,
        "date_to": end_date
    }
    
    headers = {"Authorization": f"Bearer {id_token}"}
    
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
    
    # Filter data to the specified date range
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    # Filter to only include dates within the specified range (inclusive)
    df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)].copy()
    
    if df.empty:
        raise ValueError(
            f"No data found for code {code} in the specified date range "
            f"({start_date} to {end_date}). The API may have returned data outside this range."
        )
    
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
