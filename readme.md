# Phase 1: Stock Data Labeling Pipeline

This project implements Phase 1 of a stock momentum analysis model. It provides tools for loading stock price data, preprocessing it, and generating Strong BUY/BUY labels based on future price movements.

## Overview

The Phase 1 implementation consists of three main modules:

1. **data_loader.py**: Loads stock price data from J-Quants API or CSV files
2. **preprocess.py**: Preprocesses data with gap filling and data type organization
3. **label_maker.py**: Generates Strong BUY (+25% in 10 business days) and BUY (+18% in 15 business days) labels

## Requirements

- Python 3.8 or higher
- Required packages (see `requirements.txt`):
  - pandas
  - numpy
  - requests
  - python-dotenv

## Installation

1. **Create a virtual environment** (recommended):
   ```bash
   python -m venv .venv
   
   # On Windows:
   .venv\Scripts\activate
   
   # On Linux/Mac:
   source .venv/bin/activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up J-Quants API key** (if using API):
   - Create a `.env` file in the project root
   - Add your API key:
     ```
     JQUANTS_API_KEY=your_api_key_here
     ```
   - If you have a sample CSV file, you can skip this step

## Usage

### Basic Usage

Run the complete pipeline using the test script:

```bash
python test_phase.py <code> <start_date> <end_date> [csv_path]
```

**Example using J-Quants API:**
```bash

```

**Example using CSV file:**
```bash
python test_phase.py 7203 2025-01-01 2025-06-30 sample_stock.csv
```

### Individual Module Usage

#### 1. Data Loader (`data_loader.py`)

Load stock data from J-Quants API or CSV:

```python
from data_loader import load_stock_data

# From API
df = load_stock_data("7203", "2024-01-01", "2024-06-30")

# From CSV
df = load_stock_data("7203", "2024-01-01", "2024-06-30", csv_path="data.csv")
```

**Output format:**
- DataFrame with columns: `date`, `code`, `open`, `high`, `low`, `close`, `volume`

#### 2. Preprocessing (`preprocess.py`)

Preprocess the loaded data:

```python
from preprocess import preprocess_data, validate_data

# Preprocess
df_processed = preprocess_data(df)

# Validate
is_valid, issues = validate_data(df_processed)
```

**Features:**
- Converts date column to datetime
- Gap filling (forward fill, backward fill)
- Data type organization
- Duplicate removal
- Data validation

#### 3. Label Maker (`label_maker.py`)

Generate Strong BUY and BUY labels:

```python
from label_maker import generate_labels

df_labeled = generate_labels(df_processed)
```

**Label definitions:**
- **Strong BUY (+25%)**: Highest price from t0 to t0+10 business days >= +25% → label = 1, else 0
- **BUY (+18%)**: Highest price from t0 to t0+15 business days >= +18% → label = 1, else 0

**Output:**
- DataFrame with added columns: `buystrong_label`, `buy_label` (0 or 1)

### Complete Pipeline Example

```python
from data_loader import load_stock_data
from preprocess import preprocess_data
from label_maker import generate_labels

# 1. Load data
df = load_stock_data("7203", "2024-01-01", "2024-06-30", csv_path="sample.csv")

# 2. Preprocess
df = preprocess_data(df)

# 3. Generate labels
df = generate_labels(df)

# 4. View results
print(df[['date', 'close', 'buystrong_label', 'buy_label']].head(20))
```

## CSV File Format

If using a CSV file, it should contain the following columns (case-insensitive):
- `date` or `Date`: Date in YYYY-MM-DD format
- `open` or `Open` or `open_price`: Opening price
- `high` or `High` or `high_price`: Highest price
- `low` or `Low` or `low_price`: Lowest price
- `close` or `Close` or `close_price`: Closing price
- `volume` or `Volume` or `trading_volume`: Trading volume
- `code` or `Code` (optional): Stock code

## Output Format

The final DataFrame contains:
- **Original columns**: `date`, `code`, `open`, `high`, `low`, `close`, `volume`
- **Label columns**: `buystrong_label`, `buy_label` (0 or 1)

## Notes

- All dates are converted to datetime format
- Data is sorted by date automatically
- Missing values are filled using forward fill and backward fill
- Labels are based on **business days** (trading days), not calendar days
- The label generation looks at the **highest price** (high) within the future window, not the closing price

## Troubleshooting

### J-Quants API Issues
- Ensure your API key is set in the `.env` file
- Check that the API key has proper permissions
- Verify the stock code format (4-digit code for Japanese stocks)

### CSV Loading Issues
- Ensure the CSV file has the required columns
- Check that date format is YYYY-MM-DD
- Verify numeric columns don't have non-numeric values

### Label Generation Issues
- Ensure you have enough future data (at least 15 business days after the last date you want to label)
- Check that price data is valid (no negative or zero prices)

## Next Steps (Phase 2+)

After Phase 1, the project will continue with:
- Feature engineering (RSI, volume change rate, EPS growth rate, etc.)
- Model construction (LightGBM/XGBoost)
- Model evaluation and tuning
- Self-learning/retraining functionality

## License

This project is part of a stock momentum analysis model development project.
