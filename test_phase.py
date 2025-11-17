"""
Test script for Phase 1 components: data_loader, preprocess, and label_maker.

This script tests the complete pipeline:
1. Load stock data (from CSV or API)
2. Preprocess the data
3. Generate labels
"""

import sys
import pandas as pd
from data_loader import load_stock_data
from preprocess import preprocess_data, validate_data
from label_maker import generate_labels


def test_pipeline(code: str, start_date: str, end_date: str, csv_path: str = None):
    """
    Test the complete Phase 1 pipeline.
    
    Args:
        code: Stock code
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        csv_path: Optional path to CSV file
    """
    print("=" * 60)
    print("Phase 1 Pipeline Test")
    print("=" * 60)
    
    # Step 1: Load data
    print("\n[Step 1] Loading stock data...")
    try:
        df = load_stock_data(code, start_date, end_date, csv_path)
        print(f"✓ Loaded {len(df)} records")
        print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
        print(f"  Columns: {list(df.columns)}")
        print(f"\n  First few rows:")
        print(df.head())
    except Exception as e:
        print(f"✗ Error loading data: {e}")
        return
    
    # Step 2: Preprocess
    print("\n[Step 2] Preprocessing data...")
    try:
        df_processed = preprocess_data(df)
        print(f"✓ Preprocessed {len(df_processed)} records")
        
        # Validate
        is_valid, issues = validate_data(df_processed)
        if is_valid:
            print("✓ Data validation passed")
        else:
            print("⚠ Data validation found issues:")
            for issue in issues:
                print(f"  - {issue}")
        
        print(f"\n  NaN counts after preprocessing:")
        print(df_processed.isna().sum())
        
    except Exception as e:
        print(f"✗ Error preprocessing data: {e}")
        return
    
    # Step 3: Generate labels
    print("\n[Step 3] Generating labels...")
    try:
        df_labeled = generate_labels(df_processed)
        print(f"✓ Generated labels for {len(df_labeled)} records")
        
        # Label statistics
        strong_buy_count = df_labeled['buystrong_label'].sum()
        buy_count = df_labeled['buy_label'].sum()
        total = len(df_labeled)
        
        print(f"\n  Label Statistics:")
        print(f"  Strong BUY (+25% in 10 days): {strong_buy_count} ({strong_buy_count/total*100:.2f}%)")
        print(f"  BUY (+18% in 15 days): {buy_count} ({buy_count/total*100:.2f}%)")
        
        # Define result columns in the requested order
        result_cols = ['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'buystrong_label', 'buy_label']
        
        # Ensure all columns exist
        available_cols = [col for col in result_cols if col in df_labeled.columns]
        if len(available_cols) < len(result_cols):
            missing = [col for col in result_cols if col not in df_labeled.columns]
            print(f"  ⚠ Warning: Missing columns: {missing}")
        
        # Show sample records with all columns
        print(f"\n  Sample records with labels (first 20):")
        print(df_labeled[available_cols].head(20).to_string(index=False))
        
        # Show some positive labels
        strong_buy_samples = df_labeled[df_labeled['buystrong_label'] == 1]
        if len(strong_buy_samples) > 0:
            print(f"\n  Records with Strong BUY label (buystrong_label = 1):")
            print(strong_buy_samples[available_cols].to_string(index=False))
        
        buy_samples = df_labeled[df_labeled['buy_label'] == 1]
        if len(buy_samples) > 0:
            print(f"\n  Records with BUY label (buy_label = 1):")
            print(buy_samples[available_cols].to_string(index=False))
        
    except Exception as e:
        print(f"✗ Error generating labels: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 60)
    print("✓ Pipeline test completed successfully!")
    print("=" * 60)
    
    # Return DataFrame with only requested columns in correct order
    result_cols = ['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'buystrong_label', 'buy_label']
    available_cols = [col for col in result_cols if col in df_labeled.columns]
    return df_labeled[available_cols]


if __name__ == "__main__":
    # Default test parameters
    code = "7203"  # Toyota
    start_date = "2024-01-01"
    end_date = "2024-06-30"
    csv_path = None
    
    # Parse command line arguments
    if len(sys.argv) >= 4:
        code = sys.argv[1]
        start_date = sys.argv[2]
        end_date = sys.argv[3]
        csv_path = sys.argv[4] if len(sys.argv) > 4 else None
    
    print(f"\nTest Parameters:")
    print(f"  Stock Code: {code}")
    print(f"  Start Date: {start_date}")
    print(f"  End Date: {end_date}")
    print(f"  CSV Path: {csv_path if csv_path else 'Using J-Quants API'}")
    
    # Run test
    result = test_pipeline(code, start_date, end_date, csv_path)
    
    if result is not None:
        print(f"\n✓ Test completed. Final DataFrame shape: {result.shape}")
        
        # Display final result with all columns
        print(f"\nFinal Result (date, code, open, high, low, close, volume, buystrong_label, buy_label):")
        print(result.to_string(index=False))
        
        # Save to CSV
        output_file = f"result_{code}_{start_date}_{end_date}.csv"
        result.to_csv(output_file, index=False)
        print(f"\n✓ Results saved to: {output_file}")
