
import pandas as pd
import os

def inspect_price():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dataset_path = os.path.join(base_dir, 'data', 'final_dataset.parquet')
    
    if not os.path.exists(dataset_path):
        print(f"Dataset not found at {dataset_path}")
        return

    print(f"Loading dataset from {dataset_path}...")
    df = pd.read_parquet(dataset_path)
    
    # Reset index if needed
    if 'ts_code' not in df.columns:
        df = df.reset_index()
        
    target_code = '600938.SH'
    target_date = '2025-12-12'
    
    # Ensure datetime
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    
    print(f"Querying {target_code} around {target_date}...")
    
    # Filter by stock code first
    stock_df = df[df['ts_code'] == target_code].sort_values('trade_date')
    
    if stock_df.empty:
        print(f"No data for {target_code}")
        return
        
    # Get last record (closest to Dec 2025)
    row = stock_df.iloc[-1]
    
    # Check if date is close enough (within 3 days)
    target_ts = pd.Timestamp(target_date)
    date_diff = abs(row['trade_date'] - target_ts).days
    
    print(f"Found record dated: {row['trade_date']} (Diff: {date_diff} days)")
    
    if date_diff > 7:
        print("Warning: Date seems too far off.")
    
    print(f"Weekly Close: {row['weekly_close']}")
    print(f"Weekly Open:  {row['weekly_open']}")
        
    if abs(row['weekly_close'] - 28.40) < 0.05:
        print("VERIFIED: Weekly Close matches 28.40")

    expected_open = 28.99
    print(f"Expected Open (2025-12-08): {expected_open}")
    diff_open = abs(row['weekly_open'] - expected_open)
    print(f"Open Difference: {diff_open:.4f}")

    if diff_open < 0.05:
        print("VERIFIED: Weekly Open matches expectation.")
    else:
        print("MISMATCH: Weekly Open deviates from expectation.")

if __name__ == "__main__":
    inspect_price()
