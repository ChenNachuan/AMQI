
import pandas as pd
import os

def inspect_tail_ret():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dataset_path = os.path.join(base_dir, 'data', 'final_dataset.parquet')
    
    if not os.path.exists(dataset_path):
        print("Dataset not found.")
        return

    df = pd.read_parquet(dataset_path)
    if 'ts_code' not in df.columns:
        df = df.reset_index()
        
    target_code = '600938.SH'
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    
    print(f"Inspecting tail for {target_code}...")
    stock_df = df[df['ts_code'] == target_code].sort_values('trade_date')
    
    # Show last 3 rows
    cols = ['ts_code', 'trade_date', 'weekly_close', 'ret', 'next_ret']
    # Check if 'ret' exists, otherwise show columns related to return
    if 'ret' not in stock_df.columns:
        print("'ret' column missing, showing available columns...")
        cols = ['ts_code', 'trade_date', 'weekly_close', 'next_ret']
    
    print(stock_df[cols].tail(5))
    
    # Verify why next_ret exists for Dec 12
    # Check if there is a row AFTER Dec 12
    last_date = stock_df['trade_date'].max()
    print(f"\nLast Date for stock: {last_date}")
    
    dec12_row = stock_df[stock_df['trade_date'] == '2025-12-12']
    if not dec12_row.empty:
        print("\nRow for 2025-12-12:")
        print(dec12_row[cols])
        
        # Check manual calculation
        # next_ret should match ret of the next row
        next_row = stock_df[stock_df['trade_date'] > '2025-12-12'].head(1)
        if not next_row.empty:
            print("\nNext available row:")
            print(next_row[cols])
            
            calculated = next_row['ret'].values[0] if 'ret' in next_row.columns else "N/A"
            print(f"Next Row 'ret': {calculated}")
            print(f"Dec 12 'next_ret': {dec12_row['next_ret'].values[0]}")

if __name__ == "__main__":
    inspect_tail_ret()
