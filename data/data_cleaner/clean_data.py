
import pandas as pd
import os

def clean_data():
    # Define paths
    raw_data_dir = 'data/raw_data'
    output_dir = 'data/data_cleaner'
    
    stock_basic_path = os.path.join(raw_data_dir, 'stock_basic.parquet')
    daily_basic_path = os.path.join(raw_data_dir, 'daily_basic.parquet')
    daily_path = os.path.join(raw_data_dir, 'daily.parquet')
    output_path = os.path.join(output_dir, 'daily_basic_cleaned.parquet')
    
    print("Loading data...")
    stock_basic = pd.read_parquet(stock_basic_path)
    daily_basic = pd.read_parquet(daily_basic_path)
    # Only load necessary columns from daily to save memory
    daily = pd.read_parquet(daily_path, columns=['ts_code', 'trade_date', 'vol'])
    
    print(f"Original daily records: {len(daily_basic)}")
    
    # Merge necessary columns
    print("Merging data...")
    stock_info = stock_basic[['ts_code', 'name', 'list_date']]
    
    # Merge stock info
    merged_df = pd.merge(daily_basic, stock_info, on='ts_code', how='left')
    
    # Merge daily volume info
    merged_df['trade_date'] = pd.to_datetime(merged_df['trade_date'].astype(str))
    daily['trade_date'] = pd.to_datetime(daily['trade_date'].astype(str))
    merged_df['list_date'] = pd.to_datetime(merged_df['list_date'].astype(str))
    
    merged_df = pd.merge(merged_df, daily, on=['ts_code', 'trade_date'], how='left')
    
    # 1. Filter Suspensions (vol == 0)
    print("Filtering suspensions (vol == 0)...")
    # If vol is 0 or NaN, drop.
    merged_df = merged_df[merged_df['vol'] > 0]
    print(f"Records after suspension filter: {len(merged_df)}")
    
    # 2. Filter ST stocks? 
    # The user instruction said: "This script should ONLY clean 'dirty' data (e.g., rows where vol == 0 / Suspensions, or NaNs)."
    # It explicitly said "Remove the logic that filters out 'Smallest 30% Market Cap' and 'New Stocks'".
    # It didn't explicitly say remove ST filter, but usually ST is considered a universe filter, not "dirty data" in the sense of invalid.
    # However, ST stocks are tradable.
    # Let's keep ST stocks in the dataset but maybe mark them? 
    # Or just follow "ONLY clean dirty data". ST is a status, not dirty data.
    # So I will REMOVE the ST filter as well to be safe and allow strategy to decide.
    # Wait, the prompt said "Remove the logic that filters out 'Smallest 30% Market Cap' and 'New Stocks'".
    # It didn't mention ST. But "ONLY clean dirty data" suggests removing ST filter too.
    # I'll remove ST filter to provide full history.
    
    # 3. Save
    # We only need the index columns for the whitelist usually, but the loader uses this file as the "White List".
    # If we keep everything, the "White List" becomes just "Valid Trading Days".
    # That seems correct for a "base infrastructure".
    
    # Drop temporary columns
    merged_df = merged_df.drop(columns=['vol'])
    
    print(f"Final records: {len(merged_df)}")
    
    print(f"Saving to {output_path}...")
    merged_df.to_parquet(output_path)
    print("Done.")

if __name__ == "__main__":
    clean_data()
