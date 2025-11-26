
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
    # Ensure trade_date types match before merge if necessary, usually both are string or datetime.
    # Let's convert to datetime immediately to be safe and consistent.
    merged_df['trade_date'] = pd.to_datetime(merged_df['trade_date'].astype(str))
    daily['trade_date'] = pd.to_datetime(daily['trade_date'].astype(str))
    merged_df['list_date'] = pd.to_datetime(merged_df['list_date'].astype(str))
    
    merged_df = pd.merge(merged_df, daily, on=['ts_code', 'trade_date'], how='left')
    
    # 1. Filter Suspensions (vol == 0)
    print("Filtering suspensions (vol == 0)...")
    # Fill NaN vol with 0 just in case, or drop. If it's not in daily, it might be suspended or missing.
    # Assuming missing in daily means no trade -> treat as suspension or just drop.
    # Let's keep it simple: if vol is 0 or NaN, drop.
    merged_df = merged_df[merged_df['vol'] > 0]
    print(f"Records after suspension filter: {len(merged_df)}")
    
    # 2. Filter ST stocks
    print("Filtering ST stocks...")
    non_st_mask = ~merged_df['name'].str.contains('ST', case=False, na=False)
    merged_df = merged_df[non_st_mask]
    print(f"Records after ST filter: {len(merged_df)}")
    
    # 3. Filter new stocks (less than 1 year from list_date)
    print("Filtering new stocks (< 1 year)...")
    merged_df['days_listed'] = (merged_df['trade_date'] - merged_df['list_date']).dt.days
    merged_df = merged_df[merged_df['days_listed'] >= 365]
    print(f"Records after new stock filter: {len(merged_df)}")
    
    # 4. Filter smallest 30% market cap (using T-1)
    print("Filtering smallest 30% market cap (using T-1)...")
    
    # Sort to ensure shift works correctly
    merged_df = merged_df.sort_values(['ts_code', 'trade_date'])
    
    # Calculate T-1 market cap
    # We group by ts_code and shift total_mv
    merged_df['prev_total_mv'] = merged_df.groupby('ts_code')['total_mv'].shift(1)
    
    # Drop rows where prev_total_mv is NaN (e.g. first day of listing, or after a break if we were strictly continuous, 
    # but here it just means we don't have yesterday's data to decide).
    # Since we already filtered new stocks (<365 days), most stocks should have history.
    # However, the first day in the dataset for any stock will have NaN prev_total_mv.
    before_drop_nan = len(merged_df)
    merged_df = merged_df.dropna(subset=['prev_total_mv'])
    print(f"Dropped {before_drop_nan - len(merged_df)} rows due to missing T-1 market cap.")
    
    # Calculate 30th percentile of prev_total_mv per trade_date
    daily_thresholds = merged_df.groupby('trade_date')['prev_total_mv'].quantile(0.30)
    
    # Map back
    merged_df['mv_threshold'] = merged_df['trade_date'].map(daily_thresholds)
    
    # Filter
    merged_df = merged_df[merged_df['prev_total_mv'] >= merged_df['mv_threshold']]
    
    # Drop temporary columns
    merged_df = merged_df.drop(columns=['days_listed', 'prev_total_mv', 'mv_threshold', 'vol'])
    
    print(f"Records after market cap filter: {len(merged_df)}")
    
    # Save
    print(f"Saving to {output_path}...")
    merged_df.to_parquet(output_path)
    print("Done.")

if __name__ == "__main__":
    clean_data()
