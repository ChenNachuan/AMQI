
import pandas as pd
import numpy as np
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from factor_library import Universe

def merge_factors():
    print("Merging factors...")
    
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    factors_dir = os.path.join(base_dir, 'data', 'factors')
    raw_data_dir = os.path.join(base_dir, 'data', 'raw_data')
    
    fund_path = os.path.join(factors_dir, 'fundamental_factors.parquet')
    risk_path = os.path.join(factors_dir, 'risk_factors.parquet')
    tech_path = os.path.join(factors_dir, 'technical_factors.parquet')
    daily_basic_path = os.path.join(raw_data_dir, 'daily_basic.parquet')
    output_path = os.path.join(base_dir, 'data', 'final_dataset.parquet')
    
    # 1. Load Factors
    print("Loading factor files...")
    if not os.path.exists(fund_path) or not os.path.exists(risk_path):
        raise FileNotFoundError("Factor files not found. Please run factor construction scripts first.")
        
    fund_df = pd.read_parquet(fund_path)
    risk_df = pd.read_parquet(risk_path)
    
    tech_df = None
    if os.path.exists(tech_path):
        print(f"Loading technical factors from {tech_path}")
        tech_df = pd.read_parquet(tech_path)
    else:
        print("Warning: Technical factors file not found. Skipping.")
    
    print(f"Fundamental factors shape: {fund_df.shape}")
    print(f"Risk factors shape: {risk_df.shape}")
    if tech_df is not None:
        print(f"Technical factors shape: {tech_df.shape}")
    
    # 2. Load Market Cap for Filtering
    print("Loading daily_basic for market cap filtering...")
    # We need total_mv at monthly frequency to match the factors
    try:
        daily_basic = pd.read_parquet(daily_basic_path, columns=['ts_code', 'trade_date', 'total_mv'], engine='fastparquet')
    except:
        daily_basic = pd.read_parquet(daily_basic_path, columns=['ts_code', 'trade_date', 'total_mv'])
    daily_basic['trade_date'] = pd.to_datetime(daily_basic['trade_date'].astype(str))
    
    # Resample total_mv to monthly (end of month)
    # This aligns with our monthly factors
    daily_basic['month'] = daily_basic['trade_date'].dt.to_period('M')
    daily_basic = daily_basic.sort_values(['ts_code', 'trade_date'])
    monthly_mv = daily_basic.groupby(['ts_code', 'month']).last().reset_index()

    # 2.5 Load Adjusted Prices
    print("Loading adjusted prices...")
    daily_adj_path = os.path.join(raw_data_dir.replace('raw_data', 'data_cleaner'), 'daily_adj.parquet')
    if os.path.exists(daily_adj_path):
        try:
            daily_adj = pd.read_parquet(daily_adj_path, engine='fastparquet')
        except:
            daily_adj = pd.read_parquet(daily_adj_path)
        
        # Ensure trade_date is datetime
        daily_adj['trade_date'] = pd.to_datetime(daily_adj['trade_date'].astype(str))
        daily_adj['month'] = daily_adj['trade_date'].dt.to_period('M')
        
        # Select columns (hfq_close, hfq_open, etc.)
        # We only want hfq columns to avoid duplicates if 'close' also exists
        cols = ['ts_code', 'trade_date', 'month', 'hfq_close', 'hfq_open', 'hfq_high', 'hfq_low', 'hfq_vol']
        # Check if they exist
        cols = [c for c in cols if c in daily_adj.columns]
        daily_adj = daily_adj[cols]
        
        # Resample to monthly (last)
        monthly_prices = daily_adj.groupby(['ts_code', 'month']).last().reset_index()
        
        # Rename hfq columns to standard if needed
        rename_map = {
            'hfq_close': 'close', 'hfq_open': 'open', 'hfq_high': 'high', 'hfq_low': 'low', 'hfq_vol': 'vol'
        }
        monthly_prices = monthly_prices.rename(columns=rename_map)
        
        # Merge with monthly_mv
        # monthly_mv has total_mv. monthly_prices has OHLCV.
        # Both have ts_code and month.
        # Note: trade_date in both is the last date of the month (from .last()).
        # But they might differ slightly if data is missing?
        # Let's merge on ['ts_code', 'month']
        
        monthly_mv = pd.merge(monthly_mv, monthly_prices, on=['ts_code', 'month'], how='left', suffixes=('', '_price'))
        # If trade_date exists in both, we might have trade_date and trade_date_price.
        # We prefer the one from daily_basic as it aligns with fundamental factors usually?
        # Actually, fundamental factors trade_date is month end.
        # Let's keep trade_date from monthly_mv.
        if 'trade_date_price' in monthly_mv.columns:
            monthly_mv = monthly_mv.drop(columns=['trade_date_price'])
            
    else:
        print("Warning: Adjusted daily prices not found.")
    
    # 3. Merge Factors
    print("Merging datasets...")
    fund_df = fund_df.reset_index()
    risk_df = risk_df.reset_index()
    if tech_df is not None:
        tech_df = tech_df.reset_index()
    
    # Ensure columns exist
    join_keys = ['trade_date', 'ts_code']
    
    # Merge Fundamental and Risk
    merged = pd.merge(fund_df, risk_df, on=join_keys, how='inner')
    
    # Merge Technical
    if tech_df is not None:
        merged = pd.merge(merged, tech_df, on=join_keys, how='inner')
    
    # Merge with Market Cap (for filtering)
    # Note: merged has 'trade_date' which is month-end. monthly_mv has 'trade_date' which is also month-end.
    # So we can join on trade_date and ts_code.
    # Merge with Market Cap and Prices
    # Note: merged has 'trade_date' which is month-end. monthly_mv has 'trade_date' which is also month-end.
    # So we can join on trade_date and ts_code.
    # monthly_mv now contains total_mv AND prices (close, open, etc.)
    
    # Identify columns to merge from monthly_mv
    # We want total_mv, close, open, high, low, vol
    # But only if they are not already in merged
    cols_to_merge = ['ts_code', 'trade_date']
    
    # Check total_mv
    if 'total_mv' not in merged.columns and 'total_mv' in monthly_mv.columns:
        cols_to_merge.append('total_mv')
        
    price_cols = ['close', 'open', 'high', 'low', 'vol']
    for c in price_cols:
        if c in monthly_mv.columns and c not in merged.columns:
            cols_to_merge.append(c)
            
    if len(cols_to_merge) > 2: # More than just keys
        print(f"Merging additional columns: {cols_to_merge[2:]}")
        merged = pd.merge(merged, monthly_mv[cols_to_merge], on=['ts_code', 'trade_date'], how='left')
    else:
        print("No additional price/mv columns to merge.")
    
    print(f"Merged shape before filtering: {merged.shape}")
    
    # 4. Apply Universe Filter
    print("Applying Universe Filter (Bottom 30% Market Cap)...")
    # Use the Universe class
    merged = Universe.apply_market_cap_filter(merged, threshold_percent=0.3)
    
    print(f"Merged shape after filtering: {merged.shape}")
    
    # 5. Calculate Roe
    print("Calculating Roe...")
    # Roe = Ep / Bm
    merged['Roe'] = merged['Ep'] / merged['Bm']
    merged['Roe'] = merged['Roe'].replace([np.inf, -np.inf], np.nan)
    
    # 6. Create Next Return (Target)
    print("Creating next_ret...")
    merged = merged.sort_values(['ts_code', 'trade_date'])
    merged['next_ret'] = merged.groupby('ts_code')['ret'].shift(-1)
    
    # 7. Final Cleanup
    print("Finalizing...")
    # Drop total_mv if not needed in final dataset, or keep it as 'size' (we already have 'size' from fundamental factors)
    # 'size' in fundamental factors came from total_mv, so it should be redundant but let's check.
    # We can drop the extra 'total_mv' column.
    if 'total_mv' in merged.columns:
        merged = merged.drop(columns=['total_mv'])
        
    # Set index
    merged = merged.set_index(['trade_date', 'ts_code']).sort_index()
    
    # Save
    print(f"Saving to {output_path}...")
    merged.to_parquet(output_path)
    
    print("Done.")
    print("\nColumns:")
    print(merged.columns.tolist())
    print("\nSample Output:")
    print(merged.head())

if __name__ == "__main__":
    merge_factors()
