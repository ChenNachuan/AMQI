import pandas as pd
import numpy as np
import os
import sys

# Add project root to path
sys.path.append(os.getcwd())

from data.data_loader import load_data

def debug_beta():
    print("Debugging Beta Calculation...")
    
    # 1. Load Data (Replicating construct_risk_factors.py)
    clean_data_dir = 'data/data_cleaner'
    daily_adj_path = os.path.join(clean_data_dir, 'daily_adj.parquet')
    
    if not os.path.exists(daily_adj_path):
        print("daily_adj.parquet not found.")
        return
        
    print("Loading daily_adj...")
    df = pd.read_parquet(daily_adj_path)
    
    # Prepare columns
    cols_to_keep = ['ts_code', 'trade_date', 'hfq_close', 'hfq_vol']
    if 'hfq_close' not in df.columns and 'close' in df.columns:
         cols_to_keep = ['ts_code', 'trade_date', 'close', 'vol']
    
    df = df[[c for c in cols_to_keep if c in df.columns]].copy()
    
    rename_map = {'hfq_close': 'close', 'hfq_vol': 'vol'}
    df = df.rename(columns=rename_map)
    
    # Recalculate pct_chg
    print("Recalculating pct_chg...")
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values(['ts_code', 'trade_date'])
    df['pct_chg'] = df.groupby('ts_code')['close'].pct_change() * 100
    
    print("pct_chg stats:")
    print(df['pct_chg'].describe())
    print(f"NaN pct_chg: {df['pct_chg'].isna().sum()}")
    
    # Load daily_basic
    print("Loading daily_basic...")
    # Use fastparquet directly as in the fix
    daily_basic_path = 'data/raw_data/daily_basic.parquet'
    try:
        daily_basic = pd.read_parquet(daily_basic_path, engine='fastparquet', columns=['ts_code', 'trade_date', 'total_mv', 'turnover_rate'])
    except:
        daily_basic = pd.read_parquet(daily_basic_path, engine='fastparquet')
        daily_basic = daily_basic[['ts_code', 'trade_date', 'total_mv', 'turnover_rate']]
        
    daily_basic['trade_date'] = pd.to_datetime(daily_basic['trade_date'])
    
    # Check Date Ranges
    print(f"df date range: {df['trade_date'].min()} to {df['trade_date'].max()}")
    print(f"daily_basic date range: {daily_basic['trade_date'].min()} to {daily_basic['trade_date'].max()}")
    
    # Merge
    print("Merging...")
    merged = pd.merge(df, daily_basic, on=['ts_code', 'trade_date'], how='inner')
    print(f"Merged shape: {merged.shape}")
    # Check Date Frequency
    dates = merged['trade_date'].drop_duplicates().sort_values()
    print("First 20 dates:")
    print(dates.head(20))
    
    print("\nDate Diff (days):")
    print(dates.diff().describe())
    
    # Calc Market Return
    merged['ret'] = merged['pct_chg'] / 100.0
    
    def weighted_avg(x):
        if x['total_mv'].sum() == 0:
            return np.nan
        return np.average(x['ret'], weights=x['total_mv'])
        
    print("Calculating mkt_ret...")
    # This might be slow, so let's do it for a subset or check if we can vectorize
    # Vectorized weighted average:
    # sum(ret * mv) / sum(mv) per date
    
    # Drop NaNs in ret or total_mv
    valid = merged.dropna(subset=['ret', 'total_mv'])
    valid['w_ret'] = valid['ret'] * valid['total_mv']
    
    daily_sums = valid.groupby('trade_date')[['w_ret', 'total_mv']].sum()
    daily_sums['mkt_ret'] = daily_sums['w_ret'] / daily_sums['total_mv']
    
    mkt_ret = daily_sums[['mkt_ret']].reset_index()
    
    print("mkt_ret stats:")
    print(mkt_ret['mkt_ret'].describe())
    print(f"NaN mkt_ret: {mkt_ret['mkt_ret'].isna().sum()}")
    
    # Check Beta Calculation Logic
    print("Checking Beta logic on small sample...")
    # Take top 5 stocks
    sample_codes = merged['ts_code'].unique()[:5]
    sample_df = merged[merged['ts_code'].isin(sample_codes)].copy()
    sample_df = pd.merge(sample_df, mkt_ret, on='trade_date', how='left')
    
    returns_wide = sample_df.pivot(index='trade_date', columns='ts_code', values='ret')
    mkt_ret_series = sample_df[['trade_date', 'mkt_ret']].drop_duplicates().set_index('trade_date')['mkt_ret']
    
    print("Alignment Check:")
    print(f"Returns index: {returns_wide.index.min()} to {returns_wide.index.max()} (len={len(returns_wide)})")
    print(f"Mkt index: {mkt_ret_series.index.min()} to {mkt_ret_series.index.max()} (len={len(mkt_ret_series)})")
    
    mkt_ret_series = mkt_ret_series.reindex(returns_wide.index)
    print(f"Mkt after reindex: NaN count = {mkt_ret_series.isna().sum()}")
    
    # Try small window
    window = 10
    print(f"Trying window={window}...")
    
    rolling_cov = returns_wide.rolling(window).cov(mkt_ret_series)
    rolling_var = mkt_ret_series.rolling(window).var()
    beta_wide = rolling_cov.div(rolling_var, axis=0)
    
    print("Sample Beta stats (window=10):")
    print(beta_wide.describe())
    print(f"Sample Beta Valid: {beta_wide.count().sum()}")
    
    # Try window=24
    window = 24
    print(f"Trying window={window}...")
    rolling_cov = returns_wide.rolling(window).cov(mkt_ret_series)
    rolling_var = mkt_ret_series.rolling(window).var()
    beta_wide = rolling_cov.div(rolling_var, axis=0)
    print(f"Sample Beta Valid (window={window}): {beta_wide.count().sum()}")

if __name__ == "__main__":
    debug_beta()
