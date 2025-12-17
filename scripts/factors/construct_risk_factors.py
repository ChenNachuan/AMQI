
import pandas as pd
import numpy as np
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from data.data_loader import load_data
from factor_library import Beta, Ivff, Turnover, Reversal

def run_risk_factors():
    print("Running Risk Factors Construction (Adjusted Data)...")
    
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    factors_dir = os.path.join(base_dir, 'data', 'factors')
    if not os.path.exists(factors_dir):
        os.makedirs(factors_dir)
    output_path = os.path.join(factors_dir, 'risk_factors.parquet')
    
    # 1. Load Adjusted Data
    print("Loading adjusted price data (daily_adj)...")
    # Load daily_adj.parquet directly
    # Note: load_data('adj_factor') loads the factor, but we want the adjusted PRICES.
    # The prompt says: "START loading data/data_cleaner/daily_adj.parquet directly."
    # We can use pandas directly or load_data if it supports it.
    # load_data supports 'daily' but that's raw.
    # Let's check if 'daily_adj' is in DATASET_MAPPING. It likely isn't.
    # So we load directly using pandas.
    
    clean_data_dir = os.path.join(base_dir, 'data', 'data_cleaner')
    daily_adj_path = os.path.join(clean_data_dir, 'daily_adj.parquet')
    
    if not os.path.exists(daily_adj_path):
        raise FileNotFoundError(f"Adjusted data not found at {daily_adj_path}. Please run data cleaning first.")
        
    df = pd.read_parquet(daily_adj_path)
    
    # 2. Rename and Prepare Columns
    print("Preparing adjusted data...")
    # Rename hfq_close -> close, hfq_vol -> vol
    # To avoid duplicate columns (e.g. if 'close' already exists), we select only what we need.
    cols_to_keep = ['ts_code', 'trade_date', 'hfq_close', 'hfq_vol']
    
    # Check if columns exist
    missing_cols = [c for c in cols_to_keep if c not in df.columns]
    if missing_cols:
        # Fallback: maybe it's already renamed or different format?
        print(f"Warning: Missing columns {missing_cols}. Available: {df.columns.tolist()}")
        # If hfq_close is missing but close is there, maybe it's already processed?
        # But we must ensure it's ADJUSTED.
        # Let's assume strict requirement for now, or try to proceed if 'close' is there but warn.
        if 'hfq_close' not in df.columns and 'close' in df.columns:
             print("Using existing 'close' column. verify it is adjusted.")
             cols_to_keep = ['ts_code', 'trade_date', 'close', 'vol']
             # Adjust cols_to_keep to match what we have
             cols_to_keep = [c for c in cols_to_keep if c in df.columns]
    
    df = df[cols_to_keep].copy()
    
    # Rename
    rename_map = {'hfq_close': 'close', 'hfq_vol': 'vol'}
    df = df.rename(columns=rename_map)
        
    # Recalculate pct_chg from adjusted close
    print("Recalculating pct_chg from adjusted close...")
    df = df.sort_values(['ts_code', 'trade_date'])
    df['pct_chg'] = df.groupby('ts_code')['close'].pct_change() * 100
    
    # 3. Load Auxiliary Data (Turnover, Total MV)
    print("Loading auxiliary data (daily_basic)...")
    daily_basic = load_data('daily_basic', columns=['total_mv', 'turnover_rate'], filter_universe=True)
    
    # Merge
    print("Merging adjusted prices with auxiliary data...")
    # Ensure trade_date is datetime
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    daily_basic['trade_date'] = pd.to_datetime(daily_basic['trade_date'])
    
    # Merge
    merged = pd.merge(df, daily_basic[['ts_code', 'trade_date', 'total_mv', 'turnover_rate']], on=['ts_code', 'trade_date'], how='inner')
    
    # 4. Calculate Market Return
    print("Calculating Market Return...")
    # ret = pct_chg / 100
    merged['ret'] = merged['pct_chg'] / 100.0
    
    # Weighted average (Vectorized)
    # Drop NaNs in ret or total_mv for calculation
    valid = merged.dropna(subset=['ret', 'total_mv'])
    valid['w_ret'] = valid['ret'] * valid['total_mv']
    
    daily_sums = valid.groupby('trade_date')[['w_ret', 'total_mv']].sum()
    daily_sums['mkt_ret'] = daily_sums['w_ret'] / daily_sums['total_mv']
    
    mkt_ret = daily_sums[['mkt_ret']].reset_index()
    
    print("mkt_ret stats:")
    print(mkt_ret['mkt_ret'].describe())
    
    # Merge market return back
    merged = pd.merge(merged, mkt_ret, on='trade_date', how='left')
    
    # 5. Calculate Factors
    print("Calculating Factors...")
    
    # Instantiate Factors
    beta_factor = Beta()
    ivff_factor = Ivff()
    tur_factor = Turnover()
    srev_factor = Reversal()
    
    # Beta
    print(f"Calculating {beta_factor.name}...")
    beta_df = beta_factor.calculate(merged)
    
    # Add Beta to merged for Ivff calculation
    merged = pd.merge(merged, beta_df, on=['trade_date', 'ts_code'], how='left')
    
    # Ivff
    print(f"Calculating {ivff_factor.name}...")
    ivff_df = ivff_factor.calculate(merged)
    
    # Turnover
    print(f"Calculating {tur_factor.name}...")
    tur_df = tur_factor.calculate(merged)
    
    # Reversal
    print(f"Calculating {srev_factor.name}...")
    srev_df = srev_factor.calculate(merged)
    
    # 6. Merge Results
    print("Merging results...")
    dfs = [beta_df, ivff_df, tur_df, srev_df]
    final_df = pd.concat(dfs, axis=1)
    
    # Remove duplicate columns if any (from concat axis=1 with same index)
    # Remove duplicate columns if any (from concat axis=1 with same index)
    final_df = final_df.loc[:, ~final_df.columns.duplicated()]
    
    print(f"Daily shape: {final_df.shape}")
    
    # 6b. Resample to Weekly (Friday)
    print("Resampling to weekly (Friday)...")
    final_df = final_df.reset_index()
    if 'trade_date' not in final_df.columns:
         # Depending on how Beta etc return data. Usually they return MultiIndex.
         # If reset_index didn't find trade_date, implies it was column.
         pass
         
    final_df['trade_date'] = pd.to_datetime(final_df['trade_date'])
    final_df['week'] = final_df['trade_date'].dt.to_period('W-FRI')
    
    # Aggregation rules: Last for state variables
    # Risk factors like Beta, IVFF are usually state variables (updated daily).
    # Taking the end-of-week value is standard downsampling.
    weekly_df = final_df.groupby(['ts_code', 'week']).last().reset_index()
    
    weekly_df = weekly_df.drop(columns=['week'])
    
    # Set index
    weekly_df = weekly_df.set_index(['trade_date', 'ts_code']).sort_index()
    
    print(f"Weekly shape: {weekly_df.shape}")
    print("Columns:", weekly_df.columns.tolist())
    
    # 7. Save
    print(f"Saving to {output_path}...")
    weekly_df.to_parquet(output_path, engine='fastparquet')
    print("Done.")

if __name__ == "__main__":
    run_risk_factors()
