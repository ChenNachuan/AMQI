
import pandas as pd
import numpy as np
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from data.data_loader import load_data
from factor_library import Universe

def finalize_dataset():
    print("Finalizing Dataset (Robust Month-Key Join)...")
    
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    factors_dir = os.path.join(base_dir, 'data', 'factors')
    output_path = os.path.join(base_dir, 'data', 'final_dataset.parquet')
    
    fund_path = os.path.join(factors_dir, 'fundamental_factors.parquet')
    risk_path = os.path.join(factors_dir, 'risk_factors.parquet')
    
    # 1. Load Data
    print("Loading factor files...")
    if not os.path.exists(fund_path) or not os.path.exists(risk_path):
        raise FileNotFoundError("Factor files not found. Please run factor construction scripts first.")
        
    fund_df = pd.read_parquet(fund_path) # Monthly (Calendar Month End usually)
    risk_df = pd.read_parquet(risk_path) # Daily
    
    print(f"Fundamental factors shape: {fund_df.shape}")
    print(f"Risk factors shape: {risk_df.shape}")
    
    # Load Market Cap for Filtering
    print("Loading daily_basic for market cap filtering...")
    # We need total_mv. We load daily and will downsample.
    daily_basic = load_data('daily_basic', columns=['total_mv'], filter_universe=True)
    
    # 2. Pre-processing & Month Key Creation
    print("Creating Month Keys...")
    
    # Ensure datetime
    if 'trade_date' not in fund_df.columns:
        fund_df = fund_df.reset_index()
    if 'trade_date' not in risk_df.columns:
        risk_df = risk_df.reset_index()
        
    fund_df['trade_date'] = pd.to_datetime(fund_df['trade_date'])
    risk_df['trade_date'] = pd.to_datetime(risk_df['trade_date'])
    daily_basic['trade_date'] = pd.to_datetime(daily_basic['trade_date'])
    
    # Create 'month' column (Period)
    fund_df['month'] = fund_df['trade_date'].dt.to_period('M')
    risk_df['month'] = risk_df['trade_date'].dt.to_period('M')
    daily_basic['month'] = daily_basic['trade_date'].dt.to_period('M')
    
    # 3. Downsample Daily Data to Monthly (End of Month)
    print("Downsampling Daily Data...")
    
    # Risk Factors: Take last record per stock-month
    # Sort by date first to ensure .last() is the end of month
    risk_df = risk_df.sort_values(['ts_code', 'trade_date'])
    risk_monthly = risk_df.groupby(['ts_code', 'month']).last().reset_index()
    
    # Market Cap: Take last record per stock-month
    daily_basic = daily_basic.sort_values(['ts_code', 'trade_date'])
    mv_monthly = daily_basic.groupby(['ts_code', 'month']).last().reset_index()
    
    print(f"Risk monthly shape: {risk_monthly.shape}")
    
    # 4. Robust Merge
    print("Merging datasets on Month Key...")
    
    # Fundamental (Left) + Risk (Right) on ['ts_code', 'month']
    # Note: Fundamental might have 'trade_date' as Jan 31, Risk as Jan 29.
    # We want to keep the Risk trade_date as it is the actual trading day.
    
    # Rename trade_date in Fundamental to avoid collision/confusion, or just drop it after merge if we prefer Risk date.
    # Let's rename fundamental date to 'fund_date' and risk date to 'trade_date' (it already is).
    fund_df = fund_df.rename(columns={'trade_date': 'fund_date'})
    
    merged = pd.merge(fund_df, risk_monthly, on=['ts_code', 'month'], how='left')
    
    # Merge Market Cap
    merged = pd.merge(merged, mv_monthly[['ts_code', 'month', 'total_mv']], on=['ts_code', 'month'], how='left')
    
    # Restore trade_date
    # If risk data is missing (NaN), trade_date will be NaT.
    # In that case, we might want to fall back to fund_date (converted to timestamp) or just drop.
    # Usually we want valid risk factors, so we might drop rows where risk is missing.
    # But for now, let's keep left join.
    # If trade_date is NaT, it means no risk data for that month.
    
    print(f"Merged shape before filtering: {merged.shape}")
    
    # 5. Data Enrichment
    print("Calculating Roe...")
    # Roe = Ep / Bm
    merged['Roe'] = merged['Ep'] / merged['Bm']
    merged['Roe'] = merged['Roe'].replace([np.inf, -np.inf], np.nan)
    
    # 6. Universe Filtering
    print("Applying Universe Filter (Bottom 30% Market Cap)...")
    # Use the Universe class
    # Note: apply_market_cap_filter expects 'trade_date' and 'total_mv'.
    # We have 'trade_date' (from Risk) and 'total_mv'.
    # If trade_date is NaT, filtering might fail or drop.
    # Let's drop rows with missing trade_date first?
    # Or fill NaT with fund_date?
    # Let's drop rows where we don't have risk data (and thus no trade_date), as we can't analyze them.
    
    merged = merged.dropna(subset=['trade_date'])
    
    merged = Universe.apply_market_cap_filter(merged, threshold_percent=0.3)
    
    print(f"Merged shape after filtering: {merged.shape}")
    
    # 7. Target Generation
    print("Creating next_ret...")
    merged = merged.sort_values(['ts_code', 'trade_date'])
    
    # Shift ret backwards by 1
    # Note: 'ret' here comes from Fundamental (monthly return).
    merged['next_ret'] = merged.groupby('ts_code')['ret'].shift(-1)
    
    # 8. Final Cleanup
    print("Finalizing...")
    # Drop rows where next_ret is NaN
    merged = merged.dropna(subset=['next_ret'])
    
    # Drop auxiliary columns
    cols_to_drop = ['month', 'fund_date', 'total_mv']
    merged = merged.drop(columns=[c for c in cols_to_drop if c in merged.columns])
    
    # Set index
    merged = merged.set_index(['trade_date', 'ts_code']).sort_index()
    
    print(f"Final shape: {merged.shape}")
    print("Columns:", merged.columns.tolist())
    
    print(f"Saving to {output_path}...")
    merged.to_parquet(output_path)
    print("Done.")

if __name__ == "__main__":
    finalize_dataset()
