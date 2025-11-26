
import pandas as pd
import numpy as np
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.data_loader import load_data
from factor_library import Universe

def finalize_dataset():
    print("Finalizing Dataset...")
    
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    factors_dir = os.path.join(base_dir, 'data', 'factors')
    output_path = os.path.join(base_dir, 'data', 'final_dataset.parquet')
    
    fund_path = os.path.join(factors_dir, 'fundamental_factors.parquet')
    risk_path = os.path.join(factors_dir, 'risk_factors.parquet')
    
    # 1. Load Data
    print("Loading factor files...")
    if not os.path.exists(fund_path) or not os.path.exists(risk_path):
        raise FileNotFoundError("Factor files not found. Please run factor construction scripts first.")
        
    fund_df = pd.read_parquet(fund_path) # Monthly
    risk_df = pd.read_parquet(risk_path) # Daily
    
    print(f"Fundamental factors shape: {fund_df.shape}")
    print(f"Risk factors shape: {risk_df.shape}")
    
    # Load Market Cap for Filtering
    print("Loading daily_basic for market cap filtering...")
    # We need total_mv at monthly frequency to match the factors
    # We can load it via data_loader
    daily_basic = load_data('daily_basic', columns=['total_mv'], filter_universe=True)
    
    # 2. Frequency Alignment (Monthly)
    print("Aligning frequencies...")
    # Fundamental factors are already monthly (month-end).
    # We want to attach risk factors and market cap at those specific dates.
    
    # Reset index to columns for merging
    if 'trade_date' not in fund_df.columns:
        fund_df = fund_df.reset_index()
    if 'trade_date' not in risk_df.columns:
        risk_df = risk_df.reset_index()
        
    # Ensure datetime
    fund_df['trade_date'] = pd.to_datetime(fund_df['trade_date'])
    risk_df['trade_date'] = pd.to_datetime(risk_df['trade_date'])
    daily_basic['trade_date'] = pd.to_datetime(daily_basic['trade_date'])
    
    # Left Join: Fundamental (Left) -> Risk (Right)
    # This keeps only the dates present in Fundamental (Month-Ends)
    merged = pd.merge(fund_df, risk_df, on=['ts_code', 'trade_date'], how='left')
    
    # Merge Market Cap
    merged = pd.merge(merged, daily_basic[['ts_code', 'trade_date', 'total_mv']], on=['ts_code', 'trade_date'], how='left')
    
    print(f"Merged shape before filtering: {merged.shape}")
    
    # 3. Data Enrichment
    print("Calculating Roe...")
    # Roe = Ep / Bm
    merged['Roe'] = merged['Ep'] / merged['Bm']
    merged['Roe'] = merged['Roe'].replace([np.inf, -np.inf], np.nan)
    
    # 4. Universe Filtering
    print("Applying Universe Filter (Bottom 30% Market Cap)...")
    # Use the Universe class
    merged = Universe.apply_market_cap_filter(merged, threshold_percent=0.3)
    
    print(f"Merged shape after filtering: {merged.shape}")
    
    # 5. Target Generation
    print("Creating next_ret...")
    merged = merged.sort_values(['ts_code', 'trade_date'])
    
    # Shift ret backwards by 1 to get next month's return aligned with current factors
    # Note: 'ret' in fundamental factors is the monthly return of the CURRENT month.
    # We want to predict the NEXT month's return.
    merged['next_ret'] = merged.groupby('ts_code')['ret'].shift(-1)
    
    # 6. Final Save
    print("Finalizing...")
    # Drop rows where next_ret is NaN (last month)
    merged = merged.dropna(subset=['next_ret'])
    
    # Drop total_mv if not needed (we have size)
    if 'total_mv' in merged.columns:
        merged = merged.drop(columns=['total_mv'])
        
    # Set index
    merged = merged.set_index(['trade_date', 'ts_code']).sort_index()
    
    print(f"Final shape: {merged.shape}")
    print("Columns:", merged.columns.tolist())
    
    print(f"Saving to {output_path}...")
    merged.to_parquet(output_path)
    print("Done.")

if __name__ == "__main__":
    finalize_dataset()
