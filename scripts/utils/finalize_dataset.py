
import pandas as pd
import numpy as np
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from data.data_loader import load_data
from factor_library import Universe

# Define Aggregation Rules
AGGREGATION_RULES = {
    # --- 必须取平均的 (Flow / Activity) ---
    'TUR': 'mean',
    
    # --- 必须取期末值的 (State / Signal) ---
    # 技术面
    'ATR': 'last',
    'Boll': 'last',
    'Ichimoku': 'last',
    'MFI': 'last',
    'OBV': 'last',
    'PVT': 'last',
    'RVI': 'last',
    'TEMA': 'last',
    'SWMA': 'last',
    
    # 风险/基本面 (已隐含 Rolling 或本身就是截面数据)
    'beta': 'last',
    'IVFF': 'last',
    'R11': 'last',
    'Srev': 'last',
    'Bm': 'last',
    'Ep': 'last',
    'size': 'last',   # 市值通常取月末时点值
    'ret': 'last'     # 注意：如果 ret 是原始日收益，这里其实应该用 compound 或 sum，
                      # 但你的 fundamental_factors 里算的是 monthly ret，所以如果是合并后的不用管。
                      # 如果是日频数据直接转月频，ret 不能用 last 或 mean，要用累乘。
                      # *注：你的流程里 ret 已经在 fundamental_factors 里算成月频了，这里指的是 Risk/Tech 因子。*
}

def downsample_daily_to_monthly(df: pd.DataFrame, name: str = "Data") -> pd.DataFrame:
    """
    Downsample daily data to monthly using dynamic aggregation rules.
    """
    print(f"Downsampling {name}...")
    
    # Ensure datetime and sort
    if 'trade_date' not in df.columns:
        df = df.reset_index()
    
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values(['ts_code', 'trade_date'])
    
    # Create month key
    df['month'] = df['trade_date'].dt.to_period('M')
    
    # Construct Aggregation Dictionary
    agg_dict = {}
    for col in df.columns:
        if col in ['ts_code', 'month']:
            continue
        
        # trade_date should always be last (to capture the actual date of the record)
        if col == 'trade_date':
            agg_dict[col] = 'last'
            continue
            
        # Use specific rule if exists, else default to 'last'
        agg_dict[col] = AGGREGATION_RULES.get(col, 'last')
        
    print(f"Aggregation Rules for {name}:")
    # Print only non-default rules for clarity, or all if needed
    for k, v in agg_dict.items():
        if v != 'last':
            print(f"  - {k}: {v}")
            
    # Perform Aggregation
    monthly_df = df.groupby(['ts_code', 'month']).agg(agg_dict).reset_index()
    print(f"{name} monthly shape: {monthly_df.shape}")
    
    return monthly_df

def finalize_dataset():
    print("Finalizing Dataset (Robust Month-Key Join)...")
    
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    factors_dir = os.path.join(base_dir, 'data', 'factors')
    output_path = os.path.join(base_dir, 'data', 'final_dataset.parquet')
    
    fund_path = os.path.join(factors_dir, 'fundamental_factors.parquet')
    risk_path = os.path.join(factors_dir, 'risk_factors.parquet')
    tech_path = os.path.join(factors_dir, 'technical_factors.parquet')
    
    # 1. Load Data
    print("Loading factor files...")
    if not os.path.exists(fund_path) or not os.path.exists(risk_path):
        raise FileNotFoundError("Factor files not found. Please run factor construction scripts first.")
        
    fund_df = pd.read_parquet(fund_path) # Monthly (Calendar Month End usually)
    risk_df = pd.read_parquet(risk_path) # Daily
    
    if os.path.exists(tech_path):
        print("Loading technical factors...")
        tech_df = pd.read_parquet(tech_path) # Daily
    else:
        print("Warning: Technical factors file not found. Skipping.")
        tech_df = None
    
    print(f"Fundamental factors shape: {fund_df.shape}")
    print(f"Risk factors shape: {risk_df.shape}")
    if tech_df is not None:
        print(f"Technical factors shape: {tech_df.shape}")
    
    # Load Market Cap for Filtering
    print("Loading daily_basic for market cap filtering...")
    # We need total_mv. We load daily and will downsample.
    daily_basic = load_data('daily_basic', columns=['total_mv'], filter_universe=True)
    
    # 2. Pre-processing & Month Key Creation for Fundamental
    print("Creating Month Keys for Fundamental...")
    if 'trade_date' not in fund_df.columns:
        fund_df = fund_df.reset_index()
    fund_df['trade_date'] = pd.to_datetime(fund_df['trade_date'])
    fund_df['month'] = fund_df['trade_date'].dt.to_period('M')
    
    # 3. Downsample Daily Data to Monthly
    risk_monthly = downsample_daily_to_monthly(risk_df, name="Risk Factors")
    
    tech_monthly = None
    if tech_df is not None:
        tech_monthly = downsample_daily_to_monthly(tech_df, name="Technical Factors")
    
    # Market Cap: Take last record per stock-month
    # We can use the helper or just do it manually since it's simple
    daily_basic['trade_date'] = pd.to_datetime(daily_basic['trade_date'])
    daily_basic['month'] = daily_basic['trade_date'].dt.to_period('M')
    daily_basic = daily_basic.sort_values(['ts_code', 'trade_date'])
    mv_monthly = daily_basic.groupby(['ts_code', 'month']).last().reset_index()
    
    # 4. Robust Merge
    print("Merging datasets on Month Key...")
    
    # Fundamental (Left) + Risk (Right) on ['ts_code', 'month']
    # Rename trade_date in Fundamental
    fund_df = fund_df.rename(columns={'trade_date': 'fund_date'})
    
    merged = pd.merge(fund_df, risk_monthly, on=['ts_code', 'month'], how='left')
    
    # Merge Technical
    if tech_monthly is not None:
        # Drop trade_date from tech to avoid collision if it exists in risk (it should be same or close)
        # But wait, we want to ensure we have a valid trade_date.
        # Risk monthly has 'trade_date'. Tech monthly has 'trade_date'.
        # They should be the same (end of month).
        # We can drop one.
        tech_monthly = tech_monthly.drop(columns=['trade_date'])
        merged = pd.merge(merged, tech_monthly, on=['ts_code', 'month'], how='left')
    
    # Merge Market Cap
    merged = pd.merge(merged, mv_monthly[['ts_code', 'month', 'total_mv']], on=['ts_code', 'month'], how='left')
    
    print(f"Merged shape before filtering: {merged.shape}")
    
    # 5. Data Enrichment
    print("Calculating Roe...")
    # Roe = Ep / Bm
    if 'Ep' in merged.columns and 'Bm' in merged.columns:
        merged['Roe'] = merged['Ep'] / merged['Bm']
        merged['Roe'] = merged['Roe'].replace([np.inf, -np.inf], np.nan)
    else:
        print("Warning: Ep or Bm missing. Skipping Roe calculation.")
        merged['Roe'] = np.nan
    
    # 6. Universe Filtering
    print("Applying Universe Filter (Bottom 30% Market Cap)...")
    
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
