
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
    'TUR': 'mean', # Average Daily Turnover for the period
    
    # --- 必须取期末值的 (State / Signal) ---
    # 技术面 (New Factors included)
    'ATR': 'last',
    'Boll': 'last',
    'Ichimoku': 'last',
    'MFI': 'last',
    'OBV': 'last',
    'PVT': 'last',
    'RVI': 'last',
    'TEMA': 'last',
    'SWMA': 'last',
    
    # ATR Variations
    'ATR_Expansion': 'last', 'Price_Breakout': 'last', 'Price_Position': 'last', 
    'ATR_Trend': 'last', 'Volume_Confirmation': 'last',
    
    # Bollinger Variations
    'Boll_Breakout_Upper': 'last', 'Boll_Middle_Support': 'last', 
    'Boll_Oversold_Bounce': 'last', 'Boll_Squeeze_Expansion': 'last',
    
    # Ichimoku Variations
    'IchimokuCloudTrend': 'last', 'IchimokuCloudWidthMomentum': 'last', 
    'IchimokuPricePosition': 'last', 'IchimokuTKCross': 'last',
    
    # MFI Variations
    'MFI_ChangeRate_5d': 'last', 'MFI_Divergence_20d': 'last',
    
    # OBV Variations
    'OBV_Breakthrough': 'last', 'OBV_Change_Rate': 'last', 'OBV_Divergence': 'last', 
    'OBV_Rank': 'last', 'OBV_Slope': 'last',
    
    # PVT Variations
    'PVT_Divergence': 'last', 'PVT_MA_Deviation': 'last', 'PVT_Momentum_Reversal': 'last',
    
    # RVI Variations
    'RVI_Cross': 'last', 'RVI_Diff': 'last', 'RVI_Strength': 'last', 
    'RVI_Trend': 'last', 'RVI_Value': 'last', 'RVI_Volume': 'last',
    
    # 风险/基本面
    'beta': 'last',
    'IVFF': 'last',
    'R11': 'last',
    'Srev': 'last',
    'Bm': 'last',
    'Ep': 'last',
    'size': 'last',   
    'ret': 'sum'      # 如果是日频收益聚合到周频，应使用累乘 ((1+r).prod()-1) 或简单 sum (如果是 log returns)。这里暂时用 sum (近似) 或 compound。
                      # Fund factors 中 ret 可能是月频。Risk/Tech 中的 ret 是日频。
                      # 我们需要计算 "Past Week Return"。
                      # 如果 ret 是百分比，sum 是近似。
}

def downsample_daily_to_weekly(df: pd.DataFrame, name: str = "Data") -> pd.DataFrame:
    """
    Downsample daily data to Weekly (Friday) using dynamic aggregation rules.
    """
    print(f"Downsampling {name} to Weekly...")
    
    # Ensure datetime and sort
    if 'trade_date' not in df.columns:
        df = df.reset_index()
    
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values(['ts_code', 'trade_date'])
    
    # Create week key (Week ending Friday)
    df['week'] = df['trade_date'].dt.to_period('W-FRI')
    
    # Construct Aggregation Dictionary
    agg_dict = {}
    for col in df.columns:
        if col in ['ts_code', 'week', 'trade_date']:
            continue
            
        # Use specific rule if exists, else default to 'last'
        rule = AGGREGATION_RULES.get(col, 'last')
        
        # Special handling for 'ret' (Return)
        if col == 'ret':
            # Ideally: compound return. For simplicity/robustness: sum (assuming small returns) or custom func.
            # Let's just use 'sum' for now as a proxy for weekly return if log returns, or just sum.
            agg_dict[col] = 'sum'
        else:
            agg_dict[col] = rule
        
    print(f"Aggregation Rules for {name}:")
    # Print only non-default rules for clarity
    for k, v in agg_dict.items():
        if v != 'last':
            print(f"  - {k}: {v}")
            
    # Perform Aggregation
    weekly_df = df.groupby(['ts_code', 'week']).agg(agg_dict).reset_index()
    
    # Recover trade_date (Use the Friday of that week)
    weekly_df['trade_date'] = weekly_df['week'].dt.end_time
    
    print(f"{name} weekly shape: {weekly_df.shape}")
    
    # Drop week column
    weekly_df = weekly_df.drop(columns=['week'])
    
    return weekly_df

def finalize_dataset():
    print("Finalizing Dataset (Weekly Frequency)...")
    
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    factors_dir = os.path.join(base_dir, 'data', 'factors')
    output_path = os.path.join(base_dir, 'data', 'final_dataset.parquet')
    
    fund_path = os.path.join(factors_dir, 'fundamental_factors.parquet')
    risk_path = os.path.join(factors_dir, 'risk_factors.parquet')
    tech_path = os.path.join(factors_dir, 'technical_factors.parquet')
    
    # 1. Load Data
    print("Loading factor files...")
    # Fundamental Factors (Low Frequency - Monthly/Quarterly)
    if not os.path.exists(fund_path):
        raise FileNotFoundError("Fundamental factor file not found.")
    fund_df = pd.read_parquet(fund_path).reset_index() 
    
    # Risk/Tech Factors (High Frequency - Daily)
    if not os.path.exists(risk_path):
        raise FileNotFoundError("Risk factor file not found.")
    risk_df = pd.read_parquet(risk_path, engine='fastparquet') 
    
    tech_df = None
    if os.path.exists(tech_path):
        print("Loading technical factors...")
        tech_df = pd.read_parquet(tech_path) 
    else:
        print("警告: 未找到技术因子文件。跳过。")
    
    # Load Market Cap for Filtering (Daily)
    print("正在加载 daily_basic 用于市值过滤...")
    from data.data_loader import RAW_DATA_DIR, WHITELIST_PATH
    daily_basic_path = os.path.join(RAW_DATA_DIR, 'daily_basic.parquet')
    
    try:
        daily_basic = pd.read_parquet(daily_basic_path, engine='fastparquet', columns=['ts_code', 'trade_date', 'total_mv'])
    except Exception:
        daily_basic = pd.read_parquet(daily_basic_path, engine='fastparquet')
        daily_basic = daily_basic[['ts_code', 'trade_date', 'total_mv']]

    # Filter daily_basic by whitelist
    print("Filtering daily_basic by whitelist...")
    whitelist = pd.read_parquet(WHITELIST_PATH, columns=['ts_code', 'trade_date'])
    daily_basic['trade_date'] = pd.to_datetime(daily_basic['trade_date'])
    whitelist['trade_date'] = pd.to_datetime(whitelist['trade_date'])
    daily_basic = pd.merge(whitelist, daily_basic, on=['ts_code', 'trade_date'], how='inner')
    
    # Load Adjusted Prices (Daily) for Weekly Open/Close (QFQ)
    print("Loading daily_adj for Weekly Open/Close (QFQ)...")
    daily_adj_path = os.path.join(base_dir, 'data', 'data_cleaner', 'daily_adj.parquet')
    try:
        daily_adj = pd.read_parquet(daily_adj_path, engine='fastparquet', columns=['ts_code', 'trade_date', 'hfq_open', 'hfq_close', 'adj_factor'])
    except Exception:
        daily_adj = pd.read_parquet(daily_adj_path)
        # Ensure needed columns exist
        if 'hfq_open' not in daily_adj.columns or 'adj_factor' not in daily_adj.columns:
             # Fallback: try to load all and see
             pass
             
    # Filter by whitelist
    daily_adj['trade_date'] = pd.to_datetime(daily_adj['trade_date'])
    daily_adj = pd.merge(whitelist, daily_adj, on=['ts_code', 'trade_date'], how='inner')
    
    # Calculate QFQ (Pre-Adjusted) Prices
    # QFQ_t = HFQ_t / Adj_Factor_Last
    print("Calculating QFQ prices for display...")
    daily_adj = daily_adj.sort_values(['ts_code', 'trade_date'])
    
    # Get latest adj_factor for each stock
    # Note: transforming 'last' implies the last date IN THE DATASET. 
    # This aligns past prices to the price level at the end of 2025.
    latest_factor = daily_adj.groupby('ts_code')['adj_factor'].transform('last')
    
    # Avoid division by zero
    latest_factor = latest_factor.replace(0, 1)
    
    daily_adj['qfq_open'] = daily_adj['hfq_open'] / latest_factor
    daily_adj['qfq_close'] = daily_adj['hfq_close'] / latest_factor
    
    # 2. Downsample High Frequency Data to Weekly
    # Risk
    risk_weekly = downsample_daily_to_weekly(risk_df, name="风险因子")
    
    # Tech
    tech_weekly = None
    if tech_df is not None:
        tech_weekly = downsample_daily_to_weekly(tech_df, name="技术因子")
        
    # Market Cap (Weekly Last)
    mv_weekly = downsample_daily_to_weekly(daily_basic, name="市值数据")
    
    # Prices (Weekly Open/Close QFQ)
    # Custom downsampling for prices: Open=First, Close=Last
    print("Downsampling QFQ prices to Weekly...")
    daily_adj['week'] = daily_adj['trade_date'].dt.to_period('W-FRI')
    
    price_weekly_agg = daily_adj.groupby(['ts_code', 'week']).agg({
        'qfq_open': 'first',
        'qfq_close': 'last',
        'trade_date': 'last' # Use the Friday date
    }).reset_index()
    
    # Recover trade_date
    price_weekly_agg['trade_date'] = price_weekly_agg['week'].dt.end_time
    price_weekly_agg = price_weekly_agg.drop(columns=['week'])
    
    # Rename
    price_weekly_agg = price_weekly_agg.rename(columns={'qfq_open': 'weekly_open', 'qfq_close': 'weekly_close'})
    
    # 3. Merge Strategy (Left = Weekly Backbone, Right = Low Freq Fund via merge_asof)
    print("正在合并数据集 (Base: Weekly Risk)...")
    
    # Ensure all trade_dates are datetime
    risk_weekly['trade_date'] = pd.to_datetime(risk_weekly['trade_date'])
    fund_df['trade_date'] = pd.to_datetime(fund_df['trade_date'])
    
    # Sort for merge_asof
    risk_weekly = risk_weekly.sort_values('trade_date')
    fund_df = fund_df.sort_values('trade_date')
    
    # Rename Fund's trade_date to avoid collision/confusion, or keep it as match key
    # merge_asof on 'trade_date'.
    # We want to attach Fund Data to Risk Data.
    # Note: merge_asof requires sorting.
    
    print("  Merging Fundamental Factors...")
    # Fundamental usually has 'ret' (monthly return). We probably want to keep that? 
    # Or rely on weekly return calculated from price?
    # Let's assume we merge everything.
    merged = pd.merge_asof(
        risk_weekly, 
        fund_df, 
        on='trade_date', 
        by='ts_code', 
        direction='backward',
        suffixes=('', '_fund') # If collision
    )
    
    if tech_weekly is not None:
        print("  Merging Technical Factors...")
        # Tech matches Risk exactly on weekly grid (same aggregation)
        # So we can use regular merge on [ts_code, trade_date]
        tech_weekly['trade_date'] = pd.to_datetime(tech_weekly['trade_date'])
        merged = pd.merge(merged, tech_weekly, on=['ts_code', 'trade_date'], how='left')
        
    print("  Merging Market Cap...")
    mv_weekly['trade_date'] = pd.to_datetime(mv_weekly['trade_date'])
    merged = pd.merge(merged, mv_weekly[['ts_code', 'trade_date', 'total_mv']], on=['ts_code', 'trade_date'], how='left')
    
    print("  Merging Weekly Prices...")
    price_weekly_agg['trade_date'] = pd.to_datetime(price_weekly_agg['trade_date'])
    merged = pd.merge(merged, price_weekly_agg[['ts_code', 'trade_date', 'weekly_open', 'weekly_close']], on=['ts_code', 'trade_date'], how='left')
    
    print(f"合并后初步形状: {merged.shape}")
    
    # 4. Data Enrichment
    print("正在计算 Roe...")
    if 'Ep' in merged.columns and 'Bm' in merged.columns:
        merged['Roe'] = merged['Ep'] / merged['Bm']
        merged['Roe'] = merged['Roe'].replace([np.inf, -np.inf], np.nan)
    else:
        print("警告: Ep 或 Bm 缺失。跳过 Roe 计算。")
        merged['Roe'] = np.nan
        
    # 5. Universe Filtering
    print("正在应用股票池过滤 (剔除市值后 30%)...")
    merged = Universe.apply_market_cap_filter(merged, threshold_percent=0.3)
    
    # 6. Target Generation
    print("正在创建 next_ret...")
    merged = merged.sort_values(['ts_code', 'trade_date'])
    
    # Calculate Weekly Return for next period
    # If 'ret' exists (from Risk/Tech aggregation), we can shift it.
    # 'ret' in Risk usually is daily return. We aggregated it to 'sum' (approx weekly return).
    if 'ret' in merged.columns:
        merged['next_ret'] = merged.groupby('ts_code')['ret'].shift(-1)
    else:
        print("警告: 未找到 'ret' 列，无法计算 next_ret。")
    
    # 7. Final Cleanup
    print("正在完成...")
    merged = merged.dropna(subset=['next_ret'])
    
    # Set index
    merged = merged.set_index(['trade_date', 'ts_code']).sort_index()
    
    print(f"最终形状: {merged.shape}")
    print(f"正在保存至 {output_path}...")
    merged.to_parquet(output_path)
    print("完成。")

if __name__ == "__main__":
    finalize_dataset()
