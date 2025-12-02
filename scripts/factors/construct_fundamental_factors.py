
import pandas as pd
import numpy as np
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from data.data_loader import load_data, RAW_DATA_DIR, WHITELIST_PATH

def construct_factors():
    print("Constructing fundamental factors...")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    factors_dir = os.path.join(base_dir, 'data', 'factors')
    # 1. Load Market Data (Daily)
    # We need 'close' for returns and 'total_mv' for size/valuation
    print("Loading daily market data...")
    # These are daily files, so load_data works perfectly (filters and aligns)
    daily = load_data('daily', columns=['close'])
    daily_basic = load_data('daily_basic', columns=['total_mv'])
    
    # Merge daily and daily_basic
    market_data = pd.merge(daily, daily_basic, on=['ts_code', 'trade_date'], how='inner')
    
    # 2. Resample to Monthly
    print("Resampling to monthly frequency...")
    market_data['month'] = market_data['trade_date'].dt.to_period('M')
    market_data = market_data.sort_values('trade_date')
    monthly_market = market_data.groupby(['ts_code', 'month']).last().reset_index()
    
    # 3. Calculate Market Factors
    print("Calculating market factors (ret, size, R11)...")
    # Load daily_adjusted for returns
    adj_path = os.path.join(RAW_DATA_DIR, 'daily_adjusted.parquet')
    if os.path.exists(adj_path):
        print(f"Loading adjusted prices from {adj_path}...")
        adj_df = pd.read_parquet(adj_path)
        adj_df['trade_date'] = pd.to_datetime(adj_df['trade_date'])
        
        # Merge adj_close into daily_basic (which is actually daily data here? No, daily_basic is daily_basic)
        # We need to merge adj_close into the monthly resampling flow
        
        # Resample adj_close to monthly
        adj_df['month'] = adj_df['trade_date'].dt.to_period('M')
        adj_df = adj_df.sort_values(['ts_code', 'trade_date'])
        monthly_adj = adj_df.groupby(['ts_code', 'month'])['adj_close'].last().reset_index()
        
        # Calculate ret
        monthly_adj['ret'] = monthly_adj.groupby('ts_code')['adj_close'].pct_change()
        
        # Merge ret into monthly_market
        # monthly_market has ['ts_code', 'month', 'close', 'total_mv', 'pe', 'pb']
        monthly_market = pd.merge(monthly_market, monthly_adj[['ts_code', 'month', 'ret']], on=['ts_code', 'month'], how='left')
        
    else:
        print("Warning: daily_adjusted.parquet not found. Using unadjusted close for returns (May be inaccurate).")
        monthly_market = monthly_market.sort_values(['ts_code', 'trade_date'])
        monthly_market['ret'] = monthly_market.groupby('ts_code')['close'].pct_change()
    
    # size: Market Cap
    monthly_market['size'] = monthly_market['total_mv']
    
    # R11: Cumulative return from t-12 to t-2
    def calculate_r11(x):
        p_t2 = x['close'].shift(2)
        p_t13 = x['close'].shift(13)
        return (p_t2 / p_t13) - 1
        
    monthly_market['R11'] = monthly_market.groupby('ts_code').apply(calculate_r11).reset_index(level=0, drop=True)
    
    # 4. Load Financial Data (Optimized)
    print("Loading financial data (direct load)...")
    
    # Load whitelist to get valid ts_codes
    whitelist = pd.read_parquet(WHITELIST_PATH, columns=['ts_code'])
    valid_stocks = set(whitelist['ts_code'].unique())
    
    # Helper to load and filter financial data
    def load_financials(filename, columns):
        path = os.path.join(RAW_DATA_DIR, filename)
        df = pd.read_parquet(path, columns=columns)
        # Filter by universe
        df = df[df['ts_code'].isin(valid_stocks)]
        # Filter missing ann_date
        df = df.dropna(subset=['ann_date'])
        # Convert dates
        df['ann_date'] = pd.to_datetime(df['ann_date'].astype(str))
        return df.sort_values('ann_date')

    bs = load_financials('balancesheet.parquet', ['ts_code', 'ann_date', 'total_hldr_eqy_exc_min_int'])
    inc = load_financials('income.parquet', ['ts_code', 'ann_date', 'n_income_attr_p'])
    
    # 5. Merge Financials (Avoid Look-ahead Bias)
    print("Merging financial data (asof)...")
    
    monthly_market = monthly_market.sort_values('trade_date')
    
    # Merge Equity
    merged = pd.merge_asof(
        monthly_market.sort_values('trade_date'),
        bs.sort_values('ann_date'),
        left_on='trade_date',
        right_on='ann_date',
        by='ts_code',
        direction='backward'
    )
    
    # Merge Net Profit
    merged = pd.merge_asof(
        merged.sort_values('trade_date'),
        inc.sort_values('ann_date'),
        left_on='trade_date',
        right_on='ann_date',
        by='ts_code',
        direction='backward',
        suffixes=('_bs', '_inc')
    )
    
    # 6. Calculate Valuation Factors
    print("Calculating valuation factors (Bm, Ep)...")
    
    # Adjust total_mv to Yuan (it is in 10k)
    market_val_yuan = merged['total_mv'] * 10000
    
    # Bm: Equity / Market Value
    merged['Bm'] = merged['total_hldr_eqy_exc_min_int'] / market_val_yuan
    
    # Ep: Net Profit / Market Value
    merged['Ep'] = merged['n_income_attr_p'] / market_val_yuan
    
    # 7. Final Cleanup
    print("Finalizing...")
    final_cols = ['ts_code', 'trade_date', 'ret', 'size', 'R11', 'Bm', 'Ep']
    final_df = merged[final_cols].copy()
    
    final_df = final_df.set_index(['trade_date', 'ts_code']).sort_index()
    
    output_dir = 'data/factors'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'fundamental_factors.parquet')
    
    print(f"Saving to {output_path}...")
    final_df.to_parquet(output_path)
    
    print("Done.")
    print("Sample Output:")
    print(final_df.tail())

if __name__ == "__main__":
    construct_factors()
