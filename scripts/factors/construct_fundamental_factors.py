
import pandas as pd
import numpy as np
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from data.data_loader import load_data, RAW_DATA_DIR, WHITELIST_PATH
from factor_library import (
    OCFtoNI, APTurnover, APDays, FATurnover, IntCoverage, TaxRate,
    OpAssetChg, EquityRatio, NOAT, FARatio
)

def construct_factors():
    print("Constructing fundamental factors...")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    factors_dir = os.path.join(base_dir, 'data', 'factors')
    os.makedirs(factors_dir, exist_ok=True)
    
    # 1. Load Market Data (Daily) for alignment and basic factors
    print("Loading daily market data...")
    daily = load_data('daily', columns=['close'])
    daily_basic = load_data('daily_basic', columns=['total_mv'])
    
    # Merge daily and daily_basic
    market_data = pd.merge(daily, daily_basic, on=['ts_code', 'trade_date'], how='inner')
    
    # Resample to Monthly for final output
    print("Resampling market data to monthly...")
    market_data['month'] = market_data['trade_date'].dt.to_period('M')
    market_data = market_data.sort_values('trade_date')
    
    # Calculate Monthly Return (ret) and Size (size)
    # Group by ts_code and month
    def get_monthly_market(x):
        d = {}
        d['close'] = x['close'].iloc[-1]
        d['total_mv'] = x['total_mv'].iloc[-1] # Month-end market cap
        d['trade_date'] = x['trade_date'].iloc[-1] # Month-end date
        
        # Calculate monthly return: P_t / P_{t-1} - 1
        # We can approximate this by taking the last close of this month / last close of prev month - 1
        # But here we are inside a groupby, so we only have this month's data.
        # Better approach: Calculate daily returns and compound them, or just take pct_change of monthly close later.
        return pd.Series(d)

    monthly_market = market_data.groupby(['ts_code', 'month']).apply(get_monthly_market).reset_index()
    
    # Calculate 'ret' (Monthly Return)
    # Sort by ts_code and month to ensure correct shift
    monthly_market = monthly_market.sort_values(['ts_code', 'month'])
    monthly_market['ret'] = monthly_market.groupby('ts_code')['close'].pct_change()
    
    # Rename total_mv to size for factor consistency
    monthly_market['size'] = monthly_market['total_mv']
    
    # 2. Load Financial Data
    print("Loading financial data...")
    
    # Load whitelist to get valid ts_codes
    whitelist = pd.read_parquet(WHITELIST_PATH, columns=['ts_code'])
    valid_stocks = set(whitelist['ts_code'].unique())
    
    def load_financials(filename, columns=None):
        path = os.path.join(RAW_DATA_DIR, filename)
        if not os.path.exists(path):
            print(f"Warning: {path} not found.")
            return pd.DataFrame()
        
        # Determine columns to load
        if columns:
            cols_to_load = ['ts_code', 'ann_date', 'end_date'] + columns
            cols_to_load = list(set(cols_to_load))
        else:
            cols_to_load = None # Load all
            
        # Read parquet
        try:
            df = pd.read_parquet(path, columns=cols_to_load)
        except Exception as e:
            print(f"Error loading {filename}: {e}. Loading all columns.")
            df = pd.read_parquet(path)
            
        # Filter by universe
        df = df[df['ts_code'].isin(valid_stocks)]
        # Ensure dates
        if 'ann_date' in df.columns:
            df['ann_date'] = pd.to_datetime(df['ann_date'].astype(str))
        if 'end_date' in df.columns:
            df['end_date'] = pd.to_datetime(df['end_date'].astype(str))
        return df

    # Load specific columns needed for Bm and Ep + others for new factors
    # Note: The new factors classes handle their own column requirements, but we need to ensure
    # the base dataframe passed to them has all needed columns.
    # Since we don't know exactly what columns every factor needs without inspecting them, 
    # we will load the full files or a broad set. 
    # To be safe and simple given the context, let's load all columns for now, 
    # but strictly ensure Bm/Ep columns are present.
    
    bs = load_financials('balancesheet.parquet')
    inc = load_financials('income.parquet')
    cf = load_financials('cashflow.parquet')
    
    # Merge Financials
    print("Merging financial statements...")
    
    # Start with Income Statement
    financial_df = inc.copy()
    
    # Merge Balance Sheet
    if not bs.empty:
        financial_df = pd.merge(financial_df, bs, on=['ts_code', 'end_date', 'ann_date'], how='outer', suffixes=('', '_bs'))
        
    # Merge Cash Flow
    if not cf.empty:
        financial_df = pd.merge(financial_df, cf, on=['ts_code', 'end_date', 'ann_date'], how='outer', suffixes=('', '_cf'))
        
    # Sort for TTM calculation
    financial_df = financial_df.sort_values(['ts_code', 'end_date'])
    
    # 3. Calculate New Factors (Class-based)
    print("Calculating new class-based factors...")
    
    factors = [
        OCFtoNI(), APTurnover(), APDays(), FATurnover(), IntCoverage(),
        TaxRate(), OpAssetChg(), EquityRatio(), NOAT(), FARatio()
    ]
    
    # Store results
    factor_results = []
    
    for factor in factors:
        print(f"Calculating {factor.name}...")
        try:
            # Check if required columns exist (simple check, factor might fail inside otherwise)
            # We rely on the try-except block to handle missing columns gracefully
            res = factor.calculate(financial_df)
            factor_results.append(res)
        except Exception as e:
            print(f"Error calculating {factor.name}: {e}")
            # We can append an empty DF or just skip. Skipping is safer.
            
    # Merge all factor results
    if not factor_results:
        print("No new factors calculated.")
        all_factors = financial_df[['ts_code', 'ann_date', 'end_date']].copy()
    else:
        all_factors = factor_results[0]
        for i in range(1, len(factor_results)):
            all_factors = pd.merge(all_factors, factor_results[i], on=['ts_code', 'end_date', 'ann_date'], how='outer')
    
    # Add raw fields needed for Bm and Ep to all_factors so they survive the merge
    # Bm needs: total_hldr_eqy_exc_min_int (from BS)
    # Ep needs: n_income_attr_p (from Income)
    
    cols_to_preserve = []
    if 'total_hldr_eqy_exc_min_int' in financial_df.columns:
        cols_to_preserve.append('total_hldr_eqy_exc_min_int')
    if 'n_income_attr_p' in financial_df.columns:
        cols_to_preserve.append('n_income_attr_p')
        
    if cols_to_preserve:
        # Merge these columns into all_factors
        # We need to be careful about duplicates if all_factors already has them (unlikely from factor calc)
        temp_df = financial_df[['ts_code', 'end_date', 'ann_date'] + cols_to_preserve]
        all_factors = pd.merge(all_factors, temp_df, on=['ts_code', 'end_date', 'ann_date'], how='left')

    # 4. Merge to Market Data (Avoid Look-ahead Bias)
    print("Merging factors to market data (asof)...")
    
    # Ensure ann_date is valid
    all_factors = all_factors.dropna(subset=['ann_date']).sort_values('ann_date')
    monthly_market = monthly_market.sort_values('trade_date')
    
    # Merge using merge_asof
    merged = pd.merge_asof(
        monthly_market,
        all_factors,
        left_on='trade_date',
        right_on='ann_date',
        by='ts_code',
        direction='backward'
    )
    
    # 5. Calculate Bm and Ep
    print("Calculating Bm and Ep...")
    
    # Bm = Book / Market
    if 'total_hldr_eqy_exc_min_int' in merged.columns and 'total_mv' in merged.columns:
        merged['Bm'] = merged['total_hldr_eqy_exc_min_int'] / merged['total_mv']
    else:
        print("Warning: Missing columns for Bm calculation.")
        merged['Bm'] = np.nan
        
    # Ep = Earnings / Price (Market Cap)
    if 'n_income_attr_p' in merged.columns and 'total_mv' in merged.columns:
        merged['Ep'] = merged['n_income_attr_p'] / merged['total_mv']
    else:
        print("Warning: Missing columns for Ep calculation.")
        merged['Ep'] = np.nan

    # 6. Save
    output_path = os.path.join(factors_dir, 'fundamental_factors.parquet')
    print(f"Saving to {output_path}...")
    
    # Select columns
    # Basic: ts_code, trade_date, month, ret, size
    # Original: Bm, Ep
    # New: [f.name for f in factors]
    
    base_cols = ['ts_code', 'trade_date', 'month', 'ret', 'size', 'Bm', 'Ep']
    new_factor_cols = [f.name for f in factors]
    
    cols_to_keep = base_cols + new_factor_cols
    
    # Filter existing columns
    cols_to_keep = [c for c in cols_to_keep if c in merged.columns]
    
    final_df = merged[cols_to_keep].copy()
    final_df = final_df.set_index(['trade_date', 'ts_code']).sort_index()
    
    final_df.to_parquet(output_path)
    print("Done.")
    print("Sample Output:")
    print(final_df.tail())

if __name__ == "__main__":
    construct_factors()
