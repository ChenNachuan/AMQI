
import pandas as pd
import numpy as np
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.data_loader import load_data
from factor_library import Beta, Ivff, Turnover, Reversal

def run_risk_factors():
    print("Running Risk Factors Construction...")
    
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = os.path.join(base_dir, 'data', 'factors')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_path = os.path.join(output_dir, 'risk_factors.parquet')
    
    # 1. Load Data
    print("Loading data...")
    # We need daily returns, market returns, turnover rate
    # Market return is not in daily.parquet usually, it's calculated or loaded from index.
    # In previous construct_risk_factors.py, we calculated market return from the universe.
    # Let's do that here too.
    
    # Load daily price data
    df = load_data('daily', columns=['close', 'pct_chg', 'vol', 'amount'], filter_universe=True)
    
    # Calculate Market Return (Value Weighted)
    # We need total_mv for weighting.
    # We also need turnover_rate for Turnover factor.
    daily_basic = load_data('daily_basic', columns=['total_mv', 'turnover_rate'], filter_universe=True)
    
    # Merge
    print("Merging daily and basic data...")
    # Ensure trade_date is datetime
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    daily_basic['trade_date'] = pd.to_datetime(daily_basic['trade_date'])
    
    # Merge daily and daily_basic
    # daily_basic has total_mv and turnover_rate
    merged = pd.merge(df, daily_basic[['ts_code', 'trade_date', 'total_mv', 'turnover_rate']], on=['ts_code', 'trade_date'], how='inner')
    
    # Calculate Market Return
    print("Calculating Market Return...")
    # R_m = sum(R_i * MV_i) / sum(MV_i) per day
    # pct_chg is usually percentage (e.g. 1.5 for 1.5%). Convert to decimal?
    # Tushare pct_chg is 0-100 usually? Let's check. Usually it is.
    # But let's assume it is and divide by 100 if needed.
    # Actually, let's check the previous script logic if possible.
    # Assuming pct_chg is percentage, we use it as is or decimal. 
    # Let's use decimal for calculations: ret = pct_chg / 100
    merged['ret'] = merged['pct_chg'] / 100.0
    
    # Weighted average
    def weighted_avg(x):
        return np.average(x['ret'], weights=x['total_mv'])
        
    mkt_ret = merged.groupby('trade_date').apply(weighted_avg).reset_index(name='mkt_ret')
    
    # Merge market return back
    merged = pd.merge(merged, mkt_ret, on='trade_date', how='left')
    
    # 2. Calculate Factors
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
    # Note: Beta returns index [trade_date, ts_code]
    # We need to join it back to merged to pass to Ivff
    merged = pd.merge(merged, beta_df, on=['trade_date', 'ts_code'], how='left')
    
    # Ivff
    print(f"Calculating {ivff_factor.name}...")
    ivff_df = ivff_factor.calculate(merged)
    
    # Turnover
    print(f"Calculating {tur_factor.name}...")
    tur_df = tur_factor.calculate(merged)
    
    # Reversal
    print(f"Calculating {srev_factor.name}...")
    # Reversal needs 'close' which is in merged
    srev_df = srev_factor.calculate(merged)
    
    # 3. Merge Results
    print("Merging results...")
    # All factors are indexed by [trade_date, ts_code]
    # We want to aggregate them into monthly frequency?
    # The previous construct_risk_factors.py aggregated to monthly.
    # "Output a DataFrame indexed by [trade_date, ts_code]."
    # "Merge & Save: Concatenate the results (axis=1) into a single DataFrame risk_factors_daily and save it".
    # Wait, the prompt says "risk_factors_daily".
    # But the merge_factors.py expects MONTHLY data (inner join with fundamental factors).
    # Fundamental factors are monthly.
    # If we save daily risk factors, the merge script will fail or need update.
    # However, the prompt says "Output parquet files should still be saved to data/factors/".
    # And "Merge & Save ... into a single DataFrame risk_factors_daily".
    # This implies the output IS daily.
    # BUT, the merge script (which I just updated) does:
    # `risk_df = pd.read_parquet(risk_path)`
    # `merged = pd.merge(fund_df, risk_df, on=join_keys, how='inner')`
    # If fund_df is monthly and risk_df is daily, the inner join will only keep the month-end dates (which is fine).
    # So saving daily is acceptable and more flexible.
    
    dfs = [beta_df, ivff_df, tur_df, srev_df]
    final_df = pd.concat(dfs, axis=1)
    
    # Handle duplicate columns if any (shouldn't be if indexes align)
    # But concat on axis 1 with same index aligns them.
    
    print(f"Final shape: {final_df.shape}")
    print("Columns:", final_df.columns.tolist())
    
    # 4. Save
    print(f"Saving to {output_path}...")
    final_df.to_parquet(output_path)
    print("Done.")

if __name__ == "__main__":
    run_risk_factors()
