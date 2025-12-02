
import pandas as pd
import numpy as np
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backtest.engine import BacktestEngine

def debug_atr():
    print("Debugging ATR Backtest...")
    
    data_path = 'data/final_dataset.parquet'
    df = pd.read_parquet(data_path)
    
    factor = 'ATR'
    if factor not in df.columns:
        print("ATR not found in data.")
        return
        
    print(f"ATR Data Description:\n{df[factor].describe()}")
    
    # Run Engine
    engine = BacktestEngine(df, factor_name=factor)
    
    # We need to manually call calc_factor_returns to inspect intermediate results
    print("Calculating Factor Returns...")
    results = engine.analyzer.calc_factor_returns(weighting='vw')
    
    quintile_rets = results['quintile_returns']
    ls_rets = results['ls_returns']
    
    print("\nQuintile Returns (Mean):")
    print(quintile_rets.mean())
    
    print("\nLong-Short Returns (Mean):")
    print(ls_rets.mean())
    
    print("\nCumulative Returns (End Value):")
    cum_q1 = (1 + quintile_rets[1].fillna(0)).cumprod().iloc[-1]
    cum_q5 = (1 + quintile_rets[5].fillna(0)).cumprod().iloc[-1]
    cum_ls = (1 + ls_rets.fillna(0)).cumprod().iloc[-1]
    
    print(f"Q1 (Low): {cum_q1}")
    print(f"Q5 (High): {cum_q5}")
    print(f"LS: {cum_ls}")
    
    print("\nHead of Quintile Returns:")
    print(quintile_rets.head())
    
    print("\nHead of LS Returns:")
    print(ls_rets.head())

if __name__ == "__main__":
    debug_atr()
