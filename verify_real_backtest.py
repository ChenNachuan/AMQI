import pandas as pd
import sys
import os
import matplotlib.pyplot as plt

# Add project root to path
sys.path.append(os.getcwd())

from backtest.engine import BacktestEngine

def verify_real_backtest():
    print("Verifying Backtest with Real Data...")
    
    path = 'data/final_dataset.parquet'
    if not os.path.exists(path):
        print(f"{path} not found.")
        return

    df = pd.read_parquet(path)
    print(f"Loaded data: {df.shape}")
    
    # Filter for beta
    # Ensure beta is not all NaN
    if 'beta' not in df.columns or df['beta'].isna().all():
        print("Beta column is missing or empty.")
        return
        
    print("Running BacktestEngine for 'beta'...")
    engine = BacktestEngine(df, 'beta')
    
    try:
        summary = engine.run_analysis(weighting='vw')
        print("Analysis completed.")
        print("Summary:", summary)
        
        print("Plotting results (dry run)...")
        engine.plot_results()
        print("Plotting completed.")
        
    except Exception as e:
        print(f"Backtest failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify_real_backtest()
