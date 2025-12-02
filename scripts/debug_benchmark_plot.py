
import pandas as pd
import numpy as np
import sys
import os
import matplotlib.pyplot as plt

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backtest.engine import BacktestEngine

def debug_benchmark_plot():
    print("Debugging Benchmark Plot...")
    
    data_path = 'data/final_dataset.parquet'
    bench_path = 'data/benchmark_csi300_monthly.parquet'
    
    if not os.path.exists(data_path) or not os.path.exists(bench_path):
        print("Data files not found.")
        return
        
    df = pd.read_parquet(data_path)
    bench_df = pd.read_parquet(bench_path)
    
    print(f"Benchmark Data:\n{bench_df.head()}")
    
    factor = 'ATR'
    engine = BacktestEngine(df, factor_name=factor, benchmark_df=bench_df)
    
    print("Running Analysis...")
    engine.run_analysis(weighting='vw')
    
    print("Plotting Results (Check if Benchmark appears)...")
    # This will show the plot window if running locally, or just execute the code
    try:
        engine.plot_results()
        print("Plot function executed successfully.")
    except Exception as e:
        print(f"Error plotting: {e}")

if __name__ == "__main__":
    debug_benchmark_plot()
