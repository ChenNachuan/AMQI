
import pandas as pd
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backtest.engine import BacktestEngine

def test_backtest():
    print("Testing Backtest Engine...")
    
    data_path = 'data/final_dataset.parquet'
    if not os.path.exists(data_path):
        print(f"Data not found at {data_path}")
        return
        
    df = pd.read_parquet(data_path)
    print(f"Loaded data: {df.shape}")
    
    # Inspect Data Quality
    print("NaN Counts:")
    print(df[['size', 'next_ret', 'beta']].isna().sum())
    print("Head of size:")
    print(df['size'].head())
    
    # Test with 'beta' factor
    factor = 'beta'
    if factor not in df.columns:
        print(f"Factor {factor} not found in columns: {df.columns.tolist()}")
        return
        
    # Create dummy benchmark (Market Mean)
    benchmark_df = df.groupby('trade_date')['next_ret'].mean().reset_index()
    benchmark_df.columns = ['trade_date', 'ret']
    
    engine = BacktestEngine(df, factor_name='beta', benchmark_df=benchmark_df)
    summary = engine.run_analysis(weighting='vw')
    
    print("\nAnalysis Summary:")
    for k, v in summary.items():
        print(f"{k}: {v}")
        
    # Check for new keys
    expected_keys = ['Factor_Autocorr', 'Q5_Turnover', 'Q5_Return', 'Q5_Sharpe', 'Q5_Active_Return']
    missing_keys = [k for k in expected_keys if k not in summary]
    if missing_keys:
        print(f"\nFAILED: Missing keys: {missing_keys}")
        sys.exit(1)
    else:
        print("\nTest passed! All keys present.")

if __name__ == "__main__":
    test_backtest()
