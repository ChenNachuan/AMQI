import pandas as pd
import os

def check_risk_beta():
    path = 'data/factors/risk_factors.parquet'
    if not os.path.exists(path):
        print(f"{path} not found.")
        return

    print(f"Reading {path}...")
    try:
        df = pd.read_parquet(path, engine='fastparquet')
        print(f"Total rows: {len(df)}")
        
        if 'beta' in df.columns:
            print("Beta stats:")
            print(df['beta'].describe())
            print(f"NaN count: {df['beta'].isna().sum()}")
            print(f"Valid count: {df['beta'].count()}")
            
            # Check date range
            if 'trade_date' in df.columns:
                 print(f"Date range: {df['trade_date'].min()} to {df['trade_date'].max()}")
            elif isinstance(df.index, pd.MultiIndex):
                 dates = df.index.get_level_values('trade_date')
                 print(f"Date range: {dates.min()} to {dates.max()}")
                 
        else:
            print("Beta column not found.")
            
    except Exception as e:
        print(f"Error reading file: {e}")

if __name__ == "__main__":
    check_risk_beta()
