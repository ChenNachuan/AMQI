import pandas as pd
import os

def check_parquet():
    files = [
        'data/factors/risk_factors.parquet',
        'data/factors/fundamental_factors.parquet',
        'data/raw_data/daily_basic.parquet'
    ]
    
    for path in files:
        if not os.path.exists(path):
            print(f"{path} not found.")
            continue

        print(f"\nChecking {path}...")
        try:
            print("Trying pyarrow...")
            df = pd.read_parquet(path, engine='pyarrow')
            print("pyarrow success.")
        except Exception as e:
            print(f"pyarrow failed: {e}")
            
        try:
            print("Trying fastparquet...")
            df = pd.read_parquet(path, engine='fastparquet')
            print("fastparquet success.")
        except Exception as e:
            print(f"fastparquet failed: {e}")

if __name__ == "__main__":
    check_parquet()
