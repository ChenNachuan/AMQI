import pandas as pd
import os

def check_beta():
    path = 'data/final_dataset.parquet'
    if not os.path.exists(path):
        print(f"{path} not found.")
        return

    df = pd.read_parquet(path)
    print(f"Total rows: {len(df)}")
    
    if 'beta' in df.columns:
        print("Beta stats:")
        print(df['beta'].describe())
        print(f"NaN count: {df['beta'].isna().sum()}")
        print(f"Valid count: {df['beta'].count()}")
    else:
        print("Beta column not found.")

if __name__ == "__main__":
    check_beta()
