import pandas as pd
import os

def inspect_data():
    path = 'data/final_dataset.parquet'
    if not os.path.exists(path):
        print(f"{path} not found.")
        return

    df = pd.read_parquet(path)
    print("Index dtype:", df.index.dtype)
    if isinstance(df.index, pd.MultiIndex):
        print("Index levels dtypes:", [l.dtype for l in df.index.levels])
        
    print("\nColumn dtypes:")
    print(df.dtypes)
    
    print("\nHead:")
    print(df.head())
    
    # Check if any column is Period
    for col in df.columns:
        if isinstance(df[col].dtype, pd.PeriodDtype):
            print(f"Column {col} is PeriodDtype")
            
    # Check index levels
    if isinstance(df.index, pd.MultiIndex):
        for i, level in enumerate(df.index.levels):
            if isinstance(level.dtype, pd.PeriodDtype):
                print(f"Index level {i} ({df.index.names[i]}) is PeriodDtype")

if __name__ == "__main__":
    inspect_data()
