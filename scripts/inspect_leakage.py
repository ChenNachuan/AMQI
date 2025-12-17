
import pandas as pd
import numpy as np
import os

def inspect_leakage():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dataset_path = os.path.join(base_dir, 'data', 'final_dataset.parquet')
    
    if not os.path.exists(dataset_path):
        print("Dataset not found.")
        return

    print(f"Loading {dataset_path}...")
    df = pd.read_parquet(dataset_path)
    
    if 'next_ret' not in df.columns:
        print("Target 'next_ret' not found in dataset. Cannot check leakage.")
        return
        
    print(f"Dataset Shape: {df.shape}")
    
    # Select numeric columns
    numeric_df = df.select_dtypes(include=[np.number])
    
    # Remove non-feature columns if they exist (ts_code is usually not numeric, but date might be converted)
    # We want to check EVERY numeric column against next_ret
    
    print("Calculating correlations with 'next_ret'...")
    corrs = numeric_df.corrwith(df['next_ret']).abs().sort_values(ascending=False)
    
    print("-" * 50)
    print("Top 20 Correlated Features with Target (next_ret):")
    print("-" * 50)
    print(corrs.head(20))
    
    # Heuristic Checks
    potential_leaks = corrs[corrs > 0.2]
    # Remove next_ret itself
    potential_leaks = potential_leaks.drop('next_ret', errors='ignore')
    
    if not potential_leaks.empty:
        print("\n" + "!" * 50)
        print("WARNING: POTENTIAL LEAKAGE DETECTED (Corr > 0.2)")
        print("!" * 50)
        print(potential_leaks)
        print("Note: Financial returns typically have correlations < 0.1 with past features.")
    else:
        print("\nPASS: No anomalously high correlations found (all features < 0.2).")
        
    # Check shift logic specifically
    if 'ret' in df.columns:
        print("\nCorrelation between 'ret' (current) and 'next_ret' (target):")
        print(df[['ret', 'next_ret']].corr().iloc[0, 1])

if __name__ == "__main__":
    inspect_leakage()
