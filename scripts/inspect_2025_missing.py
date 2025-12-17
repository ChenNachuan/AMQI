
import pandas as pd
import os
import sys

def inspect_2025():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dataset_path = os.path.join(base_dir, 'data', 'final_dataset.parquet')
    
    if not os.path.exists(dataset_path):
        print(f"Dataset not found at {dataset_path}")
        return

    print(f"Loading dataset from {dataset_path}...")
    df = pd.read_parquet(dataset_path)
    
    # Reset index to get trade_date col if it's in index
    if 'trade_date' not in df.columns:
        df = df.reset_index()
        
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    
    # Filter for 2025
    print("Filtering for data in 2025...")
    df_2025 = df[df['trade_date'].dt.year == 2025].copy()
    
    if df_2025.empty:
        print("No data found for 2025!")
        # Print date range check
        print(f"Available date range: {df['trade_date'].min()} to {df['trade_date'].max()}")
        return
        
    print(f"2025 Data Shape: {df_2025.shape}")
    print(f"Date Range: {df_2025['trade_date'].min()} to {df_2025['trade_date'].max()}")
    
    # Analyze Missing Values
    null_counts = df_2025.isnull().sum()
    null_pct = (df_2025.isnull().sum() / len(df_2025)) * 100
    
    missing_stats = pd.DataFrame({
        'Missing Values': null_counts,
        'Percentage': null_pct
    })
    
    # Filter for columns with missing values > 0
    missing_stats = missing_stats[missing_stats['Missing Values'] > 0].sort_values('Percentage', ascending=False)
    
    print("-" * 50)
    print("Missing Values Analysis for 2025:")
    print("-" * 50)
    
    if missing_stats.empty:
        print("No missing values found in 2025 data!")
    else:
        print(missing_stats.head(30))
        
        # Check specific key factors
        key_factors = ['DownsideRiskBeta', 'standardized_operating_profit', 'capex_growth_rate', 'Roe', 'Bm', 'Ep']
        print("\nKey Factors Check in 2025:")
        for factor in key_factors:
            if factor in missing_stats.index:
                row = missing_stats.loc[factor]
                print(f"{factor}: {row['Missing Values']} ({row['Percentage']:.2f}%)")
            elif factor in df.columns:
                 print(f"{factor}: 0 (0.00%)")
            else:
                 print(f"{factor}: Not in dataset")

if __name__ == "__main__":
    inspect_2025()
