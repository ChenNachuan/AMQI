import pandas as pd
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def inspect_dataset():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dataset_path = os.path.join(base_dir, 'data', 'final_dataset.parquet')
    
    if not os.path.exists(dataset_path):
        print(f"Dataset not found at {dataset_path}")
        return

    print(f"Loading dataset from {dataset_path}...")
    df = pd.read_parquet(dataset_path)
    
    print(f"Dataset Shape: {df.shape}")
    print("-" * 30)
    
    # Count nulls
    null_counts = df.isnull().sum()
    null_pct = (df.isnull().sum() / len(df)) * 100
    
    null_stats = pd.DataFrame({
        'Missing Values': null_counts,
        'Percentage': null_pct
    })
    
    # Filter for columns with missing values
    missing_stats = null_stats[null_stats['Missing Values'] > 0].sort_values('Percentage', ascending=False)
    
    if missing_stats.empty:
        print("No missing values found in the dataset!")
    else:
        print("Columns with missing values:")
        print(missing_stats)
        
    print("-" * 30)
    print("Top 20 columns with most missing values:")
    print(missing_stats.head(20))

    # Analyze potential reasons
    print("\nPotential Reasons Analysis:")
    
    # Check if missing values are concentrated in early dates (warmup period)
    if 'trade_date' in df.index.names:
        dates = df.index.get_level_values('trade_date')
    elif 'trade_date' in df.columns:
        dates = df['trade_date']
    else:
        dates = None

    if dates is not None:
        print(f"Date range: {dates.min()} to {dates.max()}")
        # Check missingness by year
        # We can't easily check every column by year in a summary, but we can check if rows with nulls are early
        pass

if __name__ == "__main__":
    inspect_dataset()
