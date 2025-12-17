
import pandas as pd
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def inspect_factors():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    factors_dir = os.path.join(base_dir, 'data', 'factors')
    
    files = {
        'Fundamental': 'fundamental_factors.parquet',
        'Risk': 'risk_factors.parquet',
        'Technical': 'technical_factors.parquet'
    }
    
    print("=" * 60)
    print("INS PECTING FACTOR FILES")
    print("=" * 60)
    
    for name, filename in files.items():
        path = os.path.join(factors_dir, filename)
        print(f"\n--- {name} Factors ({filename}) ---")
        
        if not os.path.exists(path):
            print(f"FAILED: File not found.")
            continue
            
        try:
            df = pd.read_parquet(path)
            
            # Reset index if necessary to access trade_date
            if 'trade_date' not in df.columns:
                df = df.reset_index()
            
            # Normalize trade_date
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                import numpy as np
                dates = np.sort(df['trade_date'].unique())
                
                start_date = dates.min()
                end_date = dates.max()
                
                # Check frequency
                if len(dates) > 1:
                    diffs = pd.Series(dates).diff().dropna()
                    median_diff = diffs.median()
                    min_diff = diffs.min()
                    max_diff = diffs.max()
                    
                    freq_str = f"Median Diff: {median_diff.days} days"
                    if median_diff.days <= 1:
                         freq_guess = "Daily"
                    elif median_diff.days <= 5:
                        # Friday to Friday is 7, but if trading days gap..
                         freq_guess = "Weekly (approx)"
                    elif median_diff.days >= 28:
                         freq_guess = "Monthly"
                    elif median_diff.days >= 6:
                        freq_guess = "Weekly"
                    else:
                        freq_guess = "Unknown"
                else:
                    freq_guess = "Single Date"
                    median_diff = pd.Timedelta(0)
                    
                print(f"Shape: {df.shape}")
                print(f"Date Range: {start_date} to {end_date}")
                print(f"Frequency Analysis: {freq_guess} ({median_diff})")
                print(f"Unique Dates: {len(dates)}")
                
                # Check 2023-2025 coverage
                target_start = pd.Timestamp('2023-01-01')
                target_end = pd.Timestamp('2025-12-15')
                
                covers_start = start_date <= target_start
                covers_end = end_date >= target_end
                
                print(f"Covers Start (<= 2023-01-01): {covers_start} (Actual: {start_date})")
                print(f"Covers End (>= 2025-12-15): {covers_end} (Actual: {end_date})")
                
                # Missing Values
                null_counts = df.isnull().sum()
                if null_counts.sum() > 0:
                    print(f"Total Missing Values: {null_counts.sum()}")
                    print("Top 5 Missing Columns:")
                    print(null_counts[null_counts > 0].sort_values(ascending=False).head(5))
                else:
                    print("No Missing Values.")
                    
            else:
                print("ERROR: 'trade_date' column not found.")
                
        except Exception as e:
            print(f"ERROR reading file: {e}")

if __name__ == "__main__":
    inspect_factors()
