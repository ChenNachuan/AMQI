
import pandas as pd
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from factor_library import Momentum, Universe
from data.data_loader import load_data

def verify_logic():
    print("Verifying Logic Fixes...")
    
    # 1. Test Daily Rolling Momentum
    print("\n1. Testing Daily Rolling Momentum...")
    # Load data for 2005-2007 (sufficient history)
    df_mom = load_data('daily', columns=['close'], start_date='2005-01-01', end_date='2007-01-01', filter_universe=True)
    
    mom = Momentum()
    print(f"Calculating {mom.name}...")
    res = mom.calculate(df_mom)
    
    print("Result head:")
    print(res.head())
    
    # Check if index is daily (same as input)
    if len(res) == len(df_mom):
        print(f"PASS: Output length matches input ({len(res)}). Daily frequency preserved.")
    else:
        print(f"FAIL: Output length mismatch. Input: {len(df_mom)}, Output: {len(res)}.")
        
    # Check for valid values
    valid_count = res[mom.name].notna().sum()
    print(f"Valid {mom.name} values: {valid_count}")
    
    # 2. Test Universe Filter
    print("\n2. Testing Universe Filter...")
    # Create dummy data
    dates = pd.date_range(start='2020-01-01', periods=5)
    data = []
    for date in dates:
        # Create 10 stocks with market cap 10, 20, ..., 100
        for i in range(10):
            data.append({
                'trade_date': date,
                'ts_code': f'STOCK_{i}',
                'total_mv': (i + 1) * 10
            })
    df_univ = pd.DataFrame(data)
    
    print("Original shape:", df_univ.shape)
    
    # Apply filter (bottom 30% should be removed)
    # Threshold is 30th percentile.
    # For 10, 20, ..., 100, 30th percentile is roughly 30-40 depending on interpolation.
    # Stocks with 10, 20, 30 might be removed.
    
    filtered = Universe.apply_market_cap_filter(df_univ, threshold_percent=0.3)
    print("Filtered shape:", filtered.shape)
    
    # Check count per date
    counts = filtered.groupby('trade_date').size()
    print("Counts per date:")
    print(counts)
    
    if (counts < 10).all():
        print("PASS: Filter removed stocks.")
    else:
        print("FAIL: Filter did not remove stocks.")
        
    # 3. Run Merge Script (Integration Test)
    print("\n3. Running Merge Script...")
    # This might take time, so we just check if it runs without error
    # and if the output file is created.
    # We can use os.system or just import and run if it's fast enough.
    # Given the previous run took ~1 min, let's try running it.
    from data.merge_factors import merge_factors
    try:
        merge_factors()
        print("PASS: Merge script ran successfully.")
    except Exception as e:
        print(f"FAIL: Merge script failed with error: {e}")

if __name__ == "__main__":
    verify_logic()
