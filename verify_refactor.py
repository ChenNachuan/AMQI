
import pandas as pd
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data.data_loader import load_data
from factor_library import Momentum

def verify_refactoring():
    print("Verifying Refactoring...")
    
    # 1. Test Data Loader (Universe Filtering)
    print("\n1. Testing Data Loader (Universe Filtering)...")
    # Load with filter
    df_filtered = load_data('daily', columns=['close'], start_date='2005-01-01', end_date='2005-02-01', filter_universe=True)
    print(f"Filtered shape: {df_filtered.shape}")
    
    # Load without filter
    df_raw = load_data('daily', columns=['close'], start_date='2005-01-01', end_date='2005-02-01', filter_universe=False)
    print(f"Raw shape: {df_raw.shape}")
    
    if len(df_raw) >= len(df_filtered):
        print("PASS: Raw data is larger or equal to filtered data.")
    else:
        print("FAIL: Raw data is smaller than filtered data (unexpected).")
        
    # 2. Test Momentum Factor
    print("\n2. Testing Momentum Factor...")
    # Load enough data for 1 year lag
    # We need ~13 months of data. Let's load 2005-2007.
    df_mom = load_data('daily', columns=['close'], start_date='2005-01-01', end_date='2007-01-01', filter_universe=True)
    
    mom = Momentum()
    print(f"Calculating {mom.name}...")
    
    # DEBUG INFO
    print("Data Info:")
    print(df_mom.info())
    print("Unique stocks:", df_mom['ts_code'].nunique())
    print("Unique dates:", df_mom['trade_date'].nunique())
    
    counts = df_mom.groupby('ts_code').size()
    print("Max rows for a stock:", counts.max())
    print("Stocks with > 260 rows:", (counts > 260).sum())
    
    res = mom.calculate(df_mom)
    
    print("Result head:")
    print(res.head())
    print("Result tail:")
    print(res.tail())
    
    # Check if we have values
    valid_count = res[mom.name].notna().sum()
    print(f"Valid {mom.name} values: {valid_count}")
    
    if valid_count > 0:
        print("PASS: Momentum calculated successfully.")
    else:
        print("FAIL: Momentum calculation returned all NaNs (might be insufficient history in sample).")

if __name__ == "__main__":
    verify_refactoring()
