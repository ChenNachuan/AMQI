"""
Download dividend and stock split data from Tushare.
分红送股 - Dividend Data
"""
import argparse
import sys
import time
from pathlib import Path

import pandas as pd

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from data.data_loader.utils import (
    init_tushare_api,
    load_stock_basic,
    log_progress,
    save_to_parquet,
)

START_DATE = "20230101"
END_DATE = "20251215"
SLEEP_SECONDS = 0.4
# User requested 10 originally, then 100 failed. 
# Tushare dividend API might have limits on number of codes per query.
# Let's use a safe batch size of 50.
BATCH_SIZE = 1


def download_dividend(refresh_stock_list: bool = False):
    """Download dividend data using dynamic ts_code universe, batch processing."""
    print("=" * 60)
    print(f"Downloading Dividend Data (分红送股) - Batch Size: {BATCH_SIZE}")
    print("=" * 60)

    # Initialize API
    pro = init_tushare_api()
    
    # Load stock list
    stock_df = load_stock_basic(pro=pro, refresh=refresh_stock_list)
    ts_codes = stock_df['ts_code'].tolist()
    total_codes = len(ts_codes)
    
    print(f"\nTargeting {total_codes:,} securities")
    
    all_data = []
    
    # Batch processing
    for i in range(0, total_codes, BATCH_SIZE):
        # Explicitly calculate end index to handle the last batch safely
        end_idx = min(i + BATCH_SIZE, total_codes)
        batch_codes = ts_codes[i : end_idx]
        
        if not batch_codes:
            continue
            
        codes_str = ",".join(batch_codes)
        
        try:
            # Query dividend data
            # Retry logic could be added here if needed
            df = pro.dividend(ts_code=codes_str, start_date=START_DATE, end_date=END_DATE)
            
            if not df.empty:
                all_data.append(df)
            
            # Progress logging
            last_stock = batch_codes[-1]
            log_progress(end_idx, total_codes, f"Processed batch ending {last_stock}")
            
        except Exception as exc:
            print(f"Error downloading batch {i}-{end_idx}: {exc}")
        finally:
            time.sleep(SLEEP_SECONDS)

    # Concatenate all data
    if all_data:
        print("\nCombining all data...")
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates
        final_df = final_df.drop_duplicates(subset=['ts_code', 'end_date', 'ann_date'], keep='last')
        
        # Save to parquet
        save_to_parquet(final_df, 'dividend')
        
        print(f"\n✓ Dividend data download completed!")
        print(f"  Total records: {len(final_df):,}")
    else:
        print("\n✗ No data was downloaded. Please check date range or token permissions.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download dividend data from Tushare")
    parser.add_argument(
        "--refresh-stock-list",
        action="store_true",
        help="Refresh stock_basic cache before downloading",
    )
    args = parser.parse_args()
    download_dividend(refresh_stock_list=args.refresh_stock_list)
