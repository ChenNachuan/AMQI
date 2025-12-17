"""Download balance sheet data from Tushare."""
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
BATCH_SIZE = 5


def download_balancesheet(refresh_stock_list: bool = False):
    """Download balance sheet data using dynamic ts_code universe, 5 stocks at a time."""
    print("=" * 60)
    print(f"Downloading Balance Sheet Data (资产负债表) - Batch Size: {BATCH_SIZE}")
    print("=" * 60)

    pro = init_tushare_api()

    # Load stock list
    stock_df = load_stock_basic(pro=pro, refresh=refresh_stock_list)
    
    # Filter stocks listed before END_DATE and not delisted before START_DATE
    # Simple filtering to avoid unnecessary requests
    mask = (pd.to_datetime(stock_df['list_date']) <= pd.to_datetime(END_DATE))
    # Note: Tushare checks automatically, but we can filter. 
    # Current load_stock_basic returns all.
    # We will just take all codes, Tushare returns empty if no data.
    
    ts_codes = stock_df['ts_code'].tolist()
    print(f"\nTargeting {len(ts_codes):,} securities from {START_DATE} to {END_DATE}")

    all_data = []

    # Batch processing
    for i in range(0, len(ts_codes), BATCH_SIZE):
        batch_codes = ts_codes[i : i + BATCH_SIZE]
        codes_str = ",".join(batch_codes)
        
        try:
            df = pro.balancesheet(ts_code=codes_str, start_date=START_DATE, end_date=END_DATE)
            if not df.empty:
                all_data.append(df)
            
            # Progress logging
            log_progress(i + len(batch_codes), len(ts_codes), f"Processed batch ending {batch_codes[-1]}")
            
        except Exception as exc:
            print(f"Error downloading batch starting {batch_codes[0]}: {exc}")
        finally:
            time.sleep(SLEEP_SECONDS)

    if all_data:
        print("\nCombining all data...")
        final_df = pd.concat(all_data, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=["ts_code", "end_date", "report_type"], keep="last")
        save_to_parquet(final_df, "balancesheet")
        print("\n✓ Balance sheet download completed!")
        print(f"  Total records: {len(final_df):,}")
    else:
        print("\n✗ No data was downloaded")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download balance sheet data from Tushare")
    parser.add_argument(
        "--refresh-stock-list",
        action="store_true",
        help="Refresh stock_basic cache before downloading",
    )
    args = parser.parse_args()
    download_balancesheet(refresh_stock_list=args.refresh_stock_list)
