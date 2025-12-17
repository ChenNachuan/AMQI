"""Download financial indicator data from Tushare."""
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
BATCH_SIZE = 50


def download_fina_indicator(refresh_stock_list: bool = False):
    """Download financial indicator data using dynamic ts_code universe, 5 stocks at a time."""
    print("=" * 60)
    print(f"Downloading Financial Indicator Data (财务指标数据) - Batch Size: {BATCH_SIZE}")
    print("=" * 60)

    pro = init_tushare_api()

    stock_df = load_stock_basic(pro=pro, refresh=refresh_stock_list)
    ts_codes = stock_df['ts_code'].tolist()
    print(f"\nTargeting {len(ts_codes):,} securities from {START_DATE} to {END_DATE}")

    all_data = []

    for i in range(0, len(ts_codes), BATCH_SIZE):
        batch_codes = ts_codes[i : i + BATCH_SIZE]
        codes_str = ",".join(batch_codes)
        
        try:
            df = pro.fina_indicator(ts_code=codes_str, start_date=START_DATE, end_date=END_DATE)
            if not df.empty:
                all_data.append(df)
            log_progress(i + len(batch_codes), len(ts_codes), f"Processed batch ending {batch_codes[-1]}")
        except Exception as exc:
            print(f"Error downloading batch starting {batch_codes[0]}: {exc}")
        finally:
            time.sleep(SLEEP_SECONDS)

    if all_data:
        print("\nCombining all data...")
        final_df = pd.concat(all_data, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=["ts_code", "end_date"], keep="last")
        save_to_parquet(final_df, "fina_indicator")
        print("\n✓ Financial indicator download completed!")
        print(f"  Total records: {len(final_df):,}")
    else:
        print("\n✗ No data was downloaded")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download financial indicator data from Tushare")
    parser.add_argument(
        "--refresh-stock-list",
        action="store_true",
        help="Refresh stock_basic cache before downloading",
    )
    args = parser.parse_args()
    download_fina_indicator(refresh_stock_list=args.refresh_stock_list)
