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
    iter_ts_code_date_ranges,
    load_stock_basic,
    log_progress,
    save_to_parquet,
)

START_DATE = "20000101"
END_DATE = "20251231"
SLEEP_SECONDS = 0.12


def download_fina_indicator(refresh_stock_list: bool = False):
    """Download financial indicator data using dynamic ts_code universe."""
    print("=" * 60)
    print("Downloading Financial Indicator Data (财务指标数据)")
    print("=" * 60)

    pro = init_tushare_api()

    stock_df = load_stock_basic(pro=pro, refresh=refresh_stock_list)
    code_ranges = list(iter_ts_code_date_ranges(stock_df, START_DATE, END_DATE))
    print(f"\nTracking {len(code_ranges):,} securities from {START_DATE} to {END_DATE}")

    all_data = []

    for idx, (ts_code, start_date, end_date) in enumerate(code_ranges, 1):
        try:
            df = pro.fina_indicator(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if not df.empty:
                all_data.append(df)
            if idx % 50 == 0 or not df.empty:
                log_progress(idx, len(code_ranges), f"Processed {ts_code}")
        except Exception as exc:
            print(f"Error downloading {ts_code} ({start_date}-{end_date}): {exc}")
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
