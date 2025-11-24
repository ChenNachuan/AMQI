"""Download cash flow statement data from Tushare."""
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
END_DATE = "20251031"
SLEEP_SECONDS = 0.2


def download_cashflow(refresh_stock_list: bool = False):
    """Download cash flow statement data using dynamic ts_code universe."""
    print("=" * 60)
    print("Downloading Cash Flow Statement Data (现金流量表)")
    print("=" * 60)

    pro = init_tushare_api()

    stock_df = load_stock_basic(pro=pro, refresh=refresh_stock_list)
    code_ranges = list(iter_ts_code_date_ranges(stock_df, START_DATE, END_DATE))
    print(f"\nTracking {len(code_ranges):,} securities from {START_DATE} to {END_DATE}")

    all_data = []

    for idx, (ts_code, start_date, end_date) in enumerate(code_ranges, 1):
        try:
            df = pro.cashflow(ts_code=ts_code, start_date=start_date, end_date=end_date)
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
        final_df = final_df.drop_duplicates(subset=["ts_code", "end_date", "report_type"], keep="last")
        save_to_parquet(final_df, "cashflow")
        print("\n✓ Cash flow statement download completed!")
        print(f"  Total records: {len(final_df):,}")
    else:
        print("\n✗ No data was downloaded")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download cash flow statement data from Tushare")
    parser.add_argument(
        "--refresh-stock-list",
        action="store_true",
        help="Refresh stock_basic cache before downloading",
    )
    args = parser.parse_args()
    download_cashflow(refresh_stock_list=args.refresh_stock_list)
