"""
Download daily basic metrics from Tushare.
每日指标 - Daily Basic Metrics (PE, PB, Market Cap, etc.)
"""
import sys
from pathlib import Path
import pandas as pd
import time

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from data.data_loader.utils import init_tushare_api, save_to_parquet, generate_month_ranges, log_progress


def download_daily_basic():
    """Download daily basic metrics month by month."""
    print("="*60)
    print("Downloading Daily Basic Metrics (每日指标)")
    print("="*60)
    
    # Initialize API
    pro = init_tushare_api()
    
    # Generate month ranges from 2000-01 to 2025-11
    months = generate_month_ranges(2000, 2025, 11)
    print(f"\nWill download {len(months)} months of data (2000-01 to 2025-11)")
    
    all_data = []
    
    for i, month in enumerate(months, 1):
        # Calculate start and end dates for the month
        year = int(month[:4])
        mon = int(month[4:])
        
        # Determine the last day of the month
        if mon == 12:
            next_month = f"{year+1}0101"
        else:
            next_month = f"{year}{mon+1:02d}01"
        
        start_date = f"{month}01"
        
        try:
            df = pro.daily_basic(trade_date='', start_date=start_date, end_date=next_month,
                                fields='ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv')
            
            if not df.empty:
                all_data.append(df)
                log_progress(i, len(months), f"Downloaded {month}")
            else:
                print(f"No data for {month}")
            
            # Sleep to avoid API rate limits
            time.sleep(0.3)
            
        except Exception as e:
            print(f"Error downloading {month}: {e}")
            time.sleep(1)
            continue
    
    # Concatenate all data
    if all_data:
        print("\nCombining all data...")
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Save to parquet
        save_to_parquet(final_df, 'daily_basic')
        
        print(f"\n✓ Daily basic metrics download completed!")
        print(f"  Total records: {len(final_df):,}")
        print(f"  Date range: {final_df['trade_date'].min()} to {final_df['trade_date'].max()}")
    else:
        print("\n✗ No data was downloaded")


if __name__ == "__main__":
    download_daily_basic()
