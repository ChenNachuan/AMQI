"""
Download dividend and stock split data from Tushare.
分红送股 - Dividend Data
"""
import sys
from pathlib import Path
import pandas as pd
import time

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from data.data_loader.utils import init_tushare_api, save_to_parquet, generate_year_ranges, log_progress


def download_dividend():
    """Download dividend data year by year."""
    print("="*60)
    print("Downloading Dividend Data (分红送股)")
    print("="*60)
    
    # Initialize API
    pro = init_tushare_api()
    
    # Generate year ranges from 2000 to 2025
    years = generate_year_ranges(2000, 2025)
    print(f"\nWill download {len(years)} years of data (2000 to 2025)")
    
    all_data = []
    
    for i, year in enumerate(years, 1):
        # Download by announcement date (ann_date)
        start_date = f"{year}0101"
        end_date = f"{year}1231"
        
        try:
            df = pro.dividend(ann_date='', start_date=start_date, end_date=end_date)
            
            if not df.empty:
                all_data.append(df)
                log_progress(i, len(years), f"Downloaded {year}")
            else:
                print(f"No data for {year}")
            
            # Sleep to avoid API rate limits
            time.sleep(0.3)
            
        except Exception as e:
            print(f"Error downloading {year}: {e}")
            time.sleep(1)
            continue
    
    # Concatenate all data
    if all_data:
        print("\nCombining all data...")
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates if any
        final_df = final_df.drop_duplicates(subset=['ts_code', 'end_date', 'ann_date'], keep='last')
        
        # Save to parquet
        save_to_parquet(final_df, 'dividend')
        
        print(f"\n✓ Dividend data download completed!")
        print(f"  Total records: {len(final_df):,}")
    else:
        print("\n✗ No data was downloaded")


if __name__ == "__main__":
    download_dividend()
