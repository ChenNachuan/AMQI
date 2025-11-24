"""
Download stock basic information from Tushare.
股票列表 - Stock Basic
"""
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from data.data_loader.utils import init_tushare_api, save_to_parquet


def download_stock_basic():
    """Download stock basic information."""
    print("="*60)
    print("Downloading Stock Basic Information (股票列表)")
    print("="*60)
    
    # Initialize API
    pro = init_tushare_api()
    
    # Download stock basic data
    # Get all stocks (L: listed, D: delisted, P: paused)
    print("\nFetching stock list...")
    df = pro.stock_basic(exchange='', list_status='', 
                         fields='ts_code,symbol,name,area,industry,market,list_date,delist_date')
    
    print(f"Retrieved {len(df):,} stocks")
    
    # Save to parquet
    save_to_parquet(df, 'stock_basic')
    
    print("\n✓ Stock basic data download completed!")


if __name__ == "__main__":
    download_stock_basic()
