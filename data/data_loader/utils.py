"""
Common utilities for Tushare data loaders.
"""
import os
from pathlib import Path
from typing import Generator, List, Optional, Tuple

import pandas as pd
import tushare as ts
from dotenv import load_dotenv


def load_tushare_token() -> str:
    """Load Tushare token from .env file."""
    # Load .env from project root
    project_root = Path(__file__).parent.parent.parent
    env_path = project_root / '.env'
    load_dotenv(env_path)
    
    token = os.getenv('TUSHARE_TOKEN')
    if not token:
        raise ValueError("TUSHARE_TOKEN not found in .env file")
    return token


def init_tushare_api() -> ts.pro_api:
    """Initialize and return Tushare Pro API client."""
    token = load_tushare_token()
    return ts.pro_api(token)


def generate_month_ranges(start_year: int = 2000, end_year: int = 2025, end_month: int = 11) -> List[str]:
    """
    Generate list of month strings in YYYYMM format.
    
    Args:
        start_year: Starting year (inclusive)
        end_year: Ending year (inclusive)
        end_month: Ending month for the last year (inclusive)
    
    Returns:
        List of month strings in YYYYMM format
    """
    months = []
    for year in range(start_year, end_year + 1):
        last_month = end_month if year == end_year else 12
        for month in range(1, last_month + 1):
            months.append(f"{year}{month:02d}")
    return months


def generate_quarter_ranges(start_year: int = 2000, end_year: int = 2025) -> List[str]:
    """
    Generate list of quarter strings in YYYYMMDD format (last day of quarter).
    
    Args:
        start_year: Starting year (inclusive)
        end_year: Ending year (inclusive)
    
    Returns:
        List of quarter end dates in YYYYMMDD format
    """
    quarters = []
    quarter_ends = ['0331', '0630', '0930', '1231']
    for year in range(start_year, end_year + 1):
        for quarter_end in quarter_ends:
            quarters.append(f"{year}{quarter_end}")
    return quarters


def generate_year_ranges(start_year: int = 2000, end_year: int = 2025) -> List[int]:
    """
    Generate list of years.
    
    Args:
        start_year: Starting year (inclusive)
        end_year: Ending year (inclusive)
    
    Returns:
        List of years
    """
    return list(range(start_year, end_year + 1))


def save_to_parquet(df: pd.DataFrame, filename: str, raw_data_dir: str = None) -> None:
    """
    Save DataFrame to parquet format in raw_data directory.
    
    Args:
        df: DataFrame to save
        filename: Name of the parquet file (without extension)
        raw_data_dir: Optional custom raw_data directory path
    """
    if raw_data_dir is None:
        project_root = Path(__file__).parent.parent.parent
        raw_data_dir = project_root / 'data' / 'raw_data'
    else:
        raw_data_dir = Path(raw_data_dir)
    
    # Create directory if it doesn't exist
    raw_data_dir.mkdir(parents=True, exist_ok=True)
    
    # Save to parquet
    output_path = raw_data_dir / f"{filename}.parquet"
    df.to_parquet(output_path, index=False, compression='snappy')
    print(f"✓ Saved {len(df):,} records to {output_path}")


def log_progress(current: int, total: int, prefix: str = "Progress") -> None:
    """
    Print progress information.
    
    Args:
        current: Current iteration number
        total: Total number of iterations
        prefix: Prefix message
    """
    percentage = (current / total) * 100
    print(f"{prefix}: {current}/{total} ({percentage:.1f}%)")


def _get_raw_data_dir() -> Path:
    """Return the default raw data directory path."""
    project_root = Path(__file__).parent.parent.parent
    return project_root / 'data' / 'raw_data'


def load_stock_basic(pro: Optional[ts.pro_api] = None, refresh: bool = False) -> pd.DataFrame:
    """Load the latest stock universe, refreshing from Tushare when requested."""
    cache_path = _get_raw_data_dir() / 'stock_basic.parquet'

    df: Optional[pd.DataFrame] = None

    if not refresh and cache_path.exists():
        try:
            df = pd.read_parquet(cache_path)
        except (OSError, ValueError) as exc:
            print(f"⚠️  Failed to read cached stock_basic ({exc}); refreshing from Tushare...")
            refresh = True

    if refresh or df is None:
        if pro is None:
            pro = init_tushare_api()
        df = pro.stock_basic(
            exchange='',
            list_status='',
            fields='ts_code,symbol,name,area,industry,market,list_date,delist_date',
        )
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_path, index=False, compression='snappy')
        print(f"✓ Refreshed stock_basic cache with {len(df):,} rows at {cache_path}")

    if df is None:
        raise RuntimeError("Failed to load stock_basic data")

    df = df.copy()
    df['list_date'] = pd.to_datetime(df['list_date'], format='%Y%m%d', errors='coerce')
    df['delist_date'] = pd.to_datetime(df['delist_date'], format='%Y%m%d', errors='coerce')
    return df


def iter_ts_code_date_ranges(
    stock_df: pd.DataFrame, start_date: str, end_date: str
) -> Generator[Tuple[str, str, str], None, None]:
    """Yield ts_code with effective start/end dates within the requested window."""
    window_start = pd.to_datetime(start_date, format='%Y%m%d')
    window_end = pd.to_datetime(end_date, format='%Y%m%d')

    for row in stock_df.itertuples():
        list_date = getattr(row, 'list_date', pd.NaT)
        if pd.isna(list_date):
            continue

        list_date = pd.Timestamp(list_date)

        delist_date = getattr(row, 'delist_date', pd.NaT)
        raw_end = pd.Timestamp(delist_date) if not pd.isna(delist_date) else window_end

        effective_start = max(list_date, window_start)
        effective_end = min(raw_end, window_end)

        if effective_start > window_end or effective_end < window_start:
            continue

        ts_code = str(getattr(row, 'ts_code'))
        yield ts_code, effective_start.strftime('%Y%m%d'), effective_end.strftime('%Y%m%d')
