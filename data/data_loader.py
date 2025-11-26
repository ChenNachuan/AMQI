
import pandas as pd
import os
from typing import List, Optional, Union

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw_data')
CLEAN_DATA_DIR = os.path.join(BASE_DIR, 'data', 'data_cleaner')
WHITELIST_PATH = os.path.join(CLEAN_DATA_DIR, 'daily_basic_cleaned.parquet')

# File mapping
DATASET_MAPPING = {
    'daily': 'daily.parquet',
    'daily_basic': 'daily_basic.parquet',
    'stock_basic': 'stock_basic.parquet',
    'income': 'income.parquet',
    'balancesheet': 'balancesheet.parquet',
    'cashflow': 'cashflow.parquet',
    'fina_indicator': 'fina_indicator.parquet',
    'dividend': 'dividend.parquet'
}

def load_data(
    dataset_name: str,
    columns: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Load financial data with automatic filtering based on the whitelist.
    
    Args:
        dataset_name (str): Name of the dataset to load (e.g., 'daily', 'stock_basic').
        columns (list, optional): List of columns to load. If None, load all.
                                  'ts_code' and 'trade_date' are always included if available.
        start_date (str, optional): Start date (YYYY-MM-DD).
        end_date (str, optional): End date (YYYY-MM-DD).
        
    Returns:
        pd.DataFrame: Filtered and cleaned dataframe.
    """
    
    if dataset_name not in DATASET_MAPPING:
        raise ValueError(f"Unknown dataset: {dataset_name}. Available: {list(DATASET_MAPPING.keys())}")
        
    file_path = os.path.join(RAW_DATA_DIR, DATASET_MAPPING[dataset_name])
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
        
    # 1. Load Whitelist
    # We only need ts_code and trade_date from the whitelist
    print(f"Loading whitelist from {WHITELIST_PATH}...")
    whitelist = pd.read_parquet(WHITELIST_PATH, columns=['ts_code', 'trade_date'])
    whitelist['trade_date'] = pd.to_datetime(whitelist['trade_date'].astype(str))
    
    # 2. Apply Date Filtering to Whitelist
    if start_date:
        whitelist = whitelist[whitelist['trade_date'] >= pd.to_datetime(start_date)]
    if end_date:
        whitelist = whitelist[whitelist['trade_date'] <= pd.to_datetime(end_date)]
        
    if whitelist.empty:
        print("Warning: Whitelist is empty after date filtering.")
        return pd.DataFrame()

    print(f"Whitelist size after date filtering: {len(whitelist)}")

    # 3. Load Raw Data
    # Determine columns to load
    load_columns = None
    if columns:
        # Ensure join keys are present
        required_keys = ['ts_code']
        # Most files have trade_date, but stock_basic doesn't (it has list_date etc)
        # We need to check if the file has trade_date.
        # For efficiency, let's just load the requested columns + ts_code + trade_date (if exists)
        # We can't easily know if trade_date exists without peeking, but for our specific datasets:
        # stock_basic: ts_code, ... (no trade_date)
        # others: ts_code, trade_date, ...
        
        # Simple heuristic: always try to add 'trade_date' to load_columns if it's not stock_basic
        # Or better, just let pyarrow handle it? No, pyarrow will error if column missing.
        
        # Let's peek at the schema if needed, or hardcode knowledge.
        # Given the requirements, let's assume standard structure.
        
        keys_to_add = {'ts_code'}
        if dataset_name != 'stock_basic':
             keys_to_add.add('trade_date')
             
        load_columns = list(set(columns) | keys_to_add)
    
    print(f"Loading raw data from {file_path}...")
    try:
        raw_data = pd.read_parquet(file_path, columns=load_columns)
    except Exception as e:
        # Fallback if column doesn't exist (e.g. user asked for trade_date in stock_basic)
        print(f"Error loading columns: {e}. Loading all columns and filtering later.")
        raw_data = pd.read_parquet(file_path)
        if columns:
             raw_data = raw_data[list(set(columns) | {'ts_code', 'trade_date'} & set(raw_data.columns))]

    # Ensure date column type match
    if 'trade_date' in raw_data.columns:
        raw_data['trade_date'] = pd.to_datetime(raw_data['trade_date'].astype(str))
    
    # 4. Merge with Whitelist
    print("Merging with whitelist...")
    if 'trade_date' in raw_data.columns:
        # Standard case: join on ts_code and trade_date
        merged_data = pd.merge(whitelist, raw_data, on=['ts_code', 'trade_date'], how='inner')
    else:
        # Static data case (e.g. stock_basic): join on ts_code only
        # This expands static data to the panel defined by the whitelist
        merged_data = pd.merge(whitelist, raw_data, on='ts_code', how='left')
        
    # 5. Final Column Selection (if requested)
    if columns:
        # We might have added keys to load_columns, so filter back to what user asked + keys
        # The user likely wants the keys too, or at least the dataframe should have them to be useful.
        # Requirement says: "only load those specific columns... (plus the index columns ts_code and trade_date)"
        final_cols = list(set(columns) | {'ts_code', 'trade_date'})
        # Only keep columns that actually exist in the result
        final_cols = [c for c in final_cols if c in merged_data.columns]
        merged_data = merged_data[final_cols]
        
    print(f"Final data shape: {merged_data.shape}")
    return merged_data

if __name__ == "__main__":
    # Usage Example
    print("Running usage example...")
    df = load_data(
        dataset_name='daily',
        columns=['close', 'vol'],
        start_date='2005-01-01',
        end_date='2006-01-01'
    )
    print("\nSample Data:")
    print(df.head())
    print("\nData Info:")
    print(df.info())
