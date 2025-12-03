
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
    'dividend': 'dividend.parquet',
    'adj_factor': 'adj_factor.parquet',
    # Index Data
    'index_basic': 'index_basic.parquet',
    'index_weight': 'index_weight.parquet',
    'index_dailybasic': 'index_dailybasic.parquet',
    'index_classify': 'index_classify.parquet',
    # Macro Data
    'shibor_quote': 'shibor_quote.parquet',
    'shibor_lpr': 'shibor_lpr.parquet',
    'cn_gdp': 'cn_gdp.parquet',
    'cn_cpi': 'cn_cpi.parquet',
    'cn_pmi': 'cn_pmi.parquet'
}

# Datasets that should NOT be filtered by stock whitelist
MACRO_DATASETS = {
    'index_basic', 'index_weight', 'index_dailybasic', 'index_classify',
    'shibor_quote', 'shibor_lpr', 'cn_gdp', 'cn_cpi', 'cn_pmi'
}

def load_data(
    dataset_name: str,
    columns: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    filter_universe: bool = True
) -> pd.DataFrame:
    """
    Load financial data with optional filtering based on the whitelist.
    
    Args:
        dataset_name (str): Name of the dataset to load.
        columns (list, optional): List of columns to load.
        start_date (str, optional): Start date (YYYY-MM-DD).
        end_date (str, optional): End date (YYYY-MM-DD).
        filter_universe (bool): If True, inner join with whitelist. 
                                If False, load raw data (but still respects date range if possible).
        
    Returns:
        pd.DataFrame: Loaded dataframe sorted by [trade_date, ts_code].
    """
    
    if dataset_name not in DATASET_MAPPING:
        raise ValueError(f"Unknown dataset: {dataset_name}. Available: {list(DATASET_MAPPING.keys())}")
        
    file_path = os.path.join(RAW_DATA_DIR, DATASET_MAPPING[dataset_name])
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Determine columns to load
    load_columns = None
    if columns:
        # Always include keys
        keys_to_add = {'ts_code'}
        if dataset_name != 'stock_basic':
             keys_to_add.add('trade_date')
        load_columns = list(set(columns) | keys_to_add)

    # 1. Load Raw Data (Optimized)
    print(f"正在从 {file_path} 加载原始数据...")
    try:
        raw_data = pd.read_parquet(file_path, columns=load_columns)
    except Exception as e:
        print(f"使用 pyarrow 加载列失败: {e}。正在尝试加载所有列...")
        try:
            raw_data = pd.read_parquet(file_path)
            if columns:
                 raw_data = raw_data[list(set(columns) | {'ts_code', 'trade_date'} & set(raw_data.columns))]
        except Exception as e2:
            print(f"使用 pyarrow 加载失败: {e2}。尝试使用 fastparquet...")
            try:
                raw_data = pd.read_parquet(file_path, engine='fastparquet', columns=load_columns)
            except Exception as e3:
                print(f"使用 fastparquet 加载列失败: {e3}。尝试使用 fastparquet 加载所有列...")
                raw_data = pd.read_parquet(file_path, engine='fastparquet')
                if columns:
                     raw_data = raw_data[list(set(columns) | {'ts_code', 'trade_date'} & set(raw_data.columns))]

    # Ensure date column type match
    if 'trade_date' in raw_data.columns:
        raw_data['trade_date'] = pd.to_datetime(raw_data['trade_date'].astype(str))
        
        # Apply date filtering early if not filtering by universe (which handles it)
        if not filter_universe:
            if start_date:
                raw_data = raw_data[raw_data['trade_date'] >= pd.to_datetime(start_date)]
            if end_date:
                raw_data = raw_data[raw_data['trade_date'] <= pd.to_datetime(end_date)]

    # 2. Filter Universe (Optional)
    # Skip filtering for Macro/Index datasets
    if filter_universe and dataset_name not in MACRO_DATASETS:
        print(f"正在从 {WHITELIST_PATH} 加载白名单...")
        whitelist = pd.read_parquet(WHITELIST_PATH, columns=['ts_code', 'trade_date'])
        whitelist['trade_date'] = pd.to_datetime(whitelist['trade_date'].astype(str))
        
        # Apply Date Filtering to Whitelist
        if start_date:
            whitelist = whitelist[whitelist['trade_date'] >= pd.to_datetime(start_date)]
        if end_date:
            whitelist = whitelist[whitelist['trade_date'] <= pd.to_datetime(end_date)]
            
        print("正在与白名单合并...")
        if 'trade_date' in raw_data.columns:
            merged_data = pd.merge(whitelist, raw_data, on=['ts_code', 'trade_date'], how='inner')
        else:
            # Static data expansion
            merged_data = pd.merge(whitelist, raw_data, on='ts_code', how='left')
    else:
        merged_data = raw_data

    # 3. Final Column Selection
    if columns:
        final_cols = list(set(columns) | {'ts_code', 'trade_date'})
        final_cols = [c for c in final_cols if c in merged_data.columns]
        merged_data = merged_data[final_cols]

    # 4. Sort
    if 'trade_date' in merged_data.columns:
        merged_data = merged_data.sort_values(['trade_date', 'ts_code'])
    else:
        merged_data = merged_data.sort_values(['ts_code'])

    print(f"最终数据形状: {merged_data.shape}")
    return merged_data

if __name__ == "__main__":
    # Usage Example
    print("正在运行使用示例...")
    df = load_data(
        dataset_name='daily',
        columns=['close', 'vol'],
        start_date='2005-01-01',
        end_date='2005-02-01',
        filter_universe=True
    )
    print("\n样例数据:")
    print(df.head())
