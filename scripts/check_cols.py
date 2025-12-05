
import pandas as pd
import os

base_dir = '/Users/nachuanchen/Documents/Undergrad_Resources/资产管理与投资策略分析/AMQI'
path = os.path.join(base_dir, 'data', 'raw_data', 'daily_basic.parquet')

try:
    df = pd.read_parquet(path)
    print("Columns:", df.columns.tolist())
except Exception as e:
    print(e)
