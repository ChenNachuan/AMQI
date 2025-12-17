"""
Download Market Indices and Macroeconomic Data.
"""
import sys
from pathlib import Path
import pandas as pd
import time

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from data.data_loader.utils import init_tushare_api, save_to_parquet, _get_raw_data_dir

# Configuration
START_DATE = '20230101'
END_DATE = pd.Timestamp.now().strftime('%Y%m%d')

# Key Indices
KEY_INDICES = ['000300.SH', '000905.SH','000001.SH']

def download_index_data(pro):
    print("\n--- 下载指数数据 ---")
    # 3. Index Daily (PE/PB/Turnover for key indices)
    print(f"正在下载指数日线基本数据 {KEY_INDICES}...")
    all_daily = []
    for code in KEY_INDICES:
        print(f"  正在获取 {code} 的日线基本数据...")
        try:
            df_d = pro.index_dailybasic(ts_code=code, start_date=START_DATE, end_date=END_DATE)
            if not df_d.empty:
                all_daily.append(df_d)
        except Exception as e:
            print(f"  获取 {code} 日线基本数据失败: {e}")
        time.sleep(0.5)
        
    if all_daily:
        df_daily = pd.concat(all_daily, ignore_index=True)
        save_to_parquet(df_daily, 'index_dailybasic')

def download_macro_data(pro):
    print("\n--- 下载宏观数据 ---")
    
    # Helper to standardize date
    def standardize_date(df, date_col='date'):
        if df is None or df.empty:
            return df
        if date_col in df.columns:
            df = df.rename(columns={date_col: 'trade_date'})
        elif 'end_date' in df.columns:
             df = df.rename(columns={'end_date': 'trade_date'})
        elif 'month' in df.columns:
             df = df.rename(columns={'month': 'trade_date'})
        return df

    # 4. CPI
    print("正在下载 CPI 数据...")
    try:
        df = pro.cn_cpi(start_m=START_DATE[:6], end_m=END_DATE[:6])
        df = standardize_date(df, 'month')
        save_to_parquet(df, 'cn_cpi')
    except Exception as e:
        print(f"Error: {e}")

    # 5. PMI
    print("正在下载 PMI 数据...")
    try:
        df = pro.cn_pmi(start_m=START_DATE[:6], end_m=END_DATE[:6])
        df = standardize_date(df, 'month')
        save_to_parquet(df, 'cn_pmi')
    except Exception as e:
        print(f"Error: {e}")

def main():
    try:
        pro = init_tushare_api()
    except Exception as e:
        print(f"初始化错误: {e}")
        return

    download_index_data(pro)
    download_macro_data(pro)
    print("\n所有下载已完成。")

if __name__ == "__main__":
    main()
