"""
Download Market Indices and Macroeconomic Data.
"""
import sys
from pathlib import Path
import pandas as pd
import time

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from data.data_loader.utils import init_tushare_api, save_to_parquet

# Configuration
START_DATE = '20050101'
END_DATE = pd.Timestamp.now().strftime('%Y%m%d')

# Key Indices
KEY_INDICES = ['000300.SH', '000905.SH']

def download_index_data(pro):
    print("\n--- Downloading Index Data ---")
    
    # 1. Index Basic
    print("Downloading Index Basic...")
    try:
        df_sse = pro.index_basic(market='SSE')
        df_szse = pro.index_basic(market='SZSE')
        df_basic = pd.concat([df_sse, df_szse], ignore_index=True)
        save_to_parquet(df_basic, 'index_basic')
    except Exception as e:
        print(f"Error fetching index_basic: {e}")
    
    # 2. Index Weights (Constraint: ONLY 000300.SH and 000905.SH)
    print(f"Downloading Index Weights for {KEY_INDICES}...")
    all_weights = []
    for code in KEY_INDICES:
        print(f"  Fetching weights for {code}...")
        try:
            df_w = pro.index_weight(index_code=code, start_date=START_DATE, end_date=END_DATE)
            if not df_w.empty:
                all_weights.append(df_w)
        except Exception as e:
            print(f"  Error fetching weights for {code}: {e}")
        time.sleep(0.5) # Rate limit
        
    if all_weights:
        df_weight = pd.concat(all_weights, ignore_index=True)
        save_to_parquet(df_weight, 'index_weight')
        
    # 3. Index Daily (PE/PB/Turnover for key indices)
    print(f"Downloading Index Daily Basic for {KEY_INDICES}...")
    all_daily = []
    for code in KEY_INDICES:
        print(f"  Fetching daily basic for {code}...")
        try:
            df_d = pro.index_dailybasic(ts_code=code, start_date=START_DATE, end_date=END_DATE)
            if not df_d.empty:
                all_daily.append(df_d)
        except Exception as e:
            print(f"  Error fetching daily basic for {code}: {e}")
        time.sleep(0.5)
        
    if all_daily:
        df_daily = pd.concat(all_daily, ignore_index=True)
        save_to_parquet(df_daily, 'index_dailybasic')
        
    # 4. Industry Class (Shenwan)
    print("Downloading Industry Class (Shenwan)...")
    try:
        df_class = pro.index_classify(level='L1', src='SW2021')
        save_to_parquet(df_class, 'index_classify')
    except Exception as e:
        print(f"Error fetching index_classify: {e}")

def download_macro_data(pro):
    print("\n--- Downloading Macro Data ---")
    
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

    # 1. Shibor Quote
    print("Downloading Shibor Quote...")
    try:
        df = pro.shibor(start_date=START_DATE, end_date=END_DATE)
        df = standardize_date(df, 'date')
        save_to_parquet(df, 'shibor_quote')
    except Exception as e:
        print(f"Error: {e}")

    # 2. Shibor LPR
    print("Downloading Shibor LPR...")
    try:
        df = pro.shibor_lpr(start_date=START_DATE, end_date=END_DATE)
        df = standardize_date(df, 'date')
        save_to_parquet(df, 'shibor_lpr')
    except Exception as e:
        print(f"Error: {e}")
        
    # 3. GDP
    print("Downloading GDP...")
    try:
        df = pro.cn_gdp(start_q=START_DATE[:6], end_q=END_DATE[:6])
        save_to_parquet(df, 'cn_gdp') 
    except Exception as e:
        print(f"Error: {e}")

    # 4. CPI
    print("Downloading CPI...")
    try:
        df = pro.cn_cpi(start_m=START_DATE[:6], end_m=END_DATE[:6])
        df = standardize_date(df, 'month')
        save_to_parquet(df, 'cn_cpi')
    except Exception as e:
        print(f"Error: {e}")

    # 5. PMI
    print("Downloading PMI...")
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
        print(f"Initialization Error: {e}")
        return

    download_index_data(pro)
    download_macro_data(pro)
    print("\nAll downloads completed.")

if __name__ == "__main__":
    main()
