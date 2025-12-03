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
    print("\n--- 下载指数数据 ---")
    
    # 1. Index Basic
    print("正在下载指数基础信息...")
    try:
        df_sse = pro.index_basic(market='SSE')
        df_szse = pro.index_basic(market='SZSE')
        df_basic = pd.concat([df_sse, df_szse], ignore_index=True)
        save_to_parquet(df_basic, 'index_basic')
    except Exception as e:
        print(f"获取 index_basic 失败: {e}")
    
    # 2. Index Weights (Constraint: ONLY 000300.SH and 000905.SH)
    print(f"正在下载指数权重 {KEY_INDICES}...")
    all_weights = []
    for code in KEY_INDICES:
        print(f"  正在获取 {code} 的权重...")
        try:
            df_w = pro.index_weight(index_code=code, start_date=START_DATE, end_date=END_DATE)
            if not df_w.empty:
                all_weights.append(df_w)
        except Exception as e:
            print(f"  获取 {code} 权重失败: {e}")
        time.sleep(0.5) # Rate limit
        
    if all_weights:
        df_weight = pd.concat(all_weights, ignore_index=True)
        save_to_parquet(df_weight, 'index_weight')
        
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
        
    # 4. Industry Class (Shenwan)
    print("正在下载申万行业分类...")
    try:
        df_class = pro.index_classify(level='L1', src='SW2021')
        save_to_parquet(df_class, 'index_classify')
    except Exception as e:
        print(f"获取 index_classify 失败: {e}")

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

    # 1. Shibor Quote
    print("正在下载 Shibor 报价...")
    try:
        df = pro.shibor(start_date=START_DATE, end_date=END_DATE)
        df = standardize_date(df, 'date')
        save_to_parquet(df, 'shibor_quote')
    except Exception as e:
        print(f"Error: {e}")

    # 2. Shibor LPR
    print("正在下载 Shibor LPR...")
    try:
        df = pro.shibor_lpr(start_date=START_DATE, end_date=END_DATE)
        df = standardize_date(df, 'date')
        save_to_parquet(df, 'shibor_lpr')
    except Exception as e:
        print(f"Error: {e}")
        
    # 3. GDP
    print("正在下载 GDP 数据...")
    try:
        df = pro.cn_gdp(start_q=START_DATE[:6], end_q=END_DATE[:6])
        save_to_parquet(df, 'cn_gdp') 
    except Exception as e:
        print(f"Error: {e}")

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

def download_adj_factor(pro):
    print("\n--- 下载复权因子 ---")
    try:
        # Download adj_factor for all stocks
        # Note: adj_factor table is large, but we need it for all stocks.
        # We can download by date range or just all. Tushare pro.adj_factor supports ts_code or trade_date.
        # To get full history for all stocks efficiently, we might need to loop by date or stock.
        # However, pro.adj_factor without params might be limited.
        # Let's try downloading by date chunks if needed, but for now let's try getting all for valid stocks if possible.
        # Actually, best practice for Tushare adj_factor is usually by ts_code or date.
        # Since we have a whitelist, we could loop by ts_code, but that's 5000+ requests.
        # Better: Loop by trade_date is too much (20 years).
        # Tushare 'adj_factor' returns all history if ts_code is provided.
        # Let's try downloading for all stocks in our universe (whitelist).
        
        # Load whitelist or stock_basic
        from data_loader import WHITELIST_PATH, RAW_DATA_DIR
        
        codes = []
        if Path(WHITELIST_PATH).exists():
             print(f"正在从 {WHITELIST_PATH} 加载白名单...")
             whitelist = pd.read_parquet(WHITELIST_PATH)
             codes = whitelist['ts_code'].unique().tolist()
        else:
             print("未找到白名单。正在回退到 stock_basic...")
             stock_basic_path = Path(RAW_DATA_DIR) / 'stock_basic.parquet'
             if stock_basic_path.exists():
                 stock_basic = pd.read_parquet(stock_basic_path)
                 codes = stock_basic['ts_code'].unique().tolist()
             else:
                 print("未找到 stock_basic.parquet。无法下载复权因子。")
                 return

        if codes:
             print(f"正在下载 {len(codes)} 只股票的复权因子...")
             
             all_adj = []
             # Batch process to avoid timeouts/limits
             batch_size = 100
             for i in range(0, len(codes), batch_size):
                 batch = codes[i:i+batch_size]
                 print(f"  正在处理批次 {i//batch_size + 1}/{len(codes)//batch_size + 1}...")
                 try:
                     # pro.adj_factor can take comma separated codes? No, usually single or by date.
                     # Actually, checking Tushare docs: adj_factor(ts_code='...', trade_date='...')
                     # If we pass multiple codes, it might work.
                     # Let's try comma separated.
                     codes_str = ",".join(batch)
                     df = pro.adj_factor(ts_code=codes_str, start_date=START_DATE, end_date=END_DATE)
                     if not df.empty:
                         all_adj.append(df)
                 except Exception as e:
                     print(f"  批次 {i} 错误: {e}")
                 time.sleep(0.3)
                 
             if all_adj:
                 final_df = pd.concat(all_adj, ignore_index=True)
                 save_to_parquet(final_df, 'adj_factor')
        else:
            print("未找到白名单。跳过复权因子下载。")
            
    except Exception as e:
        print(f"下载复权因子失败: {e}")

def main():
    try:
        pro = init_tushare_api()
    except Exception as e:
        print(f"初始化错误: {e}")
        return

    download_index_data(pro)
    download_macro_data(pro)
    download_adj_factor(pro)
    print("\n所有下载已完成。")

if __name__ == "__main__":
    main()
