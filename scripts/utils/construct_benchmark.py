
import tushare as ts
import pandas as pd
import os
import sys
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

def construct_benchmark():
    print("Constructing Benchmark (CSI 300)...")
    
    # Load env
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    env_path = os.path.join(base_dir, '.env')
    load_dotenv(env_path)
    
    token = os.getenv('TUSHARE_TOKEN')
    if not token:
        raise ValueError("TUSHARE_TOKEN not found in .env")
        
    ts.set_token(token)
    pro = ts.pro_api()
    
    # Download CSI 300 Daily
    # 000300.SH
    print("Downloading CSI 300 daily data...")
    try:
        df = pro.index_daily(ts_code='000300.SH', start_date='20050101', end_date='20251231')
    except Exception as e:
        print(f"Error downloading data: {e}")
        return
        
    if df.empty:
        print("No data downloaded.")
        return
        
    # Process
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date')
    
    # Calculate Monthly Return
    df['month'] = df['trade_date'].dt.to_period('M')
    
    # Resample to monthly (last trading day)
    # We want the last record of each month
    monthly = df.groupby('month').apply(lambda x: x.iloc[-1]).reset_index(drop=True)
    
    # Calculate Return
    monthly['ret'] = monthly['close'].pct_change()
    
    # Format
    output_df = monthly[['trade_date', 'ret']].copy()
    output_df = output_df.dropna()
    
    # Save
    output_path = os.path.join(base_dir, 'data', 'benchmark_csi300_monthly.parquet')
    print(f"Saving to {output_path}...")
    output_df.to_parquet(output_path)
    
    print("Done.")
    print(output_df.head())

if __name__ == "__main__":
    construct_benchmark()
