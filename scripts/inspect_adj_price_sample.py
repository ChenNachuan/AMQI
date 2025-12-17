
import pandas as pd
import os

def inspect_adj_prices():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    daily_adj_path = os.path.join(base_dir, 'data', 'data_cleaner', 'daily_adj.parquet')
    
    if not os.path.exists(daily_adj_path):
        print(f"File not found: {daily_adj_path}")
        return
        
    print(f"Loading {daily_adj_path}...")
    df = pd.read_parquet(daily_adj_path)
    
    # Pick a sample stock: 000001.SZ (Ping An Bank) or 600519.SH (Moutai)
    # Ping An has many splits. Moutai has fewer but high price.
    sample_code = '000001.SZ'
    
    print(f"Inspecting {sample_code}...")
    sample = df[df['ts_code'] == sample_code].sort_values('trade_date').tail(10)
    
    if sample.empty:
        print(f"No data for {sample_code}. showing first 5 rows of dataset:")
        print(df.head())
    else:
        print(sample[['ts_code', 'trade_date', 'close', 'adj_factor', 'hfq_close', 'hfq_open']])
        
    # Check if hfq is significantly different from close
    if sample['hfq_close'].mean() > sample['close'].mean() * 1.5:
        print("\nObservation: HFQ Price is significantly HIGHER than Raw Price.")
        print("This confirms we are calculating Post-Adjusted (HFQ) prices.")
    else:
        print("\nObservation: HFQ Price is similar to Raw Price.")

if __name__ == "__main__":
    inspect_adj_prices()
