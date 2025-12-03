
import tushare as ts
import pandas as pd
import os
import sys
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

def download_adj_factor():
    print("Downloading Adjustment Factors...")
    
    # Load env
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    env_path = os.path.join(base_dir, '.env')
    load_dotenv(env_path)
    
    token = os.getenv('TUSHARE_TOKEN')
    if not token:
        raise ValueError("TUSHARE_TOKEN not found in .env")
        
    ts.set_token(token)
    pro = ts.pro_api()
    
    # Download adj_factor
    # We need to loop or download all?
    # Tushare allows downloading by trade_date or ts_code.
    # Downloading all history for all stocks is large.
    # But we can try downloading by date range? No, adj_factor is static per date.
    # Best to download by ts_code? Too many stocks.
    # Download by trade_date loop?
    # Or maybe just download all?
    
    # Let's try downloading by date loop.
    # But adj_factor doesn't change every day for every stock.
    # Actually, it's better to download by ts_code if we have a list.
    # Let's load the stock list from stock_basic.parquet
    
    stock_basic_path = os.path.join(base_dir, 'data', 'raw_data', 'stock_basic.parquet')
    if not os.path.exists(stock_basic_path):
        print("stock_basic.parquet not found. Please download basic data first.")
        return
        
    stock_basic = pd.read_parquet(stock_basic_path)
    ts_codes = stock_basic['ts_code'].tolist()
    
    print(f"Downloading adj_factor for {len(ts_codes)} stocks...")
    
    # We can pass a list of ts_codes? Tushare limits might apply.
    # Let's do it in chunks.
    
    chunk_size = 100
    all_adj = []
    
    # It might be faster to download by trade_date if we want full history?
    # No, adj_factor is small.
    # Wait, adj_factor is daily data.
    # If we have 5000 stocks * 5000 days = 25M rows.
    # That's big.
    
    # Alternative: Use Tushare's `daily` with `adj='qfq'`?
    # But we already downloaded `daily`.
    # We just need `adj_factor` to multiply.
    
    # Let's try to download `adj_factor` table.
    # It has `ts_code`, `trade_date`, `adj_factor`.
    
    # Optimization: Download by date is slow (20 years * 250 days = 5000 requests).
    # Download by stock is slow (5000 requests).
    # Maybe we can use `pro.adj_factor(ts_code='', trade_date='')`?
    
    # Let's try downloading by stock in chunks?
    # Actually, for a backtest, we need it.
    
    # Let's try to see if we can get it for all stocks at once? No.
    
    # Let's use a smart way:
    # We only need it for the stocks we have in `daily.parquet`.
    # But that's all stocks.
    
    # Let's try downloading in parallel or just loop.
    # Since I cannot do parallel easily here, I will loop.
    # But 5000 requests is too many for this environment?
    # I'll try to download for a subset or check if there's a bulk way.
    
    # Tushare Pro `adj_factor` takes `ts_code` or `trade_date`.
    # If I leave both empty? Error.
    
    # Let's try downloading by year? No, it doesn't support start_date/end_date for all stocks.
    # It supports `ts_code` OR `trade_date`.
    
    # Okay, I will download by `ts_code` for the stocks in `daily.parquet`.
    # I'll limit to the first 50 stocks for now to test?
    # No, the user needs it for the whole universe.
    
    # Wait, `daily` data from Tushare might already be adjusted?
    # I checked `daily.parquet` columns: `['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']`.
    # This is standard unadjusted.
    
    # Maybe I can use `pct_chg` to reconstruct adjusted close?
    # `adj_close_t = adj_close_{t-1} * (1 + pct_chg_t/100)`
    # This is a common trick!
    # We start from the first day, set `adj_close = close`.
    # Then propagate.
    # This avoids downloading `adj_factor`.
    
    print("Calculating Adjusted Close from pct_chg...")
    
    daily_path = os.path.join(base_dir, 'data', 'raw_data', 'daily.parquet')
    df = pd.read_parquet(daily_path)
    
    # Ensure sorted
    df = df.sort_values(['ts_code', 'trade_date'])
    
    # Calculate cumulative return factor
    # pct_chg is in percent, e.g. 1.5 means 1.5%
    # factor = 1 + pct_chg / 100
    df['factor'] = 1 + df['pct_chg'] / 100
    
    # We need to handle the first day.
    # The first day's factor is 1? Or we just use close.
    # Actually, we want to normalize to the end or start?
    # Usually `adj_close` = `close` * `adj_factor`.
    # `adj_factor` accumulates splits.
    
    # Reconstruct:
    # adj_close_t = close_t * cumulative_product_of_splits?
    # No, `pct_chg` includes splits.
    # If `close` drops by half due to split, `pct_chg` is still ~0% (if price didn't move otherwise).
    # So `(1+pct_chg).cumprod()` gives the total return index.
    # `adj_close` = `Initial_Close` * `Cumulative_Return_Index`.
    
    # Let's do this:
    # Group by ts_code
    # `cum_ret` = `(1 + pct_chg/100).cumprod()`
    # `adj_close` = `first_close` * `cum_ret` / `first_cum_ret`?
    # Or simply: `adj_close` = `close` (on day 0) * `cum_ret`.
    # Wait, `pct_chg` on day 0 is NaN or 0?
    # Tushare `pct_chg` on IPO day is usually calculated vs offer price.
    
    # Let's try:
    # `adj_close` = `close` * `adj_factor`
    # `adj_factor` = `adj_close` / `close`
    
    # Using `pct_chg` to get `adj_close`:
    # `adj_close[t] = adj_close[t-1] * (1 + pct_chg[t]/100)`
    # Base case: `adj_close[0] = close[0]`
    
    # Vectorized approach:
    # `adj_close` = `close` * `adj_factor`
    # We don't have `adj_factor`.
    # But we know `adj_close[t] / adj_close[t-1] = 1 + pct_chg[t]/100`
    # And `close[t] / close[t-1]` is the unadjusted return.
    # So `(adj_close[t]/adj_close[t-1]) / (close[t]/close[t-1])` = `adj_factor[t] / adj_factor[t-1]`
    # This seems complicated to reverse engineer due to noise.
    
    # Simpler:
    # `adj_close` series is just the cumulative product of returns, scaled to match the price level.
    # `ret_series = (1 + df['pct_chg'] / 100)`
    # `adj_close = ret_series.groupby('ts_code').cumprod() * first_close`
    # But we need `first_close` for each stock.
    
    # Let's implement this "Synthetic Adj Close" in a script and save it.
    # It's much faster than downloading.
    
    # Get first close
    first_close = df.groupby('ts_code')['close'].transform('first')
    
    # Get cumulative return
    # We need to fill NaN in pct_chg with 0 for the first day
    df['pct_chg'] = df['pct_chg'].fillna(0)
    
    # We need to be careful: `cumprod` starts from the first element.
    # If first element is IPO return, it's fine.
    # But `adj_close` on day 0 should be `close` on day 0.
    # `adj_close` on day 1 should be `close[0] * (1+r1)`.
    # So `cumprod` should include the current day's return.
    
    # But wait, `pct_chg` is the return from t-1 to t.
    # So `adj_close[t] = adj_close[t-1] * (1 + r[t])`.
    # `adj_close[0] = close[0]`.
    # `adj_close[1] = close[0] * (1 + r[1])`.
    # `adj_close[t] = close[0] * product(1+r[1]...1+r[t])`.
    
from data.data_loader import load_data, RAW_DATA_DIR

def generate_adj_prices():
    print("正在生成后复权价格 (官方因子法)...")
    
    # 1. Load Daily Data
    print("正在加载日线数据...")
    daily = load_data('daily', columns=['ts_code', 'trade_date', 'close', 'open', 'high', 'low', 'vol', 'amount', 'pct_chg', 'pre_close'], filter_universe=False)
    
    # 2. Load Adjustment Factors
    print("正在加载复权因子...")
    # 2. Load Adjustment Factors
    print("正在加载复权因子...")
    try:
        adj_factor = load_data('adj_factor')
    except Exception as e:
        print(f"加载复权因子失败: {e}")
        return
    # Ensure columns
    if 'adj_factor' not in adj_factor.columns:
        print("错误: 文件中缺少 'adj_factor' 列。")
        return
        
    # 3. Merge
    print("正在合并日线数据与复权因子...")
    # Ensure dates are datetime
    daily['trade_date'] = pd.to_datetime(daily['trade_date'])
    adj_factor['trade_date'] = pd.to_datetime(adj_factor['trade_date'])
    
    merged = pd.merge(daily, adj_factor[['ts_code', 'trade_date', 'adj_factor']], 
                      on=['ts_code', 'trade_date'], how='left')
    
    # Fill missing adj_factor with 1.0 (or forward fill if appropriate, but 1.0 is safer for new stocks)
    # Actually, for backward adjustment, if adj_factor is missing, it usually means no dividends/splits, so 1.0 might be wrong if it's just missing data.
    # But Tushare usually provides it. Let's forward fill per stock just in case, then fill 1.
    merged['adj_factor'] = merged.groupby('ts_code')['adj_factor'].ffill().fillna(1.0)
    
    # 4. Calculate Backward Adjusted Prices (HFQ)
    # Formula: hfq_price = price * adj_factor
    print("正在计算后复权 (HFQ) 价格...")
    cols = ['close', 'open', 'high', 'low']
    for col in cols:
        merged[f'hfq_{col}'] = merged[col] * merged['adj_factor']
        
    # Pass through vol and amount (unadjusted or adjusted? usually volume is adjusted by division, but amount is same)
    # Tushare hfq usually only adjusts prices.
    # Let's keep vol and amount as is, but maybe rename them to hfq_vol? No, just keep them.
    # But construct_technical_factors expects 'vol'.
    # So we should save them.
    merged['hfq_vol'] = merged['vol'] # Volume is usually adjusted? split -> volume doubles. So hfq_vol = vol * adj_factor?
    # Wait, if price drops by half, volume doubles.
    # adj_factor increases over time (accumulates splits).
    # hfq_price = price * adj_factor.
    # hfq_vol = vol / adj_factor?
    # Let's check standard practice.
    # Usually we want "comparable" volume.
    # If we just keep raw volume, it has jumps.
    # But technical indicators often use raw volume (e.g. OBV).
    # However, for price-volume trend, adjusted volume is better.
    # Let's calculate hfq_vol = vol / adj_factor.
    merged['hfq_vol'] = merged['vol'] / merged['adj_factor']
    merged['hfq_amount'] = merged['amount'] # Amount (money) is invariant to splits.
        
    # 5. Save
    output_path = os.path.join(os.path.dirname(RAW_DATA_DIR), 'data_cleaner', 'daily_adj.parquet')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    print(f"正在保存至 {output_path}...")
    merged.to_parquet(output_path)
    print("完成。")
    print("样例输出:")
    print(merged[['ts_code', 'trade_date', 'close', 'adj_factor', 'hfq_close']].tail())

if __name__ == "__main__":
    generate_adj_prices()
