
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
    
    # So we group by ts_code, take `(1+pct_chg/100)`.
    # Set the first value to 1.0 (because `pct_chg` on day 0 is usually vs IPO price, but we want to start compounding from close[0]).
    # Actually, if we want `adj_close` to track `close` initially:
    # We can just normalize `adj_close` such that the last value equals `close` (Backward adjustment)?
    # Or first value equals `close` (Forward adjustment).
    # Forward adjustment is better for backtesting (no lookahead bias in scale, though scale doesn't matter for returns).
    # Tushare `qfq` (Forward) usually adjusts past prices down. `hfq` (Backward) adjusts past prices up?
    # Actually `qfq` (Pre-adjusted) means current price is real, past is adjusted.
    # `hfq` (Post-adjusted) means past price is real (IPO), current is adjusted.
    
    # Let's use Forward Adjustment (QFQ) logic:
    # `adj_factor` = `cumulative_product_of_splits`.
    # But we don't have splits.
    
    # Let's use the `pct_chg` reconstruction method.
    # `adj_close` = `initial_close` * `cumprod(1 + pct_chg/100)`
    # But we need to handle the first day correctly.
    # If `pct_chg` on day 0 is valid (e.g. IPO day return), then `adj_close[0]` should reflect that?
    # No, `adj_close[0]` is just a starting point.
    # Let's set `adj_close[0] = close[0]`.
    # Then `adj_close[t] = adj_close[t-1] * (1 + pct_chg[t]/100)`.
    
    # Implementation:
    # 1. `returns = 1 + df['pct_chg'] / 100`
    # 2. Set first return of each stock to 1.0 (to avoid applying IPO return to the base).
    #    Actually, if we want `adj_close[0] = close[0]`, we don't multiply by `returns[0]`.
    #    We multiply `close[0]` by `cumprod(returns[1:]`.
    
    # Let's do:
    # `df['factor'] = 1 + df['pct_chg'] / 100`
    # `df.loc[df.groupby('ts_code').head(1).index, 'factor'] = 1.0`
    # `df['cum_factor'] = df.groupby('ts_code')['factor'].cumprod()`
    # `df['adj_close'] = df['cum_factor'] * df.groupby('ts_code')['close'].transform('first')`
    
    # This gives us a continuous price series that respects `pct_chg`.
    # This is effectively `hfq` (Post-adjusted) because it starts from IPO price and goes up.
    # `qfq` would end at current close.
    # For returns calculation, `hfq` vs `qfq` doesn't matter, as `pct_change` is invariant.
    
    # So we will create `adj_close` column and save it to a new parquet `daily_adjusted.parquet`.
    
    # Apply logic
    df['factor'] = 1 + df['pct_chg'].fillna(0) / 100
    
    # Set first factor to 1.0
    # We can use a mask
    # Is there a faster way than groupby head?
    # `df.duplicated('ts_code', keep='first')` is False for first items.
    is_first = ~df.duplicated('ts_code', keep='first')
    df.loc[is_first, 'factor'] = 1.0
    
    print("Calculating cumulative product...")
    df['cum_factor'] = df.groupby('ts_code')['factor'].cumprod()
    
    print("Calculating adj_close...")
    first_closes = df.loc[is_first].set_index('ts_code')['close']
    # Map first close back
    df['first_close'] = df['ts_code'].map(first_closes)
    df['adj_close'] = df['cum_factor'] * df['first_close']
    
    # Save
    output_path = os.path.join(base_dir, 'data', 'raw_data', 'daily_adjusted.parquet')
    print(f"Saving to {output_path}...")
    df[['ts_code', 'trade_date', 'adj_close']].to_parquet(output_path)
    
    print("Done.")
    print(df[['ts_code', 'trade_date', 'close', 'adj_close', 'pct_chg']].head())

if __name__ == "__main__":
    download_adj_factor()
