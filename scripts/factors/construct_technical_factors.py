
import pandas as pd
import numpy as np
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from factor_library import (
    AverageTrueRange, BollingerBands, Ichimoku, MoneyFlowIndex,
    OnBalanceVolume, PriceVolumeTrend, RelativeVigorIndex, TripleEMA, SineWMA
)

def load_daily_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    daily_path = os.path.join(base_dir, 'data', 'raw_data', 'daily.parquet')
    # We might need daily_basic for some things? No, technicals usually use OHLCV.
    # daily.parquet should have open, high, low, close, vol.
    
    print(f"Loading daily data from {daily_path}...")
    df = pd.read_parquet(daily_path)
    return df

def construct_technical_factors():
    print("Constructing technical factors...")
    
    # 1. Load Data
    df = load_daily_data()
    
    # Ensure columns are lower case
    df.columns = [c.lower() for c in df.columns]
    
    # Check required columns
    required = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in daily data: {missing}")
        
    # Ensure types
    df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str))
    df = df.sort_values(['ts_code', 'trade_date'])
    
    # 2. Calculate Factors
    factors = [
        AverageTrueRange(),
        BollingerBands(),
        Ichimoku(),
        MoneyFlowIndex(),
        OnBalanceVolume(),
        PriceVolumeTrend(),
        RelativeVigorIndex(),
        TripleEMA(),
        SineWMA()
    ]
    
    results = []
    
    for factor in factors:
        print(f"Calculating {factor.name}...")
        try:
            res = factor.calculate(df)
            # res is indexed by [trade_date, ts_code]
            results.append(res)
        except Exception as e:
            print(f"Error calculating {factor.name}: {e}")
            
    # 3. Merge all technical factors
    print("Merging technical factors...")
    if not results:
        print("No factors calculated.")
        return
        
    # Merge on index
    tech_df = pd.concat(results, axis=1)
    
    # 4. Resample to Monthly (End of Month)
    print("Resampling to monthly...")
    tech_df = tech_df.reset_index()
    tech_df['month'] = tech_df['trade_date'].dt.to_period('M')
    
    # We take the last value of the month for each stock
    monthly_tech = tech_df.groupby(['ts_code', 'month']).last().reset_index()
    
    # Restore trade_date (which is the last date of the month in the data)
    # Actually groupby last() keeps the columns. trade_date will be the date of the last record.
    
    # Drop month column
    monthly_tech = monthly_tech.drop(columns=['month'])
    
    # Set index
    monthly_tech = monthly_tech.set_index(['trade_date', 'ts_code']).sort_index()
    
    # 5. Save
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    output_path = os.path.join(base_dir, 'data', 'factors', 'technical_factors.parquet')
    
    print(f"Saving to {output_path}...")
    monthly_tech.to_parquet(output_path)
    
    print("Done.")
    print(monthly_tech.head())

if __name__ == "__main__":
    construct_technical_factors()
