import pandas as pd
import numpy as np
import os
import sys

import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from factor_library import (
    AverageTrueRange, BollingerBands, Ichimoku, MoneyFlowIndex,
    OnBalanceVolume, PriceVolumeTrend, RelativeVigorIndex, TripleEMA, SineWMA, Momentum,
    # New Factors
    ATRExpansion, PriceBreakout, PricePosition, ATRTrend, VolumeConfirmation,
    BollingerBreakoutUpper, BollingerMiddleSupport, BollingerOversoldBounce, BollingerSqueezeExpansion,
    IchimokuCloudTrend, IchimokuCloudWidthMomentum, IchimokuPricePosition, IchimokuTKCross,
    MFIChangeRate, MFIDivergence,
    OBVBreakthrough, OBVChangeRate, OBVDivergence, OBVRank, OBVSlope,
    PVTDivergence, PVTMADeviation, PVTMomentumReversal,
    RVICrossFactor, RVIDiffFactor, RVIStrengthFactor, RVITrendFactor, RVIValueFactor, RVIVolumeFactor,
    # Renamed/New Technical Factors
    AmihudIlliquidity, CVILLIQ, Coppock, CVaR, DailyTurnoverRate, DownsideRiskBeta,
    AdjustedBetaFP, HistoricalVolatility, HighPrice52Week, Momentum12M,
    MonthlyExcessReturnSeasonalReversal, MonthlyTurnover, RankMomentum, RSI,
    ShortTermReversal, LiquidityMktCapNeutralTurnover, TurnoverVolatilityCoefficient,
    VolumeVWAPDivergence,
    # Market Factors
    FFMC, DividendYield
)

from data.data_loader import load_data

def load_daily_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    daily_adj_path = os.path.join(base_dir, 'data', 'data_cleaner', 'daily_adj.parquet')
    
    print(f"Loading adjusted daily data from {daily_adj_path}...")
    try:
        df = pd.read_parquet(daily_adj_path, engine='fastparquet')
    except Exception:
        df = pd.read_parquet(daily_adj_path)
        
    # Select only adjusted columns and keys
    cols_to_use = ['ts_code', 'trade_date', 'hfq_open', 'hfq_high', 'hfq_low', 'hfq_close', 'hfq_vol', 'amount', 'pct_chg', 'pre_close']
    # Check if hfq_vol exists (it should now)
    if 'hfq_vol' not in df.columns:
        # Fallback if hfq_vol missing (shouldn't happen with new generate_adj_prices)
        if 'vol' in df.columns:
             df['hfq_vol'] = df['vol']
        else:
             raise ValueError("Missing volume data")
             
    df = df[cols_to_use].copy()

    # Rename hfq_ columns to standard names for technical analysis
    rename_dict = {
        'hfq_close': 'close',
        'hfq_open': 'open',
        'hfq_high': 'high',
        'hfq_low': 'low',
        'hfq_vol': 'vol'
    }
    df = df.rename(columns=rename_dict)
    df['ret'] = df['pct_chg']
    
    # Load daily_basic for extra fields (free_share, total_share, dv_ttm)
    print("Loading daily_basic for extra fields...")
    daily_basic = load_data('daily_basic', columns=['ts_code', 'trade_date', 'free_share', 'total_share', 'total_mv', 'dv_ttm', 'turnover_rate', 'circ_mv'])
    
    # Merge
    df = pd.merge(df, daily_basic, on=['ts_code', 'trade_date'], how='left')
    
    # Prepare columns for specific factors
    if 'total_share' in df.columns:
        df['total_shares'] = df['total_share']
        
    if 'dv_ttm' in df.columns and 'total_mv' in df.columns:
        # Construct dividends (Total TTM Dividend Amount)
        # dv_ttm is %, so / 100 * total_mv
        df['dividends'] = (df['dv_ttm'] / 100.0) * (df['close'] * df['total_shares'])
        
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
        SineWMA(),
        Momentum(),
        # New Factors
        ATRExpansion(), PriceBreakout(), PricePosition(), ATRTrend(), VolumeConfirmation(),
        BollingerBreakoutUpper(), BollingerMiddleSupport(), BollingerOversoldBounce(), BollingerSqueezeExpansion(),
        IchimokuCloudTrend(), IchimokuCloudWidthMomentum(), IchimokuPricePosition(), IchimokuTKCross(),
        MFIChangeRate(), MFIDivergence(),
        OBVBreakthrough(), OBVChangeRate(), OBVDivergence(), OBVRank(), OBVSlope(),
        PVTDivergence(), PVTMADeviation(), PVTMomentumReversal(),
        RVICrossFactor(), RVIDiffFactor(), RVIStrengthFactor(), RVITrendFactor(), RVIValueFactor(), RVIVolumeFactor(),
        # Renamed/New Technical Factors
        AmihudIlliquidity(), CVILLIQ(), Coppock(), CVaR(), DailyTurnoverRate(), DownsideRiskBeta(),
        AdjustedBetaFP(), HistoricalVolatility(), HighPrice52Week(), Momentum12M(),
        MonthlyExcessReturnSeasonalReversal(), MonthlyTurnover(), RankMomentum(), RSI(),
        ShortTermReversal(), LiquidityMktCapNeutralTurnover(), TurnoverVolatilityCoefficient(),
        VolumeVWAPDivergence(),
        # Market Factors
        FFMC(), DividendYield()
    ]
    
    # Load market data for AdjustedBetaFP
    print("Constructing synthetic market data (Equal Weighted) from stock data...")
    try:
        # Create synthetic market data from the loaded stock data
        # We use equal-weighted average of pct_chg
        market_df = df.groupby('trade_date')[['pct_chg']].mean().reset_index()
        
        # Create a synthetic close price starting at 1000
        # We need to handle the first value properly
        market_df['close'] = 1000 * (1 + market_df['pct_chg']/100).cumprod()
        
        # Ensure trade_date is datetime (it should be)
        market_df['trade_date'] = pd.to_datetime(market_df['trade_date'])
        market_df['ret'] = market_df['pct_chg']
        
    except Exception as e:
        print(f"Warning: Could not construct market data: {e}. AdjustedBetaFP will fail.")
        market_df = None

    results = []
    
    for factor in factors:
        print(f"Calculating {factor.name}...")
        try:
            if factor.name == 'AdjustedBeta_FP':
                if market_df is not None:
                    res = factor.calculate(df, market_df)
                else:
                    raise ValueError("Market data not available")
            elif factor.name == 'DownsideRiskBeta':
                if market_df is not None:
                    res = factor.calculate(df, market_df)
                else:
                    raise ValueError("Market data not available")
            else:
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
    
    # 4. Resample to Weekly (Friday)
    print("Resampling to weekly (Friday)...")
    tech_df = tech_df.reset_index()
    tech_df['week'] = tech_df['trade_date'].dt.to_period('W-FRI')
    
    # We take the last value of the week for each stock
    weekly_tech = tech_df.groupby(['ts_code', 'week']).last().reset_index()
    
    # Restore trade_date to the end of the week (Friday)
    # Note: .last() on groupby keeps the original columns if they were not keys, 
    # and since we grouped by [ts_code, week], trade_date (the column) will be the last date in that group.
    
    # Drop week column
    weekly_tech = weekly_tech.drop(columns=['week'])
    
    # Set index
    weekly_tech = weekly_tech.set_index(['trade_date', 'ts_code']).sort_index()
    
    # 5. Save
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    output_path = os.path.join(base_dir, 'data', 'factors', 'technical_factors.parquet')
    
    print(f"Saving to {output_path}...")
    weekly_tech.to_parquet(output_path)
    
    print("Done.")
    print(weekly_tech.head())

if __name__ == "__main__":
    construct_technical_factors()
