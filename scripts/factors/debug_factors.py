
import pandas as pd
import numpy as np
import os
import sys

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
    
    # Load daily_basic for extra fields (free_share, total_share, dv_ttm)
    print("Loading daily_basic for extra fields...")
    daily_basic = load_data('daily_basic', columns=['ts_code', 'trade_date', 'free_share', 'total_share', 'total_mv', 'dv_ttm'])
    
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

def debug_technical_factors():
    print("Constructing technical factors...")
    
    # 1. Load Data
    df = load_daily_data()
    
    # Ensure columns are lower case
    df.columns = [c.lower() for c in df.columns]
    
    # Ensure types
    df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str))
    df = df.sort_values(['ts_code', 'trade_date'])
    
    # Take a small subset for debugging
    unique_codes = df['ts_code'].unique()[:5]
    df = df[df['ts_code'].isin(unique_codes)].copy()
    print(f"Debugging with subset of {len(df)} rows and {len(unique_codes)} stocks.")
    
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
    
    results = []
    
    for factor in factors:
        print(f"Checking {factor.name}...")
        try:
            res = factor.calculate(df)
            print(f"  -> Index Type: {type(res.index)}")
            if isinstance(res.index, pd.MultiIndex):
                print(f"  -> Index Levels: {res.index.names}")
            else:
                print(f"  -> Index Name: {res.index.name}")
            print(f"  -> Shape: {res.shape}")
            results.append(res)
        except Exception as e:
            print(f"  -> Error calculating {factor.name}: {e}")

if __name__ == "__main__":
    debug_technical_factors()
