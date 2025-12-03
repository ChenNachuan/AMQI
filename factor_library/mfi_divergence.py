import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class MFIDivergence(BaseFactor):
    """
    MFI-Price Divergence Factor.
    Detects divergence between price and money flow, signaling potential trend reversal.
    
    Divergence Types:
    
    1. Bullish Divergence (Buy Signal, factor = +1):
       - Price makes new low (panic sell-off)
       - BUT MFI doesn't make new low (money not fleeing)
       - Interpretation: Downtrend exhaustion, potential bounce
       - More effective when MFI < 30 (oversold zone)
    
    2. Bearish Divergence (Sell Signal, factor = -1):
       - Price makes new high (continuing rally)
       - BUT MFI doesn't make new high (money not chasing)
       - Interpretation: Uptrend exhaustion, potential pullback
       - More effective when MFI > 70 (overbought zone)
    
    3. No Divergence (factor = 0):
       - Price and MFI moving in sync
    
    Selection Strategy:
    - factor = +1: Bullish divergence, buy signal
    - factor = -1: Bearish divergence, sell signal
    - factor = 0: No divergence
    """
    
    def __init__(self, mfi_period: int = 14, lookback: int = 20, 
                 oversold_threshold: float = 30, overbought_threshold: float = 70):
        """
        Initialize MFI Divergence Factor.
        
        Args:
            mfi_period: MFI calculation period (default: 14 days)
            lookback: Lookback period for divergence detection (default: 20 days)
            oversold_threshold: MFI oversold threshold (default: 30)
            overbought_threshold: MFI overbought threshold (default: 70)
        """
        self.mfi_period = mfi_period
        self.lookback = lookback
        self.oversold_threshold = oversold_threshold
        self.overbought_threshold = overbought_threshold
    
    @property
    def name(self) -> str:
        return f"MFI_Divergence_{self.lookback}d"
        
    @property
    def required_fields(self) -> list:
        return ['high', 'low', 'close', 'vol']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate MFI-Price Divergence.
        
        Steps:
        1. Calculate basic MFI
        2. Calculate rolling highest/lowest price
        3. Calculate rolling highest/lowest MFI
        4. Detect bullish divergence:
           - close == N-day lowest price AND
           - MFI > N-day lowest MFI AND
           - MFI < oversold_threshold
        5. Detect bearish divergence:
           - close == N-day highest price AND
           - MFI < N-day highest MFI AND
           - MFI > overbought_threshold
        
        Args:
            df: Daily dataframe with 'high', 'low', 'close', 'vol'.
            
        Returns:
            DataFrame with divergence signals (+1=bullish, -1=bearish, 0=none).
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # Step 1: Calculate basic MFI
        mfi = self._calculate_mfi(df, self.mfi_period)
        close = df['close']
        
        # Step 2: Calculate rolling highest/lowest price
        price_high = close.groupby(df['ts_code']).rolling(self.lookback).max().reset_index(0, drop=True)
        price_low = close.groupby(df['ts_code']).rolling(self.lookback).min().reset_index(0, drop=True)
        
        # Step 3: Calculate rolling highest/lowest MFI
        mfi_high = mfi.groupby(df['ts_code']).rolling(self.lookback).max().reset_index(0, drop=True)
        mfi_low = mfi.groupby(df['ts_code']).rolling(self.lookback).min().reset_index(0, drop=True)
        
        # Step 4: Detect bullish divergence (buy signal)
        # Condition 1: Price touches N-day low
        # Condition 2: MFI above N-day low (money not panicking)
        # Condition 3: MFI in oversold zone (< oversold_threshold)
        bullish_divergence = (
            (close == price_low) &
            (mfi > mfi_low) &
            (mfi < self.oversold_threshold)
        ).astype(float)
        
        # Step 5: Detect bearish divergence (sell signal)
        # Condition 1: Price touches N-day high
        # Condition 2: MFI below N-day high (money not chasing)
        # Condition 3: MFI in overbought zone (> overbought_threshold)
        bearish_divergence = (
            (close == price_high) &
            (mfi < mfi_high) &
            (mfi > self.overbought_threshold)
        ).astype(float)
        
        # Factor assignment: bullish is positive, bearish is negative
        divergence_signal = bullish_divergence - bearish_divergence
        
        # Prepare result
        result = pd.DataFrame({
            self.name: divergence_signal,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
    
    def _calculate_mfi(self, df: pd.DataFrame, period: int) -> pd.Series:
        """
        Calculate basic MFI (Money Flow Index).
        
        Args:
            df: Input dataframe
            period: MFI calculation period
            
        Returns:
            MFI series (0-100 range)
        """
        # Typical Price
        tp = (df['high'] + df['low'] + df['close']) / 3
        
        # Raw Money Flow
        rmf = tp * df['vol']
        
        # Previous Typical Price
        prev_tp = tp.groupby(df['ts_code']).shift(1)
        
        # Positive and Negative Money Flow
        pos_flow = pd.Series(0.0, index=df.index)
        neg_flow = pd.Series(0.0, index=df.index)
        
        pos_mask = tp > prev_tp
        neg_mask = tp < prev_tp
        
        pos_flow[pos_mask] = rmf[pos_mask]
        neg_flow[neg_mask] = rmf[neg_mask]
        
        # Rolling Sums
        rolling_pos = pos_flow.groupby(df['ts_code']).rolling(period).sum().reset_index(0, drop=True)
        rolling_neg = neg_flow.groupby(df['ts_code']).rolling(period).sum().reset_index(0, drop=True)
        
        # Money Flow Ratio
        mfr = rolling_pos / rolling_neg.replace(0, np.nan)
        
        # MFI = 100 - (100 / (1 + MFR))
        mfi = 100 - (100 / (1 + mfr))
        
        # Handle special cases
        mask_zero_neg = (rolling_neg == 0)
        mfi[mask_zero_neg] = 100
        
        return mfi
