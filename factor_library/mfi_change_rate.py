import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class MFIChangeRate(BaseFactor):
    """
    MFI Change Rate Factor.
    Measures the acceleration of money flow (capital inflow/outflow speed).
    
    Factor Logic:
    - MFI_Change_Rate > 0: Accelerating capital inflow (bullish)
    - MFI_Change_Rate < 0: Accelerating capital outflow (bearish)
    - Larger absolute value indicates stronger momentum
    
    Selection Strategy:
    - Long high change rate: Buy stocks with accelerating capital inflow
    - Threshold: Typically > +5 for strong signal, < -5 for weak signal
    """
    
    def __init__(self, mfi_period: int = 14, change_period: int = 5):
        """
        Initialize MFI Change Rate Factor.
        
        Args:
            mfi_period: MFI calculation period (default: 14 days)
            change_period: Change rate calculation period (default: 5 days)
        """
        self.mfi_period = mfi_period
        self.change_period = change_period
    
    @property
    def name(self) -> str:
        return f"MFI_ChangeRate_{self.change_period}d"
        
    @property
    def required_fields(self) -> list:
        return ['high', 'low', 'close', 'vol']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate MFI Change Rate.
        
        Steps:
        1. Calculate basic MFI
        2. MFI_Change_Nd = MFI(t) - MFI(t-N)
        
        Args:
            df: Daily dataframe with 'high', 'low', 'close', 'vol'.
            
        Returns:
            DataFrame with MFI Change Rate column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # Step 1: Calculate basic MFI
        mfi = self._calculate_mfi(df, self.mfi_period)
        
        # Step 2: Calculate N-day change in MFI
        # MFI_Change_Nd = MFI(today) - MFI(N days ago)
        mfi_change = mfi.groupby(df['ts_code']).diff(self.change_period)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: mfi_change,
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
