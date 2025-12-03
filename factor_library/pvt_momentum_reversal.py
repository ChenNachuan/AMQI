import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class PVTMomentumReversal(BaseFactor):
    """
    PVT Momentum Reversal Factor.
    Based on overreaction theory: excessive short-term momentum may reverse.
    factor = -PVT_momentum = -(PVT[t] - PVT[t-period]) / PVT[t-period]
    - Positive factor: PVT declining fast, potential bounce (buy signal)
    - Negative factor: PVT rising fast, potential pullback (sell signal)
    """
    
    def __init__(self, momentum_period: int = 10):
        """
        Initialize PVT Momentum Reversal Factor.
        
        Args:
            momentum_period: Period for momentum calculation (default: 10 days).
        """
        self.momentum_period = momentum_period
    
    @property
    def name(self) -> str:
        return "PVT_Momentum_Reversal"
        
    @property
    def required_fields(self) -> list:
        return ['close', 'vol']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate PVT momentum reversal factor.
        
        Args:
            df: Daily dataframe with 'close', 'vol'.
            
        Returns:
            DataFrame with 'PVT_Momentum_Reversal' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        def calculate_for_stock(group):
            close_arr = group['close'].values
            vol_arr = group['vol'].values
            
            n = len(close_arr)
            
            # Calculate PVT
            pvt = np.zeros(n)
            for i in range(1, n):
                if close_arr[i-1] != 0:
                    price_change_rate = (close_arr[i] - close_arr[i-1]) / close_arr[i-1]
                    pvt[i] = pvt[i-1] + vol_arr[i] * price_change_rate
                else:
                    pvt[i] = pvt[i-1]
            
            # Calculate momentum and reverse
            factor = np.zeros(n)
            for i in range(self.momentum_period, n):
                if pvt[i - self.momentum_period] != 0:
                    pvt_momentum = (pvt[i] - pvt[i - self.momentum_period]) / abs(pvt[i - self.momentum_period])
                    factor[i] = -pvt_momentum  # Reverse signal
                else:
                    factor[i] = 0.0
            
            return pd.Series(factor, index=group.index)
        
        # Calculate for each stock
        factor_values = df.groupby('ts_code', group_keys=False).apply(calculate_for_stock)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: factor_values,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
