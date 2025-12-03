import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class PVTMADeviation(BaseFactor):
    """
    PVT Moving Average Deviation Factor.
    Measures standardized deviation of PVT from its moving average.
    Based on mean reversion theory: extreme deviations tend to revert.
    factor = -(PVT - PVT_MA) / PVT_STD
    - Positive factor: PVT below MA, potential bounce (buy signal)
    - Negative factor: PVT above MA, potential pullback (sell signal)
    """
    
    def __init__(self, ma_window: int = 20):
        """
        Initialize PVT MA Deviation Factor.
        
        Args:
            ma_window: Moving average window (default: 20 days).
        """
        self.ma_window = ma_window
    
    @property
    def name(self) -> str:
        return "PVT_MA_Deviation"
        
    @property
    def required_fields(self) -> list:
        return ['close', 'vol']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate PVT MA deviation factor.
        
        Args:
            df: Daily dataframe with 'close', 'vol'.
            
        Returns:
            DataFrame with 'PVT_MA_Deviation' column.
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
            
            # Calculate standardized deviation
            factor = np.zeros(n)
            for i in range(self.ma_window, n):
                window_data = pvt[i - self.ma_window:i]
                
                pvt_ma = np.mean(window_data)
                pvt_std = np.std(window_data)
                
                if pvt_std > 1e-8:
                    deviation = (pvt[i] - pvt_ma) / pvt_std
                    factor[i] = -deviation  # Reverse signal
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
