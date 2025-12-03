import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class PVTDivergence(BaseFactor):
    """
    PVT Divergence Factor.
    Detects divergence between price and PVT trend.
    - Bearish divergence: price new high but PVT not new high -> factor = 1.0 (sell signal)
    - Bullish divergence: price new low but PVT not new low -> factor = -1.0 (buy signal)
    Returns: Smoothed divergence signal (5-day MA).
    """
    
    def __init__(self, divergence_window: int = 60):
        """
        Initialize PVT Divergence Factor.
        
        Args:
            divergence_window: Window for detecting divergence (default: 60 days).
        """
        self.divergence_window = divergence_window
    
    @property
    def name(self) -> str:
        return "PVT_Divergence"
        
    @property
    def required_fields(self) -> list:
        return ['close', 'high', 'low', 'vol']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate PVT divergence factor.
        
        Args:
            df: Daily dataframe with 'close', 'high', 'low', 'vol'.
            
        Returns:
            DataFrame with 'PVT_Divergence' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        def calculate_for_stock(group):
            close_arr = group['close'].values
            high_arr = group['high'].values
            low_arr = group['low'].values
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
            
            # Calculate divergence
            factor = np.zeros(n)
            for i in range(self.divergence_window, n):
                window_slice = slice(i - self.divergence_window, i)
                
                price_high_max = np.max(high_arr[window_slice])
                price_low_min = np.min(low_arr[window_slice])
                pvt_high_max = np.max(pvt[window_slice])
                pvt_low_min = np.min(pvt[window_slice])
                
                is_price_new_high = high_arr[i] >= price_high_max
                is_price_new_low = low_arr[i] <= price_low_min
                is_pvt_new_high = pvt[i] >= pvt_high_max
                is_pvt_new_low = pvt[i] <= pvt_low_min
                
                if is_price_new_high and not is_pvt_new_high:
                    factor[i] = 1.0  # Bearish divergence
                elif is_price_new_low and not is_pvt_new_low:
                    factor[i] = -1.0  # Bullish divergence
                else:
                    factor[i] = 0.0
            
            # 5-day MA smoothing
            smoothed_factor = np.zeros(n)
            for i in range(4, n):
                smoothed_factor[i] = np.mean(factor[i-4:i+1])
            
            return pd.Series(smoothed_factor, index=group.index)
        
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
