
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class BollingerMiddleSupport(BaseFactor):
    """
    Bollinger Bands Middle Support Factor.
    中轨支撑策略：捕捉上升趋势中的回调买点
    
    选股逻辑：
    1. 当日价格回踩到中轨附近：0.4 < percent_b < 0.6
    2. 前日价格不在此区间：percent_b_prev <= 0.4 或 percent_b_prev >= 0.6
    """
    
    @property
    def name(self) -> str:
        return "Boll_Middle_Support"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Bollinger Bands Middle Support Signal.
        
        Args:
            df: Daily dataframe with 'close'.
            
        Returns:
            DataFrame with 'Boll_Middle_Support' column (1 for signal, 0 for no signal).
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        window = 20
        num_std = 2
        
        # Calculate Bollinger Bands
        rolling = df.groupby('ts_code')['close'].rolling(window)
        ma = rolling.mean().reset_index(0, drop=True)
        std = rolling.std().reset_index(0, drop=True)
        
        upper = ma + num_std * std
        lower = ma - num_std * std
        
        # Calculate Percent B
        denominator = upper - lower
        percent_b = (df['close'] - lower) / denominator
        
        # Calculate previous day's Percent B
        percent_b_prev = df.groupby('ts_code')['close'].transform(
            lambda x: ((x - (x.rolling(window).mean() - num_std * x.rolling(window).std())) / 
                      ((x.rolling(window).mean() + num_std * x.rolling(window).std()) - 
                       (x.rolling(window).mean() - num_std * x.rolling(window).std()))).shift(1)
        )
        
        # Check if in middle zone
        in_middle_zone = (percent_b > 0.4) & (percent_b < 0.6)
        
        # Check if was outside
        was_outside = (percent_b_prev <= 0.4) | (percent_b_prev >= 0.6)
        
        # Generate signal
        signal = (in_middle_zone & was_outside).astype(int)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: signal,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
