
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class BollingerBreakoutUpper(BaseFactor):
    """
    Bollinger Bands Breakout Upper Factor.
    突破上轨策略：追踪强势股
    适用场景：趋势市
    
    选股逻辑：
    1. 前一日价格在上轨下方：percent_b_prev < 0.9
    2. 当日价格突破上轨：percent_b > 0.9
    """
    
    @property
    def name(self) -> str:
        return "Boll_Breakout_Upper"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Bollinger Bands Breakout Upper Signal.
        
        Args:
            df: Daily dataframe with 'close'.
            
        Returns:
            DataFrame with 'Boll_Breakout_Upper' column (1 for signal, 0 for no signal).
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
        
        # Generate signal
        signal = (
            (percent_b_prev < 0.9) &      # 前日在上轨下方
            (percent_b > 0.9)              # 当日突破上轨
        ).astype(int)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: signal,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
