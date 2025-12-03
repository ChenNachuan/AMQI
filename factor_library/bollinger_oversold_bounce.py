
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class BollingerOversoldBounce(BaseFactor):
    """
    Bollinger Bands Oversold Bounce Factor.
    超卖反弹策略：捕捉价格触及下轨后反弹的机会
    适用场景：震荡市
    
    选股逻辑：
    1. 前一日价格触及下轨：percent_b_prev < 0.1
    2. 当日价格反弹离开下轨：percent_b > 0.1
    3. 价格仍在下半部分：percent_b < 0.5
    """
    
    @property
    def name(self) -> str:
        return "Boll_Oversold_Bounce"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Bollinger Bands Oversold Bounce Signal.
        
        Args:
            df: Daily dataframe with 'close'.
            
        Returns:
            DataFrame with 'Boll_Oversold_Bounce' column (1 for signal, 0 for no signal).
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
            (percent_b_prev < 0.1) &      # 前日触及下轨
            (percent_b > 0.1) &            # 当日反弹
            (percent_b < 0.5)              # 仍在下半部分
        ).astype(int)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: signal,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
