import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class RVIStrengthFactor(BaseFactor):
    """
    RVI Strength Factor.
    RVI交叉强度因子 - 交叉时的RVI变化率。
    
    因子含义：
    - factor > 0：金叉时RVI快速上升，突破力度强
    - factor < 0：死叉时RVI快速下降，下跌力度强
    - |factor|越大：交叉时动量变化越剧烈
    
    选股逻辑：
    - 做多策略：选择金叉且strength最大的股票（强力突破）
    - 做空策略：选择死叉且strength最小的股票（快速下跌）
    - 信号过滤：过滤掉strength较小的弱交叉信号
    """
    
    def __init__(self, signal_period: int = 4):
        """
        Initialize RVI Strength Factor.
        
        Args:
            signal_period: Signal line period, default 4.
        """
        self.signal_period = signal_period
    
    @property
    def name(self) -> str:
        return "RVI_Strength"
        
    @property
    def required_fields(self) -> list:
        return ['open', 'high', 'low', 'close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate RVI Strength Factor.
        
        Formula:
        1. Calculate RVI change rate: (RVI[t] - RVI[t-1]) / |RVI[t-1]|
        2. If crossover occurs, factor = RVI change rate
        3. Otherwise, factor = 0
        
        Args:
            df: Daily dataframe with 'open', 'high', 'low', 'close'.
            
        Returns:
            DataFrame with 'RVI_Strength' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        def compute_strength_for_stock(group):
            """Compute RVI strength for a single stock."""
            open_prices = group['open'].values
            high_prices = group['high'].values
            low_prices = group['low'].values
            close_prices = group['close'].values
            
            n = len(close_prices)
            
            # Calculate RVI
            range_hl = high_prices - low_prices
            vigor = np.where(
                range_hl != 0,
                (close_prices - open_prices) / range_hl,
                0.0
            )
            
            numerator = np.full(n, np.nan)
            for i in range(3, n):
                numerator[i] = (vigor[i-3] + 2*vigor[i-2] + 2*vigor[i-1] + vigor[i]) / 6
            
            denominator = np.full(n, np.nan)
            for i in range(3, n):
                denominator[i] = (range_hl[i-3] + 2*range_hl[i-2] + 2*range_hl[i-1] + range_hl[i]) / 6
            
            rvi = np.where(
                (denominator != 0) & (~np.isnan(denominator)),
                numerator / denominator,
                np.nan
            )
            
            # Calculate Signal line
            signal = np.full(n, np.nan)
            if self.signal_period == 4:
                for i in range(3, n):
                    if not np.isnan(rvi[i-3:i+1]).any():
                        signal[i] = (rvi[i-3] + 2*rvi[i-2] + 2*rvi[i-1] + rvi[i]) / 6
            else:
                for i in range(self.signal_period-1, n):
                    if not np.isnan(rvi[i-self.signal_period+1:i+1]).any():
                        signal[i] = np.mean(rvi[i-self.signal_period+1:i+1])
            
            # Calculate RVI change rate
            rvi_change = np.zeros(n)
            for i in range(1, n):
                if not np.isnan(rvi[i]) and not np.isnan(rvi[i-1]) and rvi[i-1] != 0:
                    rvi_change[i] = (rvi[i] - rvi[i-1]) / abs(rvi[i-1])
                else:
                    rvi_change[i] = 0
            
            # Detect crossover and record strength
            strength = np.zeros(n)
            for i in range(1, n):
                if np.isnan(rvi[i]) or np.isnan(signal[i]) or np.isnan(rvi[i-1]) or np.isnan(signal[i-1]):
                    strength[i] = 0
                elif (rvi[i-1] <= signal[i-1] and rvi[i] > signal[i]) or \
                     (rvi[i-1] >= signal[i-1] and rvi[i] < signal[i]):
                    # Crossover occurred
                    strength[i] = rvi_change[i]
                else:
                    strength[i] = 0
            
            return pd.Series(strength, index=group.index)
        
        # Calculate strength for each stock
        strength_values = df.groupby('ts_code', group_keys=False).apply(compute_strength_for_stock)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: strength_values,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
