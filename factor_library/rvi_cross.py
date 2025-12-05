import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class RVICrossFactor(BaseFactor):
    """
    RVI Cross Signal Factor.
    RVI交叉信号因子 - 检测RVI与Signal线的交叉。
    
    因子含义：
    - factor = 1：金叉，RVI上穿Signal，动能转强，买入信号
    - factor = -1：死叉，RVI下穿Signal，动能转弱，卖出信号
    - factor = 0：无交叉信号
    
    选股逻辑：
    - 做多策略：选择factor=1的股票（刚出现金叉）
    - 做空策略：选择factor=-1的股票（刚出现死叉）
    - 持仓调整：金叉时买入，死叉时卖出
    """
    
    def __init__(self, signal_period: int = 4):
        """
        Initialize RVI Cross Factor.
        
        Args:
            signal_period: Signal line period, default 4.
        """
        self.signal_period = signal_period
    
    @property
    def name(self) -> str:
        return "RVI_Cross"
        
    @property
    def required_fields(self) -> list:
        return ['open', 'high', 'low', 'close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate RVI Cross Signal Factor.
        
        Formula:
        1. Calculate RVI and Signal line
        2. Detect crossover:
           - Golden Cross (1): RVI[t-1] <= Signal[t-1] and RVI[t] > Signal[t]
           - Death Cross (-1): RVI[t-1] >= Signal[t-1] and RVI[t] < Signal[t]
           - No Cross (0): otherwise
        
        Args:
            df: Daily dataframe with 'open', 'high', 'low', 'close'.
            
        Returns:
            DataFrame with 'RVI_Cross' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        def compute_cross_for_stock(group):
            """Compute RVI cross signal for a single stock."""
            open_prices = group['open'].values
            high_prices = group['high'].values
            low_prices = group['low'].values
            close_prices = group['close'].values
            
            n = len(close_prices)
            
            # Calculate RVI (same as RVI Value)
            # Calculate RVI (same as RVI Value)
            range_hl = high_prices - low_prices
            with np.errstate(divide='ignore', invalid='ignore'):
                vigor = np.divide(
                    close_prices - open_prices,
                    range_hl,
                    out=np.zeros_like(close_prices),
                    where=range_hl != 0
                )
            
            numerator = np.full(n, np.nan)
            for i in range(3, n):
                numerator[i] = (vigor[i-3] + 2*vigor[i-2] + 2*vigor[i-1] + vigor[i]) / 6
            
            denominator = np.full(n, np.nan)
            for i in range(3, n):
                denominator[i] = (range_hl[i-3] + 2*range_hl[i-2] + 2*range_hl[i-1] + range_hl[i]) / 6
            
            with np.errstate(divide='ignore', invalid='ignore'):
                rvi = np.divide(
                    numerator,
                    denominator,
                    out=np.full_like(numerator, np.nan),
                    where=(denominator != 0) & (~np.isnan(denominator))
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
            
            # Detect crossover
            cross_signal = np.zeros(n)
            for i in range(1, n):
                if np.isnan(rvi[i]) or np.isnan(signal[i]) or np.isnan(rvi[i-1]) or np.isnan(signal[i-1]):
                    cross_signal[i] = 0
                elif rvi[i-1] <= signal[i-1] and rvi[i] > signal[i]:
                    cross_signal[i] = 1  # Golden cross
                elif rvi[i-1] >= signal[i-1] and rvi[i] < signal[i]:
                    cross_signal[i] = -1  # Death cross
                else:
                    cross_signal[i] = 0
            
            return pd.Series(cross_signal, index=group.index)
        
        # Calculate cross signal for each stock
        cross_values = df.groupby('ts_code', group_keys=False).apply(compute_cross_for_stock)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: cross_values,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
