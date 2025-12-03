import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class RVIDiffFactor(BaseFactor):
    """
    RVI Diff Factor.
    RVI差值因子 - RVI与信号线的差值。
    
    因子含义：
    - factor > 0：RVI在信号线上方，多头占优
    - factor < 0：RVI在信号线下方，空头占优
    - |factor|越大：偏离度越大，趋势越强
    
    选股逻辑：
    - 做多策略：选择差值最大的股票（RVI远高于Signal）
    - 做空策略：选择差值最小的股票（RVI远低于Signal）
    - 趋势强度：差值绝对值越大，趋势越强
    """
    
    def __init__(self, signal_period: int = 4):
        """
        Initialize RVI Diff Factor.
        
        Args:
            signal_period: Signal line period, default 4.
        """
        self.signal_period = signal_period
    
    @property
    def name(self) -> str:
        return "RVI_Diff"
        
    @property
    def required_fields(self) -> list:
        return ['open', 'high', 'low', 'close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate RVI Diff Factor.
        
        Formula:
        factor = RVI - Signal
        
        Args:
            df: Daily dataframe with 'open', 'high', 'low', 'close'.
            
        Returns:
            DataFrame with 'RVI_Diff' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        def compute_diff_for_stock(group):
            """Compute RVI diff for a single stock."""
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
            
            # Calculate difference
            diff = rvi - signal
            
            return pd.Series(diff, index=group.index)
        
        # Calculate diff for each stock
        diff_values = df.groupby('ts_code', group_keys=False).apply(compute_diff_for_stock)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: diff_values,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
