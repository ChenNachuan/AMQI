
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class SineWMA(BaseFactor):
    """
    Sine Weighted Moving Average (SWMA) Factor.
    Weighted moving average using sine wave weights.
    Returns: Close - SWMA.
    """
    
    @property
    def name(self) -> str:
        return "SWMA"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate SWMA factor (Close - SWMA).
        Using a 5-period window with sine weights.
        
        Args:
            df: Daily dataframe with 'close'.
            
        Returns:
            DataFrame with 'SWMA' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # Define weights for 5-period sine WMA
        # sin(i * pi / (N+1)) for i=1 to N
        # N=5
        # i=1: sin(pi/6) = 0.5
        # i=2: sin(2pi/6) = 0.866
        # i=3: sin(3pi/6) = 1.0
        # i=4: sin(4pi/6) = 0.866
        # i=5: sin(5pi/6) = 0.5
        
        weights = np.array([0.5, 0.8660254, 1.0, 0.8660254, 0.5])
        weights = weights / weights.sum()
        
        # Calculate WMA
        # We can use rolling().apply() but it's slow.
        # Faster: sum of shifted series multiplied by weights.
        # SWMA_t = w1*P_{t-4} + w2*P_{t-3} + w3*P_{t-2} + w4*P_{t-1} + w5*P_t
        # Note: rolling apply usually takes window ending at t.
        # So weights should be applied to [t-4, t-3, t-2, t-1, t].
        # Our weights array is symmetric so order doesn't matter for symmetry, but conceptually:
        # weights[0] applies to t-4 (oldest), weights[4] applies to t (newest).
        
        close = df['close']
        swma = pd.Series(0.0, index=df.index)
        
        # Groupby shift is safer
        shifts = []
        for i in range(5):
            # shift(4-i):
            # i=0: shift(4) -> t-4
            # i=4: shift(0) -> t
            shifts.append(df.groupby('ts_code')['close'].shift(4-i))
            
        # Weighted sum
        weighted_sum = sum(s * w for s, w in zip(shifts, weights))
        
        swma = weighted_sum
        
        # Factor: Close - SWMA
        factor_value = df['close'] - swma
        
        # Prepare result
        result = pd.DataFrame({
            self.name: factor_value,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
