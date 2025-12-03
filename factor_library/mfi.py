
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class MoneyFlowIndex(BaseFactor):
    """
    Money Flow Index (MFI) Factor.
    Volume-weighted RSI.
    """
    
    @property
    def name(self) -> str:
        return "MFI"
        
    @property
    def required_fields(self) -> list:
        return ['high', 'low', 'close', 'vol']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate MFI.
        
        Args:
            df: Daily dataframe with 'high', 'low', 'close', 'vol'.
            
        Returns:
            DataFrame with 'MFI' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        window = 14
        
        # Typical Price
        tp = (df['high'] + df['low'] + df['close']) / 3
        
        # Raw Money Flow
        rmf = tp * df['vol']
        
        # Positive and Negative Money Flow
        # We need previous TP to determine direction
        prev_tp = df.groupby('ts_code')['close'].shift(1) # Using close shift just to align index, but we need shift of TP
        # Actually better to shift TP itself
        # But we can't easily shift a Series within groupby without apply or transform if we want to be safe about boundaries.
        # However, since we sorted by ts_code and trade_date, we can just shift and mask.
        # But let's use groupby shift for safety.
        
        # Re-calculate TP shift properly
        # Since tp is a Series aligned with df, we can group it by df['ts_code']
        prev_tp = tp.groupby(df['ts_code']).shift(1)
        
        # Direction
        # If TP > Prev TP, flow is positive. If TP < Prev TP, flow is negative.
        # If equal, discard? Standard MFI usually discards or treats as 0.
        
        pos_flow = pd.Series(0.0, index=df.index)
        neg_flow = pd.Series(0.0, index=df.index)
        
        pos_mask = tp > prev_tp
        neg_mask = tp < prev_tp
        
        pos_flow[pos_mask] = rmf[pos_mask]
        neg_flow[neg_mask] = rmf[neg_mask]
        
        # Rolling Sums
        rolling_pos = pos_flow.groupby(df['ts_code']).rolling(window).sum().reset_index(0, drop=True)
        rolling_neg = neg_flow.groupby(df['ts_code']).rolling(window).sum().reset_index(0, drop=True)
        
        # Money Flow Ratio
        # Handle division by zero
        mfr = rolling_pos / rolling_neg.replace(0, np.nan)
        
        # MFI = 100 - (100 / (1 + MFR))
        mfi = 100 - (100 / (1 + mfr))
        
        # Fill NaNs where neg_flow was 0 (meaning MFR is infinite, MFI should be 100)
        # If rolling_neg is 0 and rolling_pos > 0, MFI is 100.
        # If both are 0, MFI is usually 50 or NaN. Let's leave as NaN or handle if needed.
        # Fix: Fill with 50 (Neutral) or leave as NaN. 
        # The user requested: "Fill with 50 (Neutral) or leave as NaN".
        # Let's fill with 50 for stability in signals.
        mfi = mfi.fillna(50) 
        
        # If neg is 0, it means no negative moves. So MFI should be 100.
        mask_zero_neg = (rolling_neg == 0)
        mfi[mask_zero_neg] = 100
        
        # Prepare result
        result = pd.DataFrame({
            self.name: mfi,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
