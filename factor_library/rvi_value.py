import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class RVIValueFactor(BaseFactor):
    """
    RVI Value Factor.
    RVI原始值因子 - 直接使用RVI值作为因子。
    
    因子含义：
    - RVI > 0：多头动能（收盘价高于开盘价）
    - RVI < 0：空头动能（收盘价低于开盘价）
    - |RVI|越大：动能越强
    
    选股逻辑：
    - 做多策略：选择RVI值最大的股票（强势股）
    - 做空策略：选择RVI值最小的股票（弱势股）
    """
    
    @property
    def name(self) -> str:
        return "RVI_Value"
        
    @property
    def required_fields(self) -> list:
        return ['open', 'high', 'low', 'close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate RVI Value Factor.
        
        Formula:
        1. Vigor = (Close - Open) / (High - Low)
        2. Numerator = WMA(Vigor, 4) with weights (1, 2, 2, 1) / 6
        3. Range = High - Low
        4. Denominator = WMA(Range, 4) with weights (1, 2, 2, 1) / 6
        5. RVI = Numerator / Denominator
        
        Args:
            df: Daily dataframe with 'open', 'high', 'low', 'close'.
            
        Returns:
            DataFrame with 'RVI_Value' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        def compute_rvi_for_stock(group):
            """Compute RVI for a single stock."""
            open_prices = group['open'].values
            high_prices = group['high'].values
            low_prices = group['low'].values
            close_prices = group['close'].values
            
            n = len(close_prices)
            
            # Step 1: Calculate Vigor
            range_hl = high_prices - low_prices
            vigor = np.where(
                range_hl != 0,
                (close_prices - open_prices) / range_hl,
                0.0
            )
            
            # Step 2: Calculate Numerator (WMA of Vigor)
            numerator = np.full(n, np.nan)
            for i in range(3, n):
                numerator[i] = (vigor[i-3] + 2*vigor[i-2] + 2*vigor[i-1] + vigor[i]) / 6
            
            # Step 3: Calculate Denominator (WMA of Range)
            denominator = np.full(n, np.nan)
            for i in range(3, n):
                denominator[i] = (range_hl[i-3] + 2*range_hl[i-2] + 2*range_hl[i-1] + range_hl[i]) / 6
            
            # Step 4: Calculate RVI
            rvi = np.where(
                (denominator != 0) & (~np.isnan(denominator)),
                numerator / denominator,
                np.nan
            )
            
            return pd.Series(rvi, index=group.index)
        
        # Calculate RVI for each stock
        rvi_values = df.groupby('ts_code', group_keys=False).apply(compute_rvi_for_stock)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: rvi_values,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
