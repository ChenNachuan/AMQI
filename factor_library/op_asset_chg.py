
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class OpAssetChg(BaseFactor):
    """
    Change in Operating Assets.
    Delta Current Operating Assets / Average Total Assets.
    Operating Assets approx = Accounts Receivable + Inventories.
    """
    
    @property
    def name(self) -> str:
        return "OpAssetChg"
        
    @property
    def required_fields(self) -> list:
        return ['accounts_receiv', 'inventories', 'total_assets']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)
        
        # Operating Assets
        op_assets = df['accounts_receiv'].fillna(0) + df['inventories'].fillna(0)
        
        # Delta Operating Assets (Year over Year change)
        # Using shift(4) for YoY change in quarterly data
        delta_op = op_assets - df.groupby('ts_code')['accounts_receiv'].shift(4).fillna(0) - df.groupby('ts_code')['inventories'].shift(4).fillna(0)
        # Or simpler: op_assets - op_assets.shift(4)
        # But we need to be careful with groupby
        
        op_assets_series = pd.Series(op_assets, index=df.index)
        delta_op = op_assets_series - df.groupby('ts_code', group_keys=False).apply(lambda x: (x['accounts_receiv'].fillna(0) + x['inventories'].fillna(0)).shift(4))
        
        # Average Total Assets
        avg_assets = df.groupby('ts_code')['total_assets'].rolling(4).mean().reset_index(level=0, drop=True)
        
        factor_value = delta_op / avg_assets
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)
        
        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })
        
        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']
            
        return result
