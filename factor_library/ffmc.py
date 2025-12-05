import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class FFMC(BaseFactor):
    """
    Free-Float Market Capitalization.
    Defined as: close_price * free_float_shares
    Where:
        - close_price: daily closing price
        - free_float_shares: daily free circulating shares
    """

    @property
    def name(self) -> str:
        return "FFMC"

    @property
    def required_fields(self) -> list:
        return ['close', 'free_share']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate Free-Float Market Capitalization
        ffmc = df['close'] * df['free_share']

        result = pd.DataFrame({
            self.name: ffmc,
            'ts_code': df['ts_code'],
            'trade_date': df['trade_date']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()

        return result
