import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class Logffmv(BaseFactor):
    """
    Log Free Float Market Value.
    Defined as: ln( close_price * free_float_shares )
    Where:
        - close_price: daily closing price
        - free_float_shares: daily free circulating shares
    """

    @property
    def name(self) -> str:
        return "logffmv"

    @property
    def required_fields(self) -> list:
        return ['close', 'free_share']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Free float market value
        ffmv = df['close'] * df['free_share']

        # Log transformation
        factor_value = np.log(ffmv.replace(0, np.nan))

        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)

        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'trade_date': df['trade_date']
        })

        return result
