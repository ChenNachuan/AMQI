import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class LogMarketCap(BaseFactor):
    """
    Log Market Capitalization Factor.
    Defined as: ln(ClosePrice * TotalShares)
    Where:
        - ClosePrice: daily closing price
        - TotalShares: daily total shares
    """

    @property
    def name(self) -> str:
        return "LogMarketCap"

    @property
    def required_fields(self) -> list:
        return ['close', 'total_shares']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate log of market capitalization
        log_market_cap = np.log(df['close'] * df['total_shares'])

        result = pd.DataFrame({
            self.name: log_market_cap,
            'ts_code': df['ts_code'],
            'trade_date': df['trade_date']
        })

        return result
