import pandas as pd
from .base_factor import BaseFactor


class DividendYield(BaseFactor):
    """
    Dividend Yield Factor.
    Defined as: DTTM / MV
    Where:
        - DTTM: Total Dividends in the trailing twelve months (TTM).
        - MV: Market Value (Total market value of the stock).
    """

    @property
    def name(self) -> str:
        return "DividendYield"

    @property
    def required_fields(self) -> list:
        return ['dividends', 'close', 'total_shares']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate TTM Dividends (assuming it's available as 'dividends' in the data)
        df['DTTM'] = df['dividends']  # This field should be filled with TTM dividend info

        # Calculate Market Value (MV)
        df['MV'] = df['close'] * df['total_shares']

        # Calculate Dividend Yield
        df['DividendYield'] = df['DTTM'] / df['MV']

        result = pd.DataFrame({
            self.name: df['DividendYield'],
            'ts_code': df['ts_code'],
            'trade_date': df['trade_date']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()

        return result
