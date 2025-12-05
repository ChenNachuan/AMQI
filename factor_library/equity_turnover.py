import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class EquityTurnover(BaseFactor):
    """
    Equity Turnover.
    This factor calculates the ratio of revenue over the average equity of the period.
    Formula: Revenue (TTM) / Average Shareholder Equity.

    Average Shareholder Equity = (Initial Shareholder Equity + Ending Shareholder Equity) / 2.

    Note: TTM Revenue is the sum of the last four quarters.
    """

    @property
    def name(self) -> str:
        return "equity_turnover"

    @property
    def required_fields(self) -> list:
        return ['revenue', 'total_hldr_eqy_inc_min_int']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate TTM Revenue by summing the last four quarters' revenue
        ttm_revenue = df['revenue'].rolling(4).sum()

        # Shift total_hldr_eqy_inc_min_int to get the previous period's equity (approximating initial equity)
        initial_equity = df['total_hldr_eqy_inc_min_int'].shift(1)

        # Ending equity is the current period's total_hldr_eqy_inc_min_int
        ending_equity = df['total_hldr_eqy_inc_min_int']

        # Calculate the average equity
        average_equity = (initial_equity + ending_equity) / 2

        # Calculate the Equity Turnover
        factor_value = ttm_revenue / average_equity

        # Ensure numeric and handle potential errors
        factor_value = pd.to_numeric(factor_value, errors='coerce')

        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
