import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class epsurplus(BaseFactor):
    """
    Earnings Surplus Per Share.
    Earnings surplus is a legal reserve deducted from the post-tax profit, used to offset losses, increase capital, or distribute dividends.
    The formula is: Earnings Surplus / Total Shares.
    """

    @property
    def name(self) -> str:
        return "epsurplus"

    @property
    def required_fields(self) -> list:
        return ['surplus_rese', 'total_share']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Earnings surplus per share = surplus reserve / total share
        factor_value = df['surplus_rese'] / df['total_share']

        # Ensure numeric and handle division by zero
        factor_value = pd.to_numeric(factor_value, errors='coerce')

        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
