import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class revenue_per_share(BaseFactor):
    """
    Revenue Per Share (TTM).
    This factor calculates the revenue per share over the last 12 months (TTM).
    Formula: Revenue (TTM) / Average Total Shares.

    Average Total Shares = (Initial Shares + Ending Shares) / 2.

    Note: The initial shares are approximated by shifting total_share based on 'end_date'.
    """

    @property
    def name(self) -> str:
        return "revenue_per_share"

    @property
    def required_fields(self) -> list:
        return ['revenue', 'total_share']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Shift total_share to get the previous period's shares (approximating initial shares)
        initial_shares = df['total_share'].shift(1)

        # Ending shares is the current period's total_share
        ending_shares = df['total_share']

        # Calculate the average shares
        average_shares = (initial_shares + ending_shares) / 2

        # Calculate Revenue Per Share
        factor_value = df['revenue'] / average_shares

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
