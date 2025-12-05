import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class OpCashRatio(BaseFactor):
    """
    Operating Cash Flow to Revenue Ratio (TTM).
    This ratio measures the net cash flow generated from operating activities in relation to the total revenue over the last 12 months (TTM).
    Formula: Operating Cash Flow (TTM) / Revenue (TTM).

    Note: TTM is calculated by summing the last four quarters of data.
    """

    @property
    def name(self) -> str:
        return "op_cash_ratio"

    @property
    def required_fields(self) -> list:
        return ['op_income', 'revenue']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate TTM Operating Cash Flow by summing the last four quarters' operating cash flow
        ttm_op_cash_flow = df['op_income'].rolling(4).sum()

        # Calculate TTM Revenue by summing the last four quarters' revenue
        ttm_revenue = df['revenue'].rolling(4).sum()

        # Calculate the Operating Cash Flow to Revenue Ratio (TTM)
        factor_value = ttm_op_cash_flow / ttm_revenue

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

