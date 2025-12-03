import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class debt_yoy_growth(BaseFactor):
    """
    Annual Total Liabilities YoY Growth.
    This factor calculates the year-over-year (YoY) growth rate of total liabilities.
    Formula: (TotalLiabilities_t - TotalLiabilities_t-1) / TotalLiabilities_t-1.

    Note: The calculation uses total_liab and shifts the data by 4 quarters to approximate the previous year.
    """

    @property
    def name(self) -> str:
        return "debt_yoy_growth"

    @property
    def required_fields(self) -> list:
        return ['total_liab']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate the YoY growth of total liabilities by comparing the current period and the same period last year
        total_liabilities_t = df['total_liab']
        total_liabilities_t_minus_1 = df['total_liab'].shift(4)  # Shift by 4 quarters for YoY comparison

        # Calculate the YoY growth rate
        yoy_growth = (total_liabilities_t - total_liabilities_t_minus_1) / total_liabilities_t_minus_1

        # Ensure numeric and handle potential errors
        yoy_growth = pd.to_numeric(yoy_growth, errors='coerce')

        result = pd.DataFrame({
            self.name: yoy_growth,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
