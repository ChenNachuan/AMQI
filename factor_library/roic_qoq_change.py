import pandas as pd
import numpy as np
from .base_factor import BaseFactor
from scripts.utils.financial_utils import convert_ytd_to_ttm


class RoicQoqChange(BaseFactor):
    """
    Quarter-over-Quarter Change in ROIC.
    This factor calculates the difference between the latest quarter's ROIC and the ROIC from four quarters ago.
    The formula is as follows:
    ROIC_Q(t) = (Operating Profit / Revenue) * (Revenue / Invested Capital)
    ROIC_Q(t) - ROIC_Q(t-4)
    """

    @property
    def name(self) -> str:
        return "roic_qoq_change"

    @property
    def required_fields(self) -> list:
        return ['operate_profit', 'revenue', 'invest_capital']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate the ROIC for the current quarter
        roic_current = (df['operate_profit'] / df['revenue']) * (df['revenue'] / df['invest_capital'])

        # Lag ROIC (from 4 quarters ago)
        roic_lag = (df['operate_profit'].shift(4) / df['revenue'].shift(4)) * (
                    df['revenue'].shift(4) / df['invest_capital'].shift(4))

        # Calculate the Quarter-over-Quarter change in ROIC
        roic_qoq_change = roic_current - roic_lag

        # Ensure numeric
        roic_qoq_change = pd.to_numeric(roic_qoq_change, errors='coerce')

        result = pd.DataFrame({
            self.name: roic_qoq_change,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
