import pandas as pd
import numpy as np
from .base_factor import BaseFactor
from scripts.utils.financial_utils import convert_ytd_to_ttm


class QuarterlyRoic(BaseFactor):
    """
    Quarterly Return on Invested Capital (ROIC).
    This factor calculates the quarterly ROIC by using EBIT after excluding non-recurring gains and losses
    and applying the formula:
    ROIC = (EBIT * (1 - Tax Rate)) / Invested Capital
    """

    @property
    def name(self) -> str:
        return "quarterly_roic"

    @property
    def required_fields(self) -> list:
        return ['operate_profit', 'int_exp', 'total_hldr_eqy_inc_min_int', 'interestdebt']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Default Tax Rate: 25%
        tax_rate = 0.25

        # Calculate EBIT (Earnings Before Interest and Taxes)
        ebit = df['operate_profit'] + df['int_exp']

        # Calculate Invested Capital
        invested_capital = df['total_hldr_eqy_inc_min_int'] + df['interestdebt']

        # Calculate ROIC
        roic = (ebit * (1 - tax_rate)) / invested_capital

        # Ensure numeric
        roic = pd.to_numeric(roic, errors='coerce')

        result = pd.DataFrame({
            self.name: roic,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
