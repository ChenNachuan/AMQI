import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class DebtGrowthRate(BaseFactor):
    """
    Annual Total Debt Log Growth Rate.
    This factor calculates the log growth rate of total debt from the previous year to the current year.
    Formula: ln(TotalDebt_t / TotalDebt_t-1).
    """

    @property
    def name(self) -> str:
        return "debt_growth_rate"

    @property
    def required_fields(self) -> list:
        return ['total_liab']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate the log growth rate of total debt
        debt_growth = np.log(df['total_liab'] / df['total_liab'].shift(1))

        # Ensure numeric and handle potential errors
        debt_growth = pd.to_numeric(debt_growth, errors='coerce')

        result = pd.DataFrame({
            self.name: debt_growth,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
