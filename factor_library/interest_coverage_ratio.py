import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class InterestCoverageRatio(BaseFactor):
    """
    Interest Coverage Ratio.
    This factor calculates the interest coverage ratio using the formula:
    Interest Coverage Ratio = EBIT_TTM / Interest Expense_TTM
    """

    @property
    def name(self) -> str:
        return "interest_coverage_ratio"

    @property
    def required_fields(self) -> list:
        return ['ebit', 'int_exp']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate Interest Coverage Ratio
        interest_coverage_ratio = df['ebit'] / df['int_exp']

        # Ensure numeric
        interest_coverage_ratio = pd.to_numeric(interest_coverage_ratio, errors='coerce')

        result = pd.DataFrame({
            self.name: interest_coverage_ratio,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
