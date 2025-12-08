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
        # Calculate Interest Coverage Ratio
        # Handle missing interest expense (assume 0 if missing)
        int_exp = df['int_exp'].fillna(0)
        
        # Create a mask for zero interest expense
        zero_int_mask = (int_exp == 0)
        
        # Calculate ratio where int_exp != 0
        interest_coverage_ratio = df['ebit'] / int_exp
        
        # Handle zero interest expense cases
        # If int_exp is 0 and EBIT >= 0, it's very good (infinite coverage). Cap at 100.
        interest_coverage_ratio.loc[zero_int_mask & (df['ebit'] >= 0)] = 100
        # If int_exp is 0 and EBIT < 0, it's bad (negative coverage). Cap at -100.
        interest_coverage_ratio.loc[zero_int_mask & (df['ebit'] < 0)] = -100

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
