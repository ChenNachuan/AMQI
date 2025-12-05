import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class CagrCapex(BaseFactor):
    """
    Capital Expenditure Growth Rate (CAGR-N).
    This factor calculates the capital expenditure growth rate over the past N years.
    The formula is as follows:
    CAGR-N = (CapitalExpenditure_TTM - CapitalExpenditure_TTM-N) / CapitalExpenditure_TTM-N
    """

    @property
    def name(self) -> str:
        return "cagr_capex"

    @property
    def required_fields(self) -> list:
        return ['capex']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        N = 2  # Default to N=2 (2 years or 24 months)

        # Get TTM Capital Expenditure
        capex_ttm = df['capex']

        # Get N years ago TTM Capital Expenditure
        capex_ttm_n = df['capex'].shift(N * 4)  # N years ago, assuming quarterly data

        # Calculate Capital Expenditure Growth Rate (CAGR-N)
        cagr_capex = (capex_ttm - capex_ttm_n) / capex_ttm_n

        # Ensure numeric
        cagr_capex = pd.to_numeric(cagr_capex, errors='coerce')

        result = pd.DataFrame({
            self.name: cagr_capex,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
