import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class CapexGrowthRate(BaseFactor):
    """
    Capital Expenditure Abnormal Growth Rate (CI).
    This factor measures the deviation of the current capital expenditure ratio (CE) from its 3-year average,
    capturing abnormal capital expenditure behavior.
    Formula: CI_t-1 = CE_t-1 / (CE_t-2 + CE_t-3 + CE_t-4) / 3 - 1.

    Capital Expenditure Ratio: CE_t = (Fixed Assets Change + Intangible Assets Change - Asset Disposal Income) / Revenue_t.
    """

    @property
    def name(self) -> str:
        return "capex_growth_rate"

    @property
    def required_fields(self) -> list:
        return ['fix_assets', 'intan_assets', 'asset_disp_income', 'revenue']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate Capital Expenditure (CAPEX)
        capex = (df['fix_assets'].diff() + df['intan_assets'].diff() - df['asset_disp_income'])

        # Calculate Capital Expenditure Ratio (CE)
        ce = capex / df['revenue']

        # Calculate the 3-year average of CE
        ce_rolling_avg = ce.shift(1).rolling(3).mean()

        # Calculate Capital Expenditure Abnormal Growth Rate (CI)
        factor_value = ce.shift(1) / ce_rolling_avg - 1

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
