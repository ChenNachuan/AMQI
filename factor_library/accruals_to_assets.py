import pandas as pd
import numpy as np
from .base_factor import BaseFactor
from scripts.utils.financial_utils import convert_ytd_to_ttm


class AccrualsToAssets(BaseFactor):
    """
    Total Accruals to Total Assets Ratio.
    Calculates the ratio of total accruals (Net Income - CFO) to the average assets for the past 12 months.
    The formula is as follows:
    Accruals_TTM = (NetIncome_TTM - CFO_TTM) / AverageAssets
    AverageAssets = (TotalAssets_Beginning + TotalAssets_Ending) / 2
    """

    @property
    def name(self) -> str:
        return "accruals_to_assets"

    @property
    def required_fields(self) -> list:
        return ['netprofit_ttm', 'cfo_ttm', 'total_assets_beginning', 'total_assets_ending']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate TTM for Net Income and CFO (Cash Flow from Operations)
        df = convert_ytd_to_ttm(df, 'netprofit_ttm')
        df = convert_ytd_to_ttm(df, 'cfo_ttm')

        # Calculate Average Assets
        avg_assets = (df['total_assets_beginning'] + df['total_assets_ending']) / 2

        # Calculate Total Accruals
        accruals_ttm = df['netprofit_ttm'] - df['cfo_ttm']

        # Calculate Accruals to Assets ratio
        accruals_to_assets_ratio = accruals_ttm / avg_assets

        # Ensure numeric
        accruals_to_assets_ratio = pd.to_numeric(accruals_to_assets_ratio, errors='coerce')

        result = pd.DataFrame({
            self.name: accruals_to_assets_ratio,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
