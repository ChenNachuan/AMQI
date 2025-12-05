import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class TotalAssetTurnover(BaseFactor):
    """
    Total Asset Turnover (TTM).
    This factor calculates the total asset turnover ratio using the formula:
    Total Asset Turnover (TTM) = Revenue_TTM / Average Total Assets
    Average Total Assets = (Total Assets_beginning + Total Assets_end) / 2
    """

    @property
    def name(self) -> str:
        return "total_asset_turnover"

    @property
    def required_fields(self) -> list:
        return ['revenue', 'total_assets']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Get TTM Revenue
        revenue_ttm = df['revenue']

        # Calculate Average Total Assets
        avg_total_assets = (df['total_assets'] + df['total_assets'].shift(1)) / 2

        # Calculate Total Asset Turnover
        total_asset_turnover = revenue_ttm / avg_total_assets

        # Ensure numeric
        total_asset_turnover = pd.to_numeric(total_asset_turnover, errors='coerce')

        result = pd.DataFrame({
            self.name: total_asset_turnover,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
