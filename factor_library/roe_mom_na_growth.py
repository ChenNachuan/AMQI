import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class ROEMomNAGrowth(BaseFactor):
    """
    ROE MoM minus Net Asset YoY Growth.
    ROE MoM = (ROE(TTM) - ROE(TTM)_t-1) / abs(ROE(TTM)_t-1)
    Net Asset YoY Growth = (BV_t - BV_t-4) / BV_t-4
    """

    @property
    def name(self) -> str:
        return "roe_mom_na_growth"

    @property
    def required_fields(self) -> list:
        return ['roe_ttm', 'total_hldr_eqy_exc_min_int']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # ROE MoM: Change in ROE (TTM) from last quarter
        roe_ttm = df['roe_ttm']
        roe_mom = (roe_ttm - roe_ttm.shift(1)) / abs(roe_ttm.shift(1))

        # Net Asset YoY Growth: Change in net asset (equity) YoY
        bv = df['total_hldr_eqy_exc_min_int']
        net_asset_yoy_growth = (bv - bv.shift(4)) / bv.shift(4)

        # ROE MoM minus Net Asset YoY Growth
        factor_value = roe_mom - net_asset_yoy_growth
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)

        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
