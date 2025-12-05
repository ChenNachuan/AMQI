import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class EpChange60D(BaseFactor):
    """
    60-day Change in Earnings-to-Price Ratio (EP).
    This factor calculates the difference between the current EP and the EP from 60 trading days ago.
    The formula is as follows:
    EP_t - EP_t-60
    """

    @property
    def name(self) -> str:
        return "ep_change_60d"

    @property
    def required_fields(self) -> list:
        return ['netprofit_ttm', 'total_mv']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate the Earnings-to-Price (EP) ratio for the current day
        ep_current = df['netprofit_ttm'] / df['total_mv']

        # Lag EP (from 60 days ago)
        ep_lag_60d = df['netprofit_ttm'].shift(60) / df['total_mv'].shift(60)

        # Calculate the 60-day change in EP
        ep_change_60d = ep_current - ep_lag_60d

        # Ensure numeric
        ep_change_60d = pd.to_numeric(ep_change_60d, errors='coerce')

        result = pd.DataFrame({
            self.name: ep_change_60d,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
