import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class PegDyRatio(BaseFactor):
    """
    PEG-DY Ratio (PEG-Dividend Yield Ratio).
    This factor calculates the ratio of PEG (Price-to-Earnings Growth) to Dividend Yield.
    The formula is as follows:
    PEG-DY = PEG / Dividend Yield
    Where:
    PEG = P/E / EPS Growth Rate
    Dividend Yield = Dividend / Price
    """

    @property
    def name(self) -> str:
        return "peg_dy_ratio"

    @property
    def required_fields(self) -> list:
        return ['pe_ttm', 'eps_yoy', 'cash_div', 'close']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate PEG
        peg = df['pe_ttm'] / df['eps_yoy']

        # Calculate Dividend Yield
        dividend_yield = df['cash_div'] / df['close']

        # Calculate PEG-DY Ratio
        peg_dy = peg / dividend_yield

        # Ensure numeric
        peg_dy = pd.to_numeric(peg_dy, errors='coerce')

        result = pd.DataFrame({
            self.name: peg_dy,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
