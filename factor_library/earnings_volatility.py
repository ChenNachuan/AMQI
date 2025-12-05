import pandas as pd
import numpy as np
from .base_factor import BaseFactor
from statsmodels.tsa.ar_model import AutoReg


class EarningsVolatility(BaseFactor):
    """
    Earnings Volatility.
    This factor calculates the earnings volatility using the residuals from an AR(1) model on historical earnings.
    The formula is:
    Earnings Volatility = sqrt(Variance(vj)) = std(vj)
    Where vj is the residuals from the AR(1) model.
    """

    @property
    def name(self) -> str:
        return "earnings_volatility"

    @property
    def required_fields(self) -> list:
        return ['netprofit_ttm']  # Can also use eps_ttm if needed

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Using netprofit_ttm as the measure of earnings, but can be replaced by eps_ttm
        earnings = df['netprofit_ttm']

        # Fit an AR(1) model to the historical earnings
        ar_model = AutoReg(earnings, lags=1)
        ar_model_fitted = ar_model.fit()

        # Get the residuals (v_j)
        residuals = ar_model_fitted.resid

        # Calculate the standard deviation of the residuals, which represents the earnings volatility
        earnings_volatility = np.std(residuals)

        # Result as a DataFrame
        result = pd.DataFrame({
            self.name: [earnings_volatility] * len(df),  # Same value for all rows in the DataFrame
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
