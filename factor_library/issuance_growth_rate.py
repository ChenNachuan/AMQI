import pandas as pd
import numpy as np
from .base_factor import BaseFactor
from sklearn.linear_model import LinearRegression


class issuance_growth_rate(BaseFactor):
    """
    Issuance Growth Rate (IGRO).
    This factor uses linear regression to model the issuance over time and calculates the growth rate.
    Formula: IGRO = -β / mean(issuance_t ∈ T).

    Where issuance_t = α + β * t + ε_t is a linear regression model for the issuance over time.
    """

    @property
    def name(self) -> str:
        return "issuance_growth_rate"

    @property
    def required_fields(self) -> list:
        return ['total_share']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Creating time variable t for the linear regression model
        df['t'] = np.arange(len(df)) + 1  # time variable starting from 1

        # Prepare data for linear regression
        # Drop NaNs in total_share
        valid_data = df.dropna(subset=['total_share'])
        if valid_data.empty:
            return pd.DataFrame()
            
        X = valid_data[['t']]  # Time variable
        y = valid_data['total_share']  # Issuance (total share)

        # Perform linear regression to get the slope β
        model = LinearRegression()
        model.fit(X, y)

        # Get the slope (β) from the model
        beta = model.coef_[0]

        # Calculate the mean issuance over the period (mean of total shares)
        mean_issuance = df['total_share'].mean()

        # Calculate IGRO
        igro = -beta / mean_issuance

        # Return the result
        result = pd.DataFrame({
            self.name: [igro] * len(df),
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
