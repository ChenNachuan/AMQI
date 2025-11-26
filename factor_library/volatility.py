
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class Ivff(BaseFactor):
    """
    Idiosyncratic Volatility (IVFF).
    Standard deviation of residuals from Market Model: R_i = alpha + beta * R_m + epsilon.
    Calculated as sqrt(Var(R_i) - beta^2 * Var(R_m)).
    """
    
    @property
    def name(self) -> str:
        return "IVFF"
        
    @property
    def required_fields(self) -> list:
        return ['ret', 'mkt_ret', 'beta']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate IVFF.
        
        Args:
            df: DataFrame with 'ret' (daily return), 'mkt_ret' (market return), and 'beta'.
            
        Returns:
            DataFrame with 'IVFF' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # Define window
        window = 20 # Monthly IVFF usually calculated from daily returns within the month
        # But here we might want rolling or monthly aggregation.
        # The prompt says: "Standard deviation of residuals".
        # And "Use the efficient vectorized approach... Var(eps) = Var(R) - beta^2 * Var(Rm)".
        # This implies we need Var(R) and Var(Rm) over some window.
        # Let's assume a rolling window of 20 days (approx 1 month) to match the "monthly" nature often used,
        # OR we can do it by month if we want monthly factors.
        # However, the prompt asks for "Daily Rolling Logic" for Momentum, maybe for others too?
        # "Ensure they return a DataFrame indexed by [trade_date, ts_code]."
        # Let's use rolling 20 days for daily IVFF.
        
        # Calculate rolling variance
        # Group by ts_code
        var_r = df.groupby('ts_code')['ret'].rolling(window).var().reset_index(0, drop=True)
        
        # Var(Rm) is the same for all stocks on a given day, but we need rolling var of Rm.
        # Since mkt_ret is repeated for each stock (if joined), we can just take rolling var.
        # But wait, mkt_ret is in the df.
        var_rm = df.groupby('ts_code')['mkt_ret'].rolling(window).var().reset_index(0, drop=True)
        
        # Get Beta (assuming it's already in df, or we calculate it? The prompt says "Implement Class Beta" separately.
        # But Ivff needs Beta. So the driver script must calculate Beta first and pass it?
        # Or Ivff calculates Beta internally?
        # The prompt says: "Class Ivff: Implement ... Var(eps) = Var(R) - beta^2 * Var(Rm)".
        # It implies Beta is an input or calculated.
        # If Beta is a separate factor, we should probably assume it's in the input DF if we want to use it.
        # BUT, usually factors are independent.
        # If Ivff depends on Beta, we might need to calculate Beta inside or pass it.
        # Let's assume 'beta' is in df (calculated by Beta factor and merged before calling Ivff? No, driver loops through factors).
        # If driver loops, it might not have merged yet.
        # So Ivff might need to calculate Beta or assume it's passed.
        # Given the "Vectorized approach" hint, it strongly suggests using the formula.
        # Let's assume the user will pass a DF that HAS 'beta'.
        # The driver script should calculate Beta first, then Ivff.
        
        beta = df['beta']
        
        # Calculate Idiosyncratic Variance
        ivar = var_r - (beta ** 2) * var_rm
        
        # Handle negative variance due to floating point or estimation noise
        ivar = ivar.clip(lower=0)
        
        ivff = np.sqrt(ivar)
        
        # Result
        result = pd.DataFrame({
            self.name: ivff,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
