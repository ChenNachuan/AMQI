
import pandas as pd
import numpy as np

class Universe:
    """
    Universe filtering logic.
    """
    
    @staticmethod
    def apply_market_cap_filter(df: pd.DataFrame, threshold_percent: float = 0.3) -> pd.DataFrame:
        """
        Filter out stocks with market cap below the threshold percentile for each day.
        
        Args:
            df: DataFrame containing 'total_mv', 'trade_date', 'ts_code'.
            threshold_percent: Percentile threshold (default 0.3 for bottom 30%).
            
        Returns:
            Filtered DataFrame.
        """
        if 'total_mv' not in df.columns:
            raise ValueError("DataFrame must contain 'total_mv' for market cap filtering.")
            
        # Calculate threshold per date
        # Use transform to broadcast threshold to each row
        daily_thresholds = df.groupby('trade_date')['total_mv'].transform(lambda x: x.quantile(threshold_percent))
        
        # Filter
        mask = df['total_mv'] >= daily_thresholds
        
        return df[mask].copy()
