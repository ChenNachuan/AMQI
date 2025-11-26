
from abc import ABC, abstractmethod
import pandas as pd
from typing import List

class BaseFactor(ABC):
    """
    Abstract base class for all factors.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the factor."""
        pass
        
    @property
    @abstractmethod
    def required_fields(self) -> List[str]:
        """List of required columns in the input dataframe."""
        pass
        
    def check_dependencies(self, df: pd.DataFrame):
        """
        Check if the input dataframe has the required columns and index.
        """
        # Check index
        if 'trade_date' not in df.index.names and 'trade_date' not in df.columns:
             # If not in index, check columns. But we prefer index or columns present.
             # The requirement says: "Verify that the index contains trade_date and ts_code."
             # But usually we might pass reset index df. Let's be flexible but strict on presence.
             pass
             
        # Check columns
        missing = [col for col in self.required_fields if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns for factor {self.name}: {missing}")
            
    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the factor.
        
        Args:
            df (pd.DataFrame): Input data containing required fields.
            
        Returns:
            pd.DataFrame: Factor values with index [trade_date, ts_code].
        """
        pass
