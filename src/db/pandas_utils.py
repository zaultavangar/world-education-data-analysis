import numpy as np
from pandas import DataFrame

def validate_columns(df: DataFrame, required_columns: list):
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
def handle_nan_values(df: DataFrame, ):
   df = df.replace(np.nan, None)

   return df