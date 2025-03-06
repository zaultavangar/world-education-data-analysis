import os
import pandas as pd

# Load CSV data into dataframes

def read_country_metadata():
  base_dir = os.path.dirname(os.path.abspath(__file__))
  file_path = os.path.join(base_dir, "../../data/world-bank-education/country_metadata.csv")
  
  df = pd.read_csv(file_path)
  df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
  
  country_col = df.pop("Table Name")
  df.insert(1, "Country Name", country_col)
  df_columns = df.columns.tolist()

  return df, df_columns
  
def read_indicator_metadata():
  base_dir = os.path.dirname(os.path.abspath(__file__))
  file_path = os.path.join(base_dir, "../../data/world-bank-education/indicator_metadata.csv")
  
  df = pd.read_csv(file_path)
  df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
  
  df.rename(columns={
    "INDICATOR_CODE": "Indicator Code",
    "INDICATOR_NAME": "Indicator Name",
    "SOURCE_NOTE": "Source Note",
    "SOURCE_ORGANIZATION": "Source Organization",
  }, inplace=True)
  df_columns = df.columns.tolist()

  return df, df_columns

def read_education_stats():
  base_dir = os.path.dirname(os.path.abspath(__file__))
  file_path = os.path.join(base_dir, "../../data/world-bank-education/education_stats.csv")
  
  df = pd.read_csv(file_path, header=2)
  df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

  df_long = df.melt(
    id_vars=["Country Name", "Country Code", "Indicator Name", "Indicator Code"],
    var_name="Year",
    value_name="Value"
  )
  df_long.drop(columns=["Country Name", "Indicator Name"], inplace=True)

  df_long["Year"] = pd.to_numeric(df_long["Year"])
  df_long["Value"] = pd.to_numeric(df_long["Value"])

  df_columns = df_long.columns.tolist()

  return df_long, df_columns