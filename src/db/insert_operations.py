from sqlite3 import Connection
from pandas import DataFrame
from dataclasses import dataclass

# Supports DB insert operations

@dataclass
class Db_Insert_Params:
   conn: Connection
   df: DataFrame
   df_columns: list # List of required columns for validation

   def __iter__(self):
      return iter((self.conn, self.df, self.df_columns))

def insert_country_metadata(params: Db_Insert_Params):
  conn, df, df_columns = params

  validate_columns(df, df_columns)
  rows = df[df_columns].values.tolist()

  cursor = conn.cursor()

  cursor.executemany("""
        INSERT OR IGNORE INTO country (
            country_code, 
            country_name, 
            region,
            income_group, 
            special_notes
        )
        VALUES (?, ?, ?, ?, ?)
    """, rows)

  conn.commit()

def insert_indicator_metadata(params: Db_Insert_Params):
  conn, df, df_columns = params
  
  validate_columns(df, df_columns)
  rows = df[df_columns].values.tolist()

  cursor = conn.cursor()

  cursor.executemany("""
      INSERT OR IGNORE INTO indicator (
          indicator_code, 
          indicator_name, 
          source_note, 
          source_organization
      )
      VALUES (?, ?, ?, ?)
  """, rows)

  conn.commit()

def insert_education_stats(params: Db_Insert_Params):
  conn, df, df_columns = params

  validate_columns(df, df_columns)
  rows = df[df_columns].values.tolist()

  cursor = conn.cursor()
  
  cursor.executemany("""
      INSERT OR IGNORE INTO education_stats (
          country_code, 
          indicator_code, 
          year, 
          value
      )
      VALUES (?, ?, ?, ?)
  """, rows)
  
    
  conn.commit()

def validate_columns(df: DataFrame, required_columns: list):
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")