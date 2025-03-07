from mysql.connector import MySQLConnection
from pandas import DataFrame
from dataclasses import dataclass

@dataclass
class Db_Insert_Params:
   conn: MySQLConnection
   df: DataFrame
   df_columns: list # List of required columns for validation

   def __iter__(self):
      return iter((self.conn, self.df, self.df_columns))