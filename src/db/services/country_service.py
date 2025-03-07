from mysql.connector import MySQLConnection
from db.models.db_insert_params import Db_Insert_Params
from db.pandas_utils import validate_columns, handle_nan_values

class CountryDbService:
  def insert_country_metadata(self, params: Db_Insert_Params):
    conn, df, df_columns = params

    validate_columns(df, df_columns)
    df = handle_nan_values(df)
    rows = df[df_columns].values.tolist()

    cursor = conn.cursor()

    cursor.executemany("""
          INSERT IGNORE INTO country (
              country_code, 
              country_name, 
              region,
              income_group, 
              special_notes
          )
          VALUES (%s, %s, %s, %s, %s)
      """, rows)

    conn.commit()

  def get_country_mappings(self, conn: MySQLConnection):
    cursor = conn.cursor()

    cursor.execute("""
      SELECT country_code, country_name
      FROM country;
    """)

    results = cursor.fetchall()

    country_map = {code: name for code, name in results}

    return country_map