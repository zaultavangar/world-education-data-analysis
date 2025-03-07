from mysql.connector import MySQLConnection
from db.models.db_insert_params import Db_Insert_Params
from db.pandas_utils import validate_columns, handle_nan_values

class IndicatorDbService:
  def insert_indicator_metadata(self, params: Db_Insert_Params):
    conn, df, df_columns = params
    
    validate_columns(df, df_columns)
    df = handle_nan_values(df)
    rows = df[df_columns].values.tolist()

    cursor = conn.cursor()

    cursor.executemany("""
        INSERT IGNORE INTO indicator (
            indicator_code, 
            indicator_name, 
            source_note, 
            source_organization
        )
        VALUES (%s, %s, %s, %s)
    """, rows)

    conn.commit()

  def get_indicator_mappings(self, conn: MySQLConnection):
    cursor = conn.cursor()

    cursor.execute("""
      SELECT indicator_code, indicator_name
      FROM indicator;
    """)

    results = cursor.fetchall()

    indicator_map = {code: name for code, name in results}

    return indicator_map
  


