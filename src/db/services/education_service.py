from db.models.db_insert_params import Db_Insert_Params
from db.pandas_utils import validate_columns, handle_nan_values

class EducationDbService:
  def insert_education_stats(self, params: Db_Insert_Params):
    conn, df, df_columns = params

    validate_columns(df, df_columns)
    df = handle_nan_values(df)
    all_rows = df[df_columns].values.tolist()

    batch_size = 1000
    cursor = conn.cursor()

    # Process in batches
    for i in range(0, len(all_rows), batch_size):
      batch = all_rows[i:i+batch_size]
      cursor.executemany("""
          INSERT IGNORE INTO education_stats (
              country_code, 
              indicator_code, 
              year, 
              value
          )
          VALUES (%s, %s, %s, %s)
      """, batch)
      
      conn.commit()