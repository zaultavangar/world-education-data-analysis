import os
import sqlite3

from csv_loader.csv_loader import read_country_metadata, read_education_stats, read_indicator_metadata
from db.schema import create_tables
from db.insert_operations import Db_Insert_Params, insert_country_metadata, insert_indicator_metadata, insert_education_stats

def initialize_db():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, "world_education_data_analysis.db")

    # Load CSV files into DataFrames
    country_df, country_df_columns = read_country_metadata()
    indicator_df, indicator_df_columns = read_indicator_metadata()
    education_stats_df, education_stats_df_columns = read_education_stats()

    # Connect to database and load data
    with sqlite3.connect(db_path) as conn:
        create_tables(conn)

        insert_country_metadata(Db_Insert_Params(
          conn, 
          df=country_df, 
          df_columns=country_df_columns
        ))
        insert_indicator_metadata(Db_Insert_Params(
          conn, 
          df=indicator_df, 
          df_columns=indicator_df_columns
        ))
        insert_education_stats(Db_Insert_Params(
          conn, 
          education_stats_df, 
          df_columns=education_stats_df_columns
        ))

