import mysql.connector
from mysql.connector import MySQLConnection, Error
from csv_loader.csv_loader import read_country_metadata, read_education_stats, read_indicator_metadata
from db.services.country_service import CountryDbService
from db.services.indicator_service import IndicatorDbService
from db.services.education_service import EducationDbService
from db.models.db_insert_params import Db_Insert_Params

def get_database_connection() -> MySQLConnection:
    connection = mysql.connector.connect(
            host="localhost",
            user="zaul_tavangar",
            password="admin",
            database="world_education_data_analysis"
        )
    if connection.is_connected():
      print("Connected to MySQL Server")

    return connection

def create_database(conn: MySQLConnection):
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS world_education_data_analysis")

def create_tables(conn: MySQLConnection):
    cursor = conn.cursor()
    
    # Table creation logic (as in your existing code)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS country (
            country_code VARCHAR(3) PRIMARY KEY,
            country_name VARCHAR(256),
            region VARCHAR(256),
            income_group VARCHAR(256),
            special_notes TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS indicator (
            indicator_code VARCHAR(256) PRIMARY KEY,
            indicator_name TEXT,
            source_note TEXT,
            source_organization TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS education_stats (
            country_code VARCHAR(3),
            indicator_code VARCHAR(256),
            year INTEGER,
            value REAL,
            FOREIGN KEY (country_code) REFERENCES country(country_code),
            FOREIGN KEY (indicator_code) REFERENCES indicator(indicator_code),
            PRIMARY KEY (country_code, indicator_code, year)
        )
    """)

    create_indexes(conn)

def create_indexes(conn):
    cursor = conn.cursor()
    
    def create_index_safely(table, index_name, columns):
        try:
            cursor.execute(f"""
                CREATE INDEX {index_name} ON {table}({columns});
            """)
        except:
            # Index might already exist, ignore the error
            pass
    
    # Create indexes for education_stats table
    create_index_safely("education_stats", "idx_edu_country_code", "country_code")
    create_index_safely("education_stats", "idx_edu_indicator_code", "indicator_code")
    create_index_safely("education_stats", "idx_edu_country_year", "country_code, year")
    create_index_safely("education_stats", "idx_edu_indicator_year", "indicator_code, year")
    
    conn.commit()

def initialize_db():
    try:
        connection = get_database_connection()

        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS world_education_data_analysis")

         # Load CSV files into DataFrames
        country_df, country_df_columns = read_country_metadata()
        indicator_df, indicator_df_columns = read_indicator_metadata()
        education_stats_df, education_stats_df_columns = read_education_stats()

        create_tables(connection)

        country_service = CountryDbService()
        indicator_service = IndicatorDbService()
        education_service = EducationDbService()


        country_service.insert_country_metadata(Db_Insert_Params(
                connection,
                df=country_df,
                df_columns=country_df_columns
        ))
        indicator_service.insert_indicator_metadata(Db_Insert_Params(
            connection,
            df=indicator_df,
            df_columns=indicator_df_columns
        ))
        education_service.insert_education_stats(Db_Insert_Params(
            connection,
            df=education_stats_df,
            df_columns=education_stats_df_columns
        ))


    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            print("Connection closed")

