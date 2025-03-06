from sqlite3 import Connection

def create_tables(conn: Connection):
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
            country_code TEXT,
            indicator_code TEXT,
            year INTEGER,
            value REAL,
            FOREIGN KEY (country_code) REFERENCES country(country_code),
            FOREIGN KEY (indicator_code) REFERENCES indicator(indicator_code),
            PRIMARY KEY (country_code, indicator_code, year)
        )
    """)

    cursor.execute("""CREATE INDEX IF NOT EXISTS idx_edu_country_code ON education_stats(country_code);""")
    cursor.execute("""CREATE INDEX IF NOT EXISTS idx_edu_indicator_code ON education_stats(indicator_code);""")     
    cursor.execute("""CREATE INDEX IF NOT EXISTS idx_edu_country_year ON education_stats(country_code, year);""")
    cursor.execute("""CREATE INDEX IF NOT EXISTS idx_edu_country_year ON education_stats(country_code, year);""")         

