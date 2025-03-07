from db.database import get_database_connection, initialize_db
from db.services.indicator_service import IndicatorDbService
from db.services.country_service import CountryDbService


def main():
    # initialize_db()
    conn = get_database_connection()

    IndicatorDbService().get_indicator_mappings(conn)
    CountryDbService().get_country_mappings(conn)
    
if __name__ == "__main__":
    main()
