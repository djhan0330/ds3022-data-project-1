import duckdb
import logging

#Set up Logging
logging.basicConfig(
    level = logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="clean.log"
)
logger = logging.getLogger(__name__)

DB_FILE = "nyc_taxi.duckdb"

def clean_table(con, table_name):
    clean_table_name = f"{table_name.split('_')[0]}_clean"
    
    logger.info(f"Cleaning {table_name} -> {clean_table_name}")

    #The pickup/dropoff for yellow is tpep while green is lpep
    if "yellow" in table_name:
        pickup_col = "tpep_pickup_datetime"
        dropoff_col = "tpep_dropoff_datetime"
    elif "green" in table_name:
        pickup_col = "lpep_pickup_datetime"
        dropoff_col = "lpep_dropoff_datetime"
    else:
        raise ValueError(f"Unknown taxi type in {table_name}")

    con.execute(f"""
                CREATE OR REPLACE TABLE {clean_table_name} AS
                SELECT * FROM (
                    SELECT DISTINCT * 
                    FROM {table_name})
                WHERE passenger_count > 0
                    AND trip_distance > 0 
                    AND trip_distance <= 100
                    AND date_diff('hour', {pickup_col}, {dropoff_col}) <= 24
                    """)
    
    count = con.execute(f"SELECT COUNT(*) FROM {clean_table_name}").fetchone()[0]
    print(f"{clean_table_name}: {count:, } rows")
    logger.info(f"Finished Cleaning {table_name}. Rows kept{count:,}")

    tests = {
        "zero_passengers": f"SELECT COUNT(*) FROM {clean_table_name} WHERE passenger_count = 0",
        "zero_distance": f"SELECT COUNT(*) FROM {clean_table_name} WHERE trip_distance = 0",
        "long_distance": f"SELECT COUNT(*) FROM {clean_table_name} WHERE trip_distance > 100",
        "long_duration": f"""SELECT COUNT(*) 
                            FROM {clean_table_name}
                            WHERE date_diff('hour', {pickup_col}, {dropoff_col}) > 24
        """
    }

    for name, sql in tests.items():
        result = con.execute(sql).fetchone()[0]
        print(f"Test {name}: {result} violations")
        logger.info(f"Test {name}: {result} violations")

def main():
    con = duckdb.connect("nyc_taxi.duckdb")
    clean_table(con, "yellow_all")
    clean_table(con, "green_all")

    con.close()

if __name__ == "__main__":
    logger.info("Starting clean.py")
    main()
    logger.info("Completed clean.py")