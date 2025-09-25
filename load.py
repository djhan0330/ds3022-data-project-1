import duckdb
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="load.log"
)
logger = logging.getLogger(__name__)

DB_FILE = "nyc_taxi.duckdb"
BUCKET = "nyc-taxi-2015-2024-djhan"
REGION = "us-east-1"

def load_taxi_data(con, taxi_type):
    table_name = f"{taxi_type}_2024"
    created = False

    for month in range(1, 13):
        path = f"s3://{BUCKET}/raw/{taxi_type}_tripdata_2024-{month:02d}.parquet"

        if not created:
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_parquet('{path}', union_by_name=true)
            """)
            created = True
            logger.info(f"Created {table_name} with {path}")
        else:
            con.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM read_parquet('{path}', union_by_name=true)
            """)
            logger.info(f"Inserted {path} into {table_name}")

    # Row count
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"{table_name}: {count:,} rows")
    logger.info(f"Final row count for {table_name}: {count:,}")

    #Final row count for yellow_2024: 41,169,720
    #Final row count for green_2024: 660,218


def load_emissions(con):
    path = "data/vehicle_emissions.csv"  # keep this file locally
    con.execute(f"""
        CREATE OR REPLACE TABLE vehicle_emissions AS
        SELECT * FROM read_csv_auto('{path}', HEADER=TRUE)
    """)
    count = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
    print(f"vehicle_emissions: {count:,} rows")
    logger.info(f"Loaded {count} rows into vehicle_emissions")
    #Loaded 8 rows into vehicle_emissions

def main():
    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    con.execute("SET s3_region='us-east-1';")
    con.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';")
    con.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';")

    load_taxi_data(con, "yellow")
    load_taxi_data(con, "green")
    load_emissions(con)

    con.close()


if __name__ == "__main__":
    logger.info("Starting load.py")
    main()
    logger.info("Completed load.py")
