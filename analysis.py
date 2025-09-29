import duckdb
import logging
import matplotlib.pyplot as plt
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="analysis.log"
)
logger = logging.getLogger(__name__)

os.makedirs("outputs", exist_ok=True)

DB_FILE = "nyc_taxi.duckdb"

def analysis():
    con = duckdb.connect(DB_FILE)

    queries = {
    "largest_trip": """
        SELECT 'YELLOW' AS cab_type, MAX(trip_co2_kgs) AS max_co2
        FROM main_main.yellow_transform
        UNION ALL
        SELECT 'GREEN' AS cab_type, MAX(trip_co2_kgs)
        FROM main_main.green_transform;
    """,
    "heavy_light_hour": """
        SELECT cab_type, hour_of_day, AVG(trip_co2_kgs) AS avg_co2
        FROM (
            SELECT 'YELLOW' AS cab_type, hour_of_day, trip_co2_kgs
            FROM main_main.yellow_transform
            UNION ALL
            SELECT 'GREEN' AS cab_type, hour_of_day, trip_co2_kgs
            FROM main_main.green_transform
        )
        GROUP BY cab_type, hour_of_day
        ORDER BY cab_type, avg_co2;
    """,
    "heavy_light_day": """
        SELECT cab_type, day_of_week, AVG(trip_co2_kgs) AS avg_co2
        FROM (
            SELECT 'YELLOW' AS cab_type, day_of_week, trip_co2_kgs
            FROM main_main.yellow_transform
            UNION ALL
            SELECT 'GREEN' AS cab_type, day_of_week, trip_co2_kgs
            FROM main_main.green_transform
        )
        GROUP BY cab_type, day_of_week
        ORDER BY cab_type, avg_co2;
    """,
    "heavy_light_week": """
        SELECT cab_type, week_of_year, AVG(trip_co2_kgs) AS avg_co2
        FROM (
            SELECT 'YELLOW' AS cab_type, week_of_year, trip_co2_kgs
            FROM main_main.yellow_transform
            UNION ALL
            SELECT 'GREEN' AS cab_type, week_of_year, trip_co2_kgs
            FROM main_main.green_transform
        )
        GROUP BY cab_type, week_of_year
        ORDER BY cab_type, avg_co2;
    """,
    "heavy_light_month": """
        SELECT cab_type, month_of_year, AVG(trip_co2_kgs) AS avg_co2
        FROM (
            SELECT 'YELLOW' AS cab_type, month_of_year, trip_co2_kgs
            FROM main_main.yellow_transform
            UNION ALL
            SELECT 'GREEN' AS cab_type, month_of_year, trip_co2_kgs
            FROM main_main.green_transform
        )
        GROUP BY cab_type, month_of_year
        ORDER BY cab_type, avg_co2;
    """,
    "monthly_totals": """
        SELECT 'YELLOW' AS cab_type, month_of_year, SUM(trip_co2_kgs) AS total_co2
        FROM main_main.yellow_transform
        GROUP BY month_of_year
        UNION ALL
        SELECT 'GREEN' AS cab_type, month_of_year, SUM(trip_co2_kgs)
        FROM main_main.green_transform
        GROUP BY month_of_year
        ORDER BY cab_type, month_of_year;
    """
}


    #Running the query
    for name, sql in queries.items():
        results_df = con.execute(sql).fetchdf()
        print(f"\n---{name.upper()}---")
        print(results_df)
        logger.info(f"\n---{name.upper()}---\n{results_df.to_string(index=False)}")

    #Plotting monthly CO2 total
    df = con.execute(queries["monthly_totals"]).fetchdf()

    yellow = df[df["cab_type"] == "YELLOW"]
    green = df[df["cab_type"] == "GREEN"]

    plt.figure(figsize=(10, 6))
    plt.plot(yellow["month_of_year"], yellow["total_co2"], marker='o', label="Yellow Taxi")
    plt.plot(green["month_of_year"], green["total_co2"], marker='o', label="Green Taxi")
    plt.xlabel("Month")
    plt.ylabel("Total CO2 (kg)")
    plt.title("Monthly CO2 Emissions by Taxi Type")
    plt.legend()
    plt.grid(True)
    plt.savefig("outputs/co2_trends.png")
    plt.close()

    con.close()

if __name__ == "__main__":
    analysis()

