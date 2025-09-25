import duckdb
import os
import logging
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

TYPES = ['yellow', 'green']
YEARS = range(2015, 2024)
MONTHS = [f"{m:02d}" for m in range(1, 13)]
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
SAVE_DIR = "data"
os.makedirs(SAVE_DIR, exist_ok=True) #Save the data in data folder

def download_all_parquet_files():
    for taxi_type in TYPES:
        for year in YEARS:
            for month in MONTHS:
                filename = f"{taxi_type}_tripdata_{year}-{month}.parquet"
                url = f"{BASE_URL}/{filename}"
                local_path = os.path.join(SAVE_DIR, filename)

                if os.path.exists(local_path):
                    logger.info(f"Skipped (already exists): {filename}")
                    continue

                try:
                    logger.info(f"Downloading: {url}")
                    response = requests.get(url, stream=True)

                    if response.status_code == 200:
                        with open(local_path, "wb") as f:
                            f.write(response.content)
                        logger.info(f"Downloaded: {filename}")
                    else:
                        logger.warning(f"Failed: {filename} - HTTP {response.status_code}")

                except Exception as e:
                    logger.error(f"Error downloading {filename}: {e}")

if __name__ == "__main__":
    logger.info("Starting full TLC taxi dataset download (2015–2024)...")
    download_all_parquet_files()
    logger.info("Download completed.")
