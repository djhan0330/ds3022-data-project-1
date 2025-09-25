import urllib.request
import duckdb
import logging
import boto3
import time
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="load.log"
)
logger = logging.getLogger(__name__)

BUCKET = "nyc-taxi-2015-2024-djhan"
s3 = boto3.client("s3") #No need to use os as it picks up my credentials from ~/.aws/credentials
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TYPES = ["yellow", "green"] 
YEARS = range(2015, 2025)

def s3_upload_files():
    for taxi_type in TYPES:
        for year in YEARS:
            for month in range(1, 13):
                filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
                url = f"{BASE_URL}/{filename}"
                key = f"raw/{filename}" #This is the path inside s3 bucket

                try:
                    # Checking if file already exists in s3
                    try:
                        s3.head_object(Bucket=BUCKET, Key=key)
                        logger.info(f"Skipped {filename} (already in S3)")
                        continue #Skip Upload if found
                    except s3.exceptions.ClientError:
                        pass  #Continue to upload

                    #Download file from the TLC Taxi Data 
                    logger.info(f"Uploading {filename}")
                    r = requests.get(url, stream=True)
                    r.raise_for_status()

                    #Upload file directly to s3
                    s3.upload_fileobj(r.raw, BUCKET, key)
                    logger.info(f"Uploaded {filename}")
                    
                except Exception as e:
                    logger.error(f"Failed {filename}: {e}")

                # Throttle to avoid 403 / timeout
                time.sleep(30)  # wait 30 seconds

if __name__ == "__main__":
    logger.info("Starting upload of NYC Taxi data to S3")
    s3_upload_files()
    logger.info("Upload complete")