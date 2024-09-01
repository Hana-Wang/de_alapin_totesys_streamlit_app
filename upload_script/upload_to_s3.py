import boto3
import os
import logging
from datetime import datetime as dt
from dotenv import load_dotenv



logger = logging.getLogger('s3_uploader')
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

logger.addHandler(ch)

load_dotenv()

BUCKET_NAME = os.environ.get("DATA_BUCKET_NAME")



def upload_files_to_s3(bucket_name, files_to_upload, s3_folder=""):
    """
    Uploads a list of files to an S3 bucket.

    Parameters:
    - bucket_name: str: The name of the S3 bucket
    - files_to_upload: list: List of file paths to upload
    - s3_folder: str: The S3 folder where files should be uploaded. Optional.
    """

    s3_client = boto3.client('s3')

    for file_path in files_to_upload:
        file_name = os.path.basename(file_path)
        timestamp = dt.now().strftime("%Y/%m/%d")
        s3_key = f"{s3_folder}/{timestamp}/{file_name}" if s3_folder else file_name

        try:
            s3_client.upload_file(file_path, bucket_name, s3_key)
            logger.info(f"File {file_name} uploaded to {bucket_name}/{s3_key}")
        except Exception as e:
            logger.error(f"Failed to upload {file_name}: {e}")


def lambda_handler(event, context):
    """
    AWS Lambda handler function.

    Parameters:
    - event: dict: The event data passed by EventBridge
    - context: object: The context in which the function is called
    """

    try:
        bucket_name = BUCKET_NAME

        tables = ['fact_sales_order', 'dim_staff', 'dim_location', 'dim_design', 'dim_date', 'dim_currency', 'dim_counterparty']

        files_to_upload = [f"db/parquet_files/tmp/{table}.parquet" for table in tables]

        s3_folder = "db/parquet_files"

        upload_files_to_s3(bucket_name, files_to_upload, s3_folder)

        return {
            'statusCode': 200,
            'body':'Files uploaded successfully'
            
        }
    except Exception as e:
        logger.error(f"An error occured: {e}")

        return {
            'statusCode': 500,
            'body': f"An error occured: {e}"
        }

if __name__ == "__main__":
    event = {}
    context = None

    response = lambda_handler(event, context)
    logger.info(f"Lambda response: {response}")

