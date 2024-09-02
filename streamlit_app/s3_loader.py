import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
import logging
from dotenv import load_dotenv
from datetime import datetime

import os
import json

logger = logging.getLogger('s3_loader')
logger.setLevel(logging.INFO)

load_dotenv()

BUCKET_NAME = os.environ.get("DATA_BUCKET_NAME")

st.write("BUCKET_NAME:", st.secrets["DATA_BUCKET_NAME"])
st.write("AWS_ACCESS_KEY_ID:", st.secrets["AWS_ACCESS_KEY_ID"])
st.write("AWS_SECRET_ACCESS_KEY:", st.secrets["AWS_SECRET_ACCESS_KEY"])
st.write("AWS_DEFAULT_REGION:", st.secrets["AWS_DEFAULT_REGION"])

def load_data_from_s3(bucket_name, s3_folder="", aws_access_key_id=None,    aws_secret_access_key=None, region_name=None):

    """
    Loads the most recent parquet files from an S3 bucket

    Parameters:
    - bucket_name: str: The name of S3 bucket to read from.
    - s3_folder: str: The S3 folder where the files are stored

    Returns:
    - dict: A dictionary where keys are table names and values are pandas DataFrames
    """

    s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
    )

    data = {}

    tables = ['fact_sales_order', 'dim_staff', 'dim_location', 'dim_design', 'dim_date', 'dim_currency', 'dim_counterparty']

    prefix = f"{s3_folder}/"            
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if not response["Contents"]:
        logger.error(f"No files found in the S3 folder: {s3_folder}")
    else:
        all_keys = [obj['Key'] for obj in response['Contents']]

        for table in tables:
            matching_files = [key for key in all_keys if table in key]

            if not matching_files:
                logger.error(f"No files found for table: {table}")
                continue

            latest_file_key = sorted(matching_files)[-1]
            logger.info(f"Latest file for table {table}: {latest_file_key}")

            obj = s3_client.get_object(Bucket=bucket_name, Key=latest_file_key)
            data[table] = pd.read_parquet(BytesIO(obj['Body'].read()))
            logger.info(f"Loaded data from {latest_file_key}")

    return data 


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()  # Convert datetime to ISO 8601 format
        # For all other types, use the default behavior
        return super().default(o)

def list_s3_objects_and_write_to_file(bucket_name, prefix, output_file=""):
    """
    List objects in an S3 bucket and write the response to a file using a custom JSON encoder
    that specifically handles datetime objects.

    Parameters:
    - bucket_name: str: The name of the S3 bucket.
    - prefix: str: The prefix of the S3 keys to list.
    - output_file: str: The file path where the response will be written.
    """
    s3_client = boto3.client('s3')

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    output_file = "responses_files/list_s3_objects.json"

    with open(output_file, 'w', encoding="utf-8") as wfile:
        json_response = json.dumps(response, cls=CustomJSONEncoder, indent=4)
        wfile.write(json_response)

    return response


def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    """
    bucket_name = BUCKET_NAME
    s3_folder = "db/parquet_files"

    # Load data from S3
    data = load_data_from_s3(bucket_name, s3_folder)

    return {
        'statusCode': 200,
        'body': json.dumps('Data loaded from parquet files in S3 to data frame  successfully')
    }


if __name__ == "__main__":


    # prefix = f"{s3_folder}/"            
    # list_s3_objects_and_write_to_file(bucket_name, prefix)

    event = {}
    context = None

    response = lambda_handler(event, context)
    logger.info(f"Lambda response: {response}")

