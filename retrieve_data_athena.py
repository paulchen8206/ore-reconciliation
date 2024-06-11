from config.log_config import LOG_CONFIG
from os.path import expanduser
import boto3
import configparser
import logging.config
import pandas as pd
import time

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger("athena_canonical")

config = configparser.RawConfigParser()

credentials_file = '/.aws/credentials'
home = expanduser("~")
credentials_file = home + credentials_file
config.read(credentials_file)

profile = "saml-prd"

AWS_ACCESS_KEY = config.get(profile, 'aws_access_key_id')
AWS_SECRET_KEY = config.get(profile, 'aws_secret_access_key')
AWS_SESSION_TOKEN = config.get(profile, 'aws_session_token')
AWS_REGION = config.get(profile, 'region')

SCHEMA_NAME = "ore_recon"
S3_STAGING_DIR = "s3://dt-ore-prd/athena_results_qa/Unsaved/"
S3_BUCKET_NAME = "dt-ore-prd"
S3_OUTPUT_DIRECTORY = "athena_results_qa/Unsaved"

LOCAL_FILE = "table_counts_athena.csv"


def download_and_load_query_results(client, query_response):
    while True:
        try:
            client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                raise err
    logger.info(f"Time to complete query: {time.time() - start_time}s")
    temp_file_location: str = LOCAL_FILE
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION,
    )
    s3_client.download_file(
        S3_BUCKET_NAME,
        f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
        temp_file_location,
    )
    return pd.read_csv(temp_file_location)


def getClient():
    logger.info("Created athena client")
    athena_client = boto3.client(
        "athena",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION,
    )

    return athena_client


def getResponse(athena_client):
    logger.info("Created athena connection")

    resp = athena_client.start_query_execution(
        QueryString="SELECT * FROM canonical_table_counts",
        QueryExecutionContext={"Database": SCHEMA_NAME},
        ResultConfiguration={
            "OutputLocation": S3_STAGING_DIR,
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    return resp


if __name__ == "__main__":
    athena_client = getClient()
    start_time = time.time()
    response = getResponse(athena_client)
    df_data = download_and_load_query_results(athena_client, response)
    logger.info(df_data.head())
    logger.info(f"Data fetched in {time.time() - start_time}s")
