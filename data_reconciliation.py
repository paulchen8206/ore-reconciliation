from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from log_config import LOG_CONFIG
from os.path import expanduser
import boto3
import configparser
import csv
import logging.config
import logging
import mysql.connector
import pandas as pd
import smtplib
import time

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger("ORE_RECONCILIATION")

aurora_file_path = "log/table_counts_aurora.csv"
athena_file_path = "log/table_counts_athena.csv"

aurora_query_str = """
            SELECT 'appointment' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT appointment_identifier FROM appointment) a
            UNION SELECT 'article' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT article_number FROM article) b
            UNION SELECT 'employee' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT employee_identifier FROM employee) c
            UNION SELECT 'sales_order' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT sales_order_identifier FROM sales_order) d
            UNION SELECT 'sales_order_receipt' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT sales_order_identifier FROM sales_order_receipt) e
            UNION SELECT 'site' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT site_number FROM site) f
            UNION SELECT 'vehicle' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT vehicle_identifier, trim_identifier, assembly_identifier FROM vehicle) g
            UNION SELECT 'vehicleinspection' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT inspection_identifier FROM vehicle_inspection) h
            UNION SELECT 'workorder' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT work_order_identifier FROM work_order) i;
        """


def get_config(config_file_path):
    logger.info("Get the configuration.")
    config = configparser.RawConfigParser()

    credentials_file = config_file_path
    home = expanduser("~")
    credentials_file = home + credentials_file
    config.read(credentials_file)

    return config


def emailResult():
    fromaddr = "paul.chen@discounttire.com"
    toaddr = "paulchen8206@gmail.com"

    msg = MIMEMultipart()

    msg['From'] = fromaddr
    msg['To'] = toaddr

    msg['Subject'] = "Subject of the Mail"
    body = "Body_of_the_mail"
    msg.attach(MIMEText(body, 'plain'))
    filename = "table_counts_aurora.csv"
    attachment = open(filename, "rb")
    p = MIMEBase('application', 'octet-stream')
    p.set_payload((attachment).read())
    encoders.encode_base64(p)

    p.add_header('Content-Disposition', "attachment; filename= %s" % filename)
    msg.attach(p)
    s = smtplib.SMTP('smtp.outlook.com', 587)
    s.starttls()
    s.login(fromaddr, "password123456789")
    text = msg.as_string()
    s.sendmail(fromaddr, toaddr, text)
    s.quit()


def retrieve_counts_from_athena(file):
    config = get_config('/.aws/credentials')

    profile = "saml-prd"

    AWS_ACCESS_KEY = config.get(profile, 'aws_access_key_id')
    AWS_SECRET_KEY = config.get(profile, 'aws_secret_access_key')
    AWS_SESSION_TOKEN = config.get(profile, 'aws_session_token')
    AWS_REGION = config.get(profile, 'region')

    SCHEMA_NAME = "ore_recon"
    S3_STAGING_DIR = "s3://dt-ore-prd/athena_results_qa/Unsaved/"
    S3_BUCKET_NAME = "dt-ore-prd"
    S3_OUTPUT_DIRECTORY = "athena_results_qa/Unsaved"

    logger.info("Created athena client")
    athena_client = boto3.client(
        "athena",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION,
    )
    start_time = time.time()

    logger.info("Created athena connection")

    resp = athena_client.start_query_execution(
        QueryString="SELECT * FROM canonical_table_counts",
        QueryExecutionContext={"Database": SCHEMA_NAME},
        ResultConfiguration={
            "OutputLocation": S3_STAGING_DIR,
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    while True:
        try:
            athena_client.get_query_results(
                QueryExecutionId=resp["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                raise err
    logger.info(f"Time to complete query: {time.time() - start_time}s")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION,
    )
    s3_client.download_file(
        S3_BUCKET_NAME,
        f"{S3_OUTPUT_DIRECTORY}/{resp['QueryExecutionId']}.csv",
        file,
    )


def retrieve_counts_from_aurora(file, query):
    config = get_config('/.rds/config')

    profile = "prd"

    config = {
        "host": config.get(profile, 'host'),
        "user": config.get(profile, 'user'),
        "password": config.get(profile, 'password'),
        "database": config.get(profile, 'database'),
    }

    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()

    cursor.execute(query)

    with open(file, mode='w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(("table_name", "counts"))

        for i in cursor:
            logger.info(i)
            writer.writerow(i)

    cursor.close()
    cnx.close()


if __name__ == "__main__":
    logger.info("===== Start table counts from Aurora database =====")
    retrieve_counts_from_aurora(aurora_file_path, aurora_query_str)
    logger.info("===== Complete table counts from Aurora database =====")

    logger.info("===== Start table counts from Athena database =====")
    retrieve_counts_from_athena(athena_file_path)
    logger.info("===== Complete table counts from Athena database =====")

    df_aurora = pd.read_csv(aurora_file_path, header='infer').sort_values(by=['table_name'])
    df_athena = pd.read_csv(athena_file_path, header='infer').sort_values(by=['table_name'])

    result = pd.concat([df_aurora, df_athena]).sort_values(by=['table_name'])

    logger.info(result)
