import boto3
import re
import time
import botocore
import sys
from func_timeout import func_timeout, FunctionTimedOut

import yaml

from config.log_config import LOG_CONFIG
from os.path import expanduser
import boto3
import configparser
import csv
import logging.config
import logging

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger("ORE_ATHENA")

with open('../config/athena_config.yml', 'r') as f:
    args = yaml.load(f, Loader=yaml.SafeLoader)

# logger.info the values as a dictionary
logger.info(args["athena"]["tableName"])

params = {
    'region': args["aws"]['region'],
    's3Bucket': args["aws"]['s3Bucket'],
    's3Folder': args["aws"]['s3Folder'] + '/',
    'database': args["athena"]['database'],
    'tableName': args["athena"]['tableName'],
    'athenaResultBucket': args["athena"]['athenaResultBucket'],
    'athenaResultFolder': args["athena"]['athenaResultFolder'],
    'timeout': int(args["athena"]['timeout'])  # in sec
}
logger.info("Parameters : ")
logger.info(params)
logger.info("----------------------------------")



def get_config(config_file_path):
    logger.info("Get the configuration.")
    config = configparser.RawConfigParser()

    credentials_file = config_file_path
    home = expanduser("~")
    credentials_file = home + credentials_file
    config.read(credentials_file)

    return config


config = get_config('/.aws/credentials')

profile = "saml-prd"

AWS_ACCESS_KEY = config.get(profile, 'aws_access_key_id')
AWS_SECRET_KEY = config.get(profile, 'aws_secret_access_key')
AWS_SESSION_TOKEN = config.get(profile, 'aws_session_token')
AWS_REGION = config.get(profile, 'region')

s3Client = boto3.client('s3',
                        aws_access_key_id=AWS_ACCESS_KEY,
                        aws_secret_access_key=AWS_SECRET_KEY,
                        aws_session_token=AWS_SESSION_TOKEN,
                        region_name=AWS_REGION)
s3Resource = boto3.resource('s3',
                            aws_access_key_id=AWS_ACCESS_KEY,
                            aws_secret_access_key=AWS_SECRET_KEY,
                            aws_session_token=AWS_SESSION_TOKEN,
                            region_name=AWS_REGION)
athenaClient = boto3.client('athena',
                            aws_access_key_id=AWS_ACCESS_KEY,
                            aws_secret_access_key=AWS_SECRET_KEY,
                            aws_session_token=AWS_SESSION_TOKEN,
                            region_name=AWS_REGION)


def s3CheckIfBucketExists(s3Resource, bucketName):
    try:
        s3Resource.meta.client.head_bucket(Bucket=bucketName)
        logger.info("Athena Bucket exists")
        logger.info("----------------------------------")
        logger.info()
    except botocore.exceptions.ClientError as e:
        logger.info("Athena Bucket does not exist.")
        logger.info(e)
        logger.info("----------------------------------")
        location = {'LocationConstraint': params['region']}
        s3Client.create_bucket(Bucket=params['s3Bucket'], CreateBucketConfiguration=location)
        logger.info()
        logger.info("Athena Bucket Created Successfully.")
        logger.info()


def athena_query(athenaClient, queryString):
    response = athenaClient.start_query_execution(
        QueryString=queryString,
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['athenaResultBucket'] + '/' + params['athenaResultFolder'] + '/'
        }
    )
    return response


def athena_to_s3(athenaClient, params):
    queryString = "SHOW PARTITIONS " + params["tableName"]
    logger.info("Show Partition Query : ")
    logger.info(queryString)
    logger.info("----------------------------------")
    logger.info()
    execution = athena_query(athenaClient, queryString)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'
    while (state in ['RUNNING', 'QUEUED']):
        response = athenaClient.get_query_execution(QueryExecutionId=execution_id)
        if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in \
                response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                logger.info(response)
                logger.info("state == FAILED")
                return False
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                filename = re.findall('.*\/(.*)', s3_path)[0]
                return filename
        time.sleep(1)
    return False


def s3ListObject(s3, prefix):
    resultList = []
    result = s3.list_objects_v2(
        Bucket=params['s3Bucket'],
        Delimiter='/',
        Prefix=prefix
    )
    if result['KeyCount'] == 0:
        return False
    try:
        resultList.extend(result.get('CommonPrefixes'))
        while (result['IsTruncated']):
            result = s3.list_objects_v2(
                Bucket=params['s3Bucket'],
                Delimiter='/',
                Prefix=prefix,
                ContinuationToken=result['NextContinuationToken']
            )
            resultList.extend(result.get('CommonPrefixes'))
    except Exception as e:
        logger.info("#~ FAILURE ~#")
        logger.info("Error with :")
        logger.info(result)
        raise

    return resultList


def cleanup(s3Resource, params):
    logger.info('Cleaning Temp Folder Created: ')
    logger.info(params['athenaResultBucket'] + '/' + params["athenaResultFolder"] + '/')
    logger.info()
    s3Resource.Bucket(params['athenaResultBucket']).objects.filter(Prefix=params["athenaResultFolder"]).delete()
    logger.info('Cleaning Completed')
    logger.info("----------------------------------")
    logger.info()
    # s3Resource.Bucket(params['athenaResultBucket']).delete()


def split(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i + n]


# MAIN EXECUTION BEGINS
# Check if Bucket Exists
s3CheckIfBucketExists(s3Resource, params["athenaResultBucket"])

# Fetch Athena result file from S3
try:
    s3_filename = func_timeout(params['timeout'], athena_to_s3, args=(athenaClient, params))
except FunctionTimedOut:
    logger.error("Athena Show Partition query timed out.")
    raise
# s3_filename = athena_to_s3(athenaClient, params)
logger.info("Athena Result File At :")
logger.info(params['athenaResultBucket'] + '/' + params["athenaResultFolder"] + '/' + s3_filename)
logger.info("----------------------------------")
logger.info()

# Read Athena Query Result file and create a list of partitions present in athena meta
fileObj = s3Client.get_object(
    Bucket=params['athenaResultBucket'],
    Key=params['athenaResultFolder'] + '/' + s3_filename
)
fileData = fileObj['Body'].read()
contents = fileData.decode('utf-8')
athenaList = contents.splitlines()
logger.info("Athena Partition List : ")
logger.info(athenaList)
logger.info("----------------------------------")

# Parse S3 folder structure and create partition list
prefix = params['s3Folder']
yearFolders = s3ListObject(s3Client, prefix)
if yearFolders:
    monthList = []
    for year in yearFolders:
        result = s3Client.list_objects_v2(
            Bucket=params['s3Bucket'],
            Delimiter='/',
            Prefix=year.get('Prefix')
        )
        try:
            monthList.extend(result.get('CommonPrefixes'))
        except Exception as e:
            logger.info("#~ FAILURE ~#")
            logger.info("Error with :")
            logger.info(result)
            raise

    s3List = []
    for thingType in monthList:
        string = thingType.get('Prefix').replace(params['s3Folder'], "")
        s3List.append(string.rstrip('/'))

    # To filter out default spark null partitions and folders like  _SUCCESS, _temporary, __HIVE_DEFAULT_PARTITION__
    s3List = [i for i in s3List if
              (('month' in i) and (i.startswith('year')) and not ('__HIVE_DEFAULT_PARTITION__' in i))]

    logger.info("S3 Folder Structure At :")
    logger.info(params['s3Bucket'] + '/' + params['s3Folder'])
    logger.info("----------------------------------")
    logger.info()
    logger.info("S3 Partition List : ")
    logger.info(s3List)
    logger.info("----------------------------------")
    logger.info()

    # Compare Athena Partition List with S3 Partition List
    resultSet = set(s3List) - set(athenaList)
    logger.info("Result Set : ")
    logger.info(resultSet)
    logger.info("----------------------------------")
    logger.info()

    # Create Alter Query for Athena
    try:
        if len(resultSet) != 0:
            logger.info("Partition Count : " + str(len(resultSet)))
            result = split(list(resultSet), 1000)
            for resultSet in result:
                queryString = "ALTER TABLE " + params['tableName'] + " ADD IF NOT EXISTS PARTITION(" + repr(
                    resultSet) + ")"
                queryString = queryString.replace("[", "")
                queryString = queryString.replace("]", "")
                queryString = queryString.replace("{", "")
                queryString = queryString.replace("}", "")
                queryString = queryString.replace(",", ") PARTITION(")
                queryString = queryString.replace("'", "")
                queryString = queryString.replace("date=", "date='")
                queryString = queryString.replace("/", "', ")
                logger.info("Alter Query String : ")
                logger.info(queryString)
                logger.info("----------------------------------")
                logger.info()

                # Run Alter Partition Query
                execution = athena_query(athenaClient, queryString)
                if execution['ResponseMetadata']['HTTPStatusCode'] == 200:
                    # Temp Folder Cleanup
                    cleanup(s3Resource, params)
                    logger.info("*~ SUCCESS ~*")
                    logger.info()
                else:
                    logger.info("#~ FAILURE ~#")
                    logger.info()

        else:
            # Temp Folder Cleanup
            cleanup(s3Resource, params)
            logger.info()
            logger.info("*~ SUCCESS ~*")

    except Exception as e:
        # Temp Folder Cleanup
        cleanup(s3Resource, params)
        logger.info("#~ FAILURE ~#")
        logger.info("Error with :")
        logger.info(resultSet)
        logger.info(e)
        raise
else:
    # Temp Folder Cleanup
    cleanup(s3Resource, params)
    logger.info("S3 Folder does not exist.")
    logger.info("----------------------------------")
    logger.info()
    logger.info("#~ FAILURE ~#")
