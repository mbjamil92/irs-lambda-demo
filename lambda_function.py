import pandas as pd
import boto3
import os
from dotenv import load_dotenv
import logging
import sys
import time
import datetime as dt
import io
import mysql.connector


####### LOADING ENVIRONMENT VARIABLES #######
load_dotenv()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

BUCKET = os.getenv('BUCKET')
BUCKET_PREFIX = os.getenv('BUCKET_PREFIX')

# Credentials to database connection
hostname= os.getenv('HOSTNAME')
dbname= os.getenv('DATABASE')
uname= os.getenv('USERNAME')
pwd= os.getenv('PASSWORD')
port = os.getenv('PORT')

def lambda_handler(event, context):
    try:
        logger.info("TEST")
        logger.info(BUCKET)

        s3 = boto3.resource('s3')

        # assigning the bucket:
        my_bucket = s3.Bucket(BUCKET)

        data_list = []

        for my_bucket_object in my_bucket.objects.filter(Prefix=BUCKET_PREFIX):
            if my_bucket_object.key.endswith(".csv"):
                key=my_bucket_object.key
                body=my_bucket_object.get()['Body'].read()
                temp_data = pd.read_csv(io.BytesIO(body))
                data_list.append(temp_data)
        
        # concatenating all the files together:
        df = pd.concat(data_list)

        # Create SQLAlchemy engine to connect to MySQL Database
        engine = mysql.connector.connect(user = uname, password = pwd, host= hostname, database = dbname, port = port);

        # Truncate the table everytime before an ETL:
        engine.execute("TRUNCATE TABLE irs990")

        # Convert dataframe to sql table                                   
        df.to_sql('irs990', engine, if_exists='append',index=False)

        # checking if data was successfully written:
        engine.execute("SELECT * FROM irs990 limit 10").fetchall()

    except Exception as e:
        logging.error(e)