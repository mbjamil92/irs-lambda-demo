import pandas as pd
import boto3
import os
from dotenv import load_dotenv
import logging
import sys
import time
import datetime as dt
import io
import pymysql

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

        # Connect to MySQL Database
        connection = pymysql.connect(host=hostname,user=uname,password=pwd,database=dbname)

        cursor = connection.cursor()

        # Truncate the table everytime before an ETL:
        sql_trunc = "TRUNCATE TABLE `irs990`"
        cursor.execute(sql_trunc)

        # commit the results
        connection.commit()

        # creating columns from the dataframe:
        cols = "`,`".join([str(i) for i in df.columns.tolist()])

        # adding dataframe to mysql RDS
        for i,row in df.iterrows():
            sql = "INSERT INTO `irs990` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cursor.execute(sql, tuple(row))
            connection.commit()

        # checking if data was successfully written:
        sql = "SELECT * FROM `irs990`"
        cursor.execute(sql)
        result = cursor.fetchall()
        for i in result:
            print(i)
            
        # closing Mysql connection:
        connection.close()

    except Exception as e:
        logging.error(e)