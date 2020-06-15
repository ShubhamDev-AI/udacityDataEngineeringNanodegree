
import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

"""------------------------------------------------------------------------
    Lesson 1 Exercise 4 objectives:
    - demonstrate how to set and use Airflow Variables (both programmatically and
via UI);
    - demonstrate how to use the S3Hook Class
------------------------------------------------------------------------"""

listKeysDag = DAG(
     dag_id='lesson1Exercise4'
    ,start_date=datetime.datetime(2020,6,13)
    ,schedule_interval='@once'
    ,max_active_runs=1
)

def listS3BucketKeys():

    # set necessary Airflow Variables and store them in metastore DB
    Variable.set("s3_bucket","udacity-dend")
    Variable.set("s3_prefix","data-pipelines")

    # instantiate S3Hook Class
    # Airflow's S3Hook Docs: https://bit.ly/2B2tHN7
    sampleHook = S3Hook(aws_conn_id='aws_credentials')

    # retrieve Variable values from metastore
    s3_bucket=Variable.get("s3_bucket")
    s3_prefix=Variable.get("s3_prefix")

    # print message
    logging.info(f'Listing Keys from S3 Bucket: {s3_bucket}/{s3_prefix}')

    # use S3Hook's "list_keys()" method return a List Object of bucket keys
    s3KeyList = sampleHook.list_keys(
         s3_bucket
        ,prefix=s3_prefix
    )

    # iterate on "keys" object and print each item
    for key in s3KeyList:
        logging.info(f"- S3://{s3_bucket}/{key}")

# create a Task by instantiating the PythonOperator
listS3KeysTask = PythonOperator(
     task_id='listS3KeysTask'
    ,python_callable=listS3BucketKeys
    ,dag=listKeysDag
)



