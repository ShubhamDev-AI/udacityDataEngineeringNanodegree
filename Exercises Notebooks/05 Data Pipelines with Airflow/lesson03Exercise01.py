
import datetime
import logging
import sql_statements

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

# import multiple operators at once
from airflow.operators import (
     HasRowsOperator
    ,S3ToRedshiftOperator
    ,PostgresOperator
    ,PythonOperator
)

"""----------------------------------------------------------------------------
    Lesson 3 Exercise 1
    This lesson takes Lesson 2 Exercise 4 as a starting point and enhances it
by generalizing repetitive code/Tasks through the addition of custom Airflow 
Plugins.

    OBJECTIVES
        - demonstrate the process to create custom Airflow plugins
        - generalize repetitive code and Task by leveraging custom Operators
----------------------------------------------------------------------------"""

bulkLoadDag = DAG(
     dag_id='lesson3Exercise1'
    ,start_date=datetime.datetime(2018,1,1,0,0,0,0)
    # set a date beyond which this DAG won't run
    ,end_date=datetime.datetime(2018,2,1,0,0,0,0)
    ,schedule_interval='@monthly'
    ,max_active_runs=1
)

# create Tasks by instantiating Operator Classes
createTripsTable = PostgresOperator(
     task_id='createTripsTable'
    ,postgres_conn_id='redshift'
    ,sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    ,dag=bulkLoadDag
)

# IMPORTANT:
# As "s3_key" parameter has been marked as "Templatable" (in its Class
# "template_fields" variable) it'll be able to fetch JINJA template 
# variables like {{ds}}, {{execution_date}} and so on.
loadTripsData = S3ToRedshiftOperator(
     task_id="loadTripsData"
    ,redshift_conn_id="redshift"
    ,aws_credentials_id="aws_credentials"
    ,table="trips"
    ,truncate=False
    ,s3_bucket="udacity-dend"
    ,s3_key="data-pipelines/divvy/partitioned/{execution_date.year}/{execution_date.month}/divvy_trips.csv"
    ,delimiter=","
    ,ignore_headers=1
    ,dag=bulkLoadDag
)

checkTripsData = HasRowsOperator(
     task_id='checkTripsData'
    ,redshift_conn_id='redshift'
    ,table='trips'
    ,dag=bulkLoadDag
)

createStationsTable = PostgresOperator(
     task_id="createStationsTable"
    ,postgres_conn_id="redshift"
    ,sql=sql_statements.CREATE_STATIONS_TABLE_SQL
    ,dag=bulkLoadDag
)

# REMEMBER:
# As "s3_key" parameter has been marked as "Templatable" (in its Class
# "template_fields" variable) it'll be able to fetch JINJA template 
# variables like {{ds}}, {{execution_date}} and so on.
loadStationsTable = S3ToRedshiftOperator(
     task_id="loadStationsTable"
    ,redshift_conn_id="redshift"
    ,aws_credentials_id="aws_credentials"
    ,table="stations"
    ,truncate=True
    ,s3_bucket="udacity-dend"
    ,s3_key="data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv"
    ,delimiter=","
    ,ignore_headers=1
)

checkStationsData = HasRowsOperator(
     task_id="checkStationsData"
    ,redshift_conn_id="redshift"
    ,table="stations"
    ,dag=bulkLoadDag
)

createTripsTable >> loadTripsData >> checkTripsData
createStationsTable >> loadStationsTable >> checkStationsData