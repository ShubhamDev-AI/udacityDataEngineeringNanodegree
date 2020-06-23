import datetime
import sql_statements
from airflow import DAG
from airflow.operators import (
     PostgresOperator
    ,S3ToRedshiftOperator
    ,HasRowsOperator
    ,FactsCalculatorOperator
)

customDag = DAG(
     dag_id='lesson3Exercise4'
    ,start_date=datetime.datetime(2020,6,21)
    ,max_active_runs=1
)

# create trips table
createTripsTable = PostgresOperator(
     task_id='createTripsTable'
    ,postgres_conn_id='redshift'
    ,sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    ,dag=customDag
)

# load data from S3 to Resdhift
loadTripData = S3ToRedshiftOperator(
     task_id='loadTripData'
    ,redshift_conn_id='redshift'
    ,aws_credentials_id='aws_credentials'
    ,table='trips'
    ,truncate=False
    ,s3_bucket='udacity-dend'
    ,s3_key='data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv'
    ,delimiter=','
    ,ignore_headers=1
    ,dag=customDag
)

# check data quality
checkDataQuality = HasRowsOperator(
     task_id='checkDataQuality'
    ,redshift_conn_id='redshift'
    ,table='trips'
    ,dag=customDag
)

# create fact table
createFactTable = FactsCalculatorOperator(
     task_id='createFactTable'
    ,redshift_conn_id='redshift'
    ,origin_table='trips'
    ,destination_table='trip_facts'
    ,fact_column='tripduration'
    ,group_by_column='bikeid'
)

# set task dependencies
createTripsTable >> loadTripData >> checkDataQuality >> createFactTable