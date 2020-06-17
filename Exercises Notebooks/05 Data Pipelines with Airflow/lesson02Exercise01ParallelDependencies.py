import datetime
import sql_statements
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

"""------------------------------------------------------------------------
    Lesson 2 Exercise 1
    OBJECTIVES
    - demonstrate how to set both parallel and linear dependencies between
tasks;
    - demonstrate how repetitive some coding patterns may become to start
creating awareness on generalization through python classes;
------------------------------------------------------------------------"""

bulkLoadDag = DAG(
     dag_id='lesson2Exercise1ParallelDependencies'
    ,start_date=datetime.datetime(2020,6,14)
    ,schedule_interval='@once'
    ,max_active_runs=1
    ,catchup=False
)

#   IMPORTANT: notice the two python functions below are EXACTLY the
# same, except for the SQL statements each functions operates upon.

def loadTripDataFromS3ToRedshift():

    # instantiate AwsHook Class
    awsHookInstance = AwsHook("aws_credentials")
    # retrieve credentials with "get_credentials()" method
    AWSCredentials = awsHookInstance.get_credentials()
    # instantiate PostgresHook Class
    derivedRedshiftHook = PostgresHook("redshift")
    # retrieve Trip data bulk load SQL statement
    bulkLoadStatement = sql_statements.COPY_ALL_TRIPS_SQL.format(
         AWSCredentials.access_key
        ,AWSCredentials.secret_key
    )
    # execute SQL statement
    derivedRedshiftHook.run(bulkLoadStatement)

def loadStationDataFromS3ToRedshift():

    # instantiate AwsHook Class
    awsHookInstance = AwsHook("aws_credentials")
    # retrieve credentials with "get_credentials()" method
    AWSCredentials = awsHookInstance.get_credentials()
    # instantiate PostgresHook Class
    derivedRedshiftHook = PostgresHook("redshift")
    # retrieve Station data bulk load SQL statement
    bulkLoadStatement = sql_statements.COPY_STATIONS_SQL.format(
         AWSCredentials.access_key
        ,AWSCredentials.secret_key
    )
    # execute SQL statement
    derivedRedshiftHook.run(bulkLoadStatement)

# create Tasks by instantiating Operator Classes
createTripsTable = PostgresOperator(
     task_id='createTripsTable'
    ,postgres_conn_id='redshift'
    ,sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    ,dag=bulkLoadDag
)

createStationsTable = PostgresOperator(
     task_id='createStationsTable'
    ,postgres_conn_id='redshift'
    ,sql=sql_statements.CREATE_STATIONS_TABLE_SQL
    ,dag=bulkLoadDag
)

loadTripsData = PythonOperator(
     task_id='loadTripsData'
    ,python_callable=loadTripDataFromS3ToRedshift
    ,dag=bulkLoadDag
)

loadStationsData = PythonOperator(
     task_id='loadStationsData'
    ,python_callable=loadStationDataFromS3ToRedshift
    ,dag=bulkLoadDag
)

# define PARALLEL Task dependencies
createTripsTable >> loadTripsData
createStationsTable >> loadStationsData