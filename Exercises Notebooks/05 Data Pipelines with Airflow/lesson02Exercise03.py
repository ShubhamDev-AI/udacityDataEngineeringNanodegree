import datetime
import logging
import sql_statements
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

"""----------------------------------------------------------------------------
    Lesson 2 Exercise 3

    This lesson builds and expands on Lesson 2 Exercise 2.

    OBJECTIVES
        - practice data backfilling by passing Airflow's context variables to
    Tasks;

    NOTES:
        Access Aiflow's Context Variables through keyword arguments dictionary.
    Example syntax is shown below:
        
        * kwargs["context_variable_name"]
    
    IMPORTANT:
    
        * PythonOperator: for a user defined function to be able to access Context
    Variables, the "provide_context" parameter must be set to "True" within
    the Operator (the actual task) instance this function is gonna be called
    within.
----------------------------------------------------------------------------"""

bulkLoadDag = DAG(
     dag_id='lesson2Exercise3'
    ,start_date=datetime.datetime(2018,1,1,0,0,0,0)
    # set a date beyond which this DAG won't run
    ,end_date=datetime.datetime(2018,2,1,0,0,0,0)
    ,schedule_interval='@monthly'
    ,max_active_runs=1
)

#   IMPORTANT: notice the two python functions below are EXACTLY the
# same, except for the SQL statements each function operates upon.

def loadTripDataFromS3ToRedshift(*args,**context):

    """------------------------------------------------------------------------
        This function loads "trips" data in bulk, according to the current
    "execution_date" the DAG is working upon.
        Notice there are individual CSV files containing the target data
    partitioned by year and month (each CSV holds a single month's data).
        Airflow's "execution_date" variable is used to determine which file
    is gonna be read during each DAG execution cycle.
    ------------------------------------------------------------------------"""

    # instantiate AwsHook Class
    awsHookInstance = AwsHook("aws_credentials")
    # retrieve credentials with "get_credentials()" method
    AWSCredentials = awsHookInstance.get_credentials()
    # instantiate PostgresHook Class
    derivedRedshiftHook = PostgresHook("redshift")

    #--------------------------------------------------------------------------
    #   IMPORTANT: retrieve current "execution_date" from Airflow's context
    # variables. REMEMBER: "execution_date" is actually a datetime object.
    #--------------------------------------------------------------------------
    execution_date=context["execution_date"]
    # output execution_date contents to the Log
    logging.info('"execution_date" = {execution_date}.')
    
    # retrieve Trip data bulk load SQL statement
    bulkLoadStatement = sql_statements.COPY_MONTHLY_TRIPS_SQL.format(
         AWSCredentials.access_key
        ,AWSCredentials.secret_key
        # extract year from "execution_date"
        ,year=execution_date.year
        # extract month from "execution_date"
        ,month=execution_date.month
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
    # IMPORTANT: make sure "provide_context" is set to "True"
    ,provide_context=True
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