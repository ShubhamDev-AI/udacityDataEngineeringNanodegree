
import datetime
import logging
import sql_statements
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

"""----------------------------------------------------------------------------
    Lesson 2 Exercise 4

    This lesson builds upon the previous one, Lesson 2 Exercise 3.

    OBJECTIVES

        - use BaseOperator's "params" argument to pass custom context variables
    to Tasks;
        - ensure data quality by implementing a simple function to check whether
    data has been loaded by the pipeline;
----------------------------------------------------------------------------"""

bulkLoadDag = DAG(
     dag_id='lesson2Exercise4'
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

#--------------------------------------------------------------------------
#                                   NEW
#--------------------------------------------------------------------------
def checkGreaterThanZero(*args,**context):

    # retrieve table name from context variables
    targetTable = context["params"]["targetTable"]

    # instantiate PostgresHook Class
    derivedRedshiftHook = PostgresHook("redshift")

    # Use BaseHook's inherited "get_records()" method to fetch DB data
    # BaseHook Docs --> https://bit.ly/3fvknjH
    # BEWARE "get_records" returns a List object
    tableRecordsList = derivedRedshiftHook.get_records(f"SELECT COUNT(*) FROM {targetTable}")

    # output "get_records()" resulting object type just to make sure
    logging.info('"tableRecordsList" object is of type ',type(tableRecordsList))

    # check List object length first
    if len(tableRecordsList) < 1 or len(tableRecordsList[0]) < 1:
        raise ValueError(f'Data Quality check failed. {targetTable} returned no results.')
    # retrieve List object contents if first check has passed
    tableRecordsContent = tableRecordsList[0][0]
    
    # check whether table returns at least 1 record/entry
    if tableRecordsContent < 1:
        raise ValueError(f'Data Quality check failed. {targetTable} contains zero rows.')

    # if both tests are passed, print success message
    logging.info(f'Data Quality check passed: {targetTable} returned {tableRecordsContent} entries.')


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

#--------------------------------------------------------------------------
#                                   NEW
#--------------------------------------------------------------------------
checkTripsTable = PythonOperator(
     task_id='checkTripsTable'
    ,python_callable=checkGreaterThanZero
    ,provide_context=True
    # The "params" dictionary is inherited from Airflow's BaseOperator
    # Docs at https://bit.ly/2Y9wIUZ
    ,params={
        'targetTable':'trips'
    }
    ,dag=bulkLoadDag
)

checkStationsTable = PythonOperator(
     task_id='checkStationsTable'
    ,python_callable=checkGreaterThanZero
    ,provide_context=True
    ,params={
        'targetTable':'stations'
    }
    ,dag=bulkLoadDag
)

# define PARALLEL Task dependencies
createTripsTable >> loadTripsData >> checkTripsTable
createStationsTable >> loadStationsData >> checkStationsTable


