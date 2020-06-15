
import datetime
import logging
import sql_statements
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

"""------------------------------------------------------------------------
    Lesson 1 Exercise 6
    OBJECTIVES:
    - demonstrate how to bulk load data from S3 to Redshift
    - 
------------------------------------------------------------------------"""

# instantiate the DAG Class
bulkLoadDag = DAG(
     dag_id='lesson1Exercise6'
    ,start_date=datetime.datetime(2020,6,14)
    ,schedule_interval='@once'
    ,max_active_runs=1
)

def bulkLoadRedshiftWithS3Data():

    """-----------------------------------------------------------------
        This function loads S3 data to a Redshift database instance.    
    
        Both AWS's "Access Key ID" and "Secret Access Key" must be setup
    as "aws_credentials" connection in Airflow's Admin menu for them to
    be available to AwsHook below.
    
        Both Postgres' Hook and Operator are used in Aiflow to manipulate
    Redshift and Postgres database instances.
    -----------------------------------------------------------------"""

    # instantiate AWS Hook with previously UI supplied credentials
    awsHookInstance = AwsHook("aws_credentials")
    # use the "get_credentials()" method to retrieve AWS credentials
    awsCredentials = awsHookInstance.get_credentials()
    # instantiate PostgresHook Class
    derivedRedshiftHook = PostgresHook("redshift")
    #  Bulk Load S3 data by using the derived Redshift Hook created above.
    # Notice how AWS credentials are supplied for S3 access.
    derivedRedshiftHook.run(
        sql_statements.COPY_ALL_TRIPS_SQL.format(
             awsCredentials.access_key
            ,awsCredentials.secret_key
        )
    )

# create Task by instantiating PostgresOperator
createTable = PostgresOperator(
     task_id='createTable'
    ,postgres_conn_id='redshift'
    ,sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    ,dag=bulkLoadDag
)

# use the PythonOperator to bulk load S3 data
copyS3Data = PythonOperator(
     task_id='copyS3Data'
    ,python_callable=bulkLoadRedshiftWithS3Data
    ,dag=bulkLoadDag
)

# use PostgresOperator to execute analytic query
locationTrafficQuery = PostgresOperator(
     task_id='locationTrafficQuery'
    ,postgres_conn_id='redshift'
    ,sql=sql_statements.LOCATION_TRAFFIC_SQL
    ,dag=bulkLoadDag
)

# set Task dependencies
createTable >> copyS3Data >> locationTrafficQuery