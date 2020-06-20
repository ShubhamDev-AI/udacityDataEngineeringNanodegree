import datetime
import sql_statements
from airflow import DAG
from airflow.operators import (
     PostgresOperator
    ,SubDagOperator
)
# import function from subdag.py file within "lesson03Exercise03" folder
from lesson03Exercise03.subdag import redshift_table_ddl_plus_bulk_load

"""----------------------------------------------------------------------------
    Lesson 3 Exercise 3

    This exercise optimizes code created in Lesson 3 Exercise 1 by:
    - encapsulating the "create", "bulk load" and "quality check" operations
in a single generic SubDag Python Factory function. The function is then called
on a "per table" basis;

----------------------------------------------------------------------------"""

# define "startDate" once as it's gonna be throughout this script
startDate = datetime.datetime(2020,6,19)
# define "main" DAG "dag_id" parameter once
parentDagId = 'lesson3Exercise3'

# instantiate DAG Class
mainDag = DAG(
     dag_id=parentDagId
    ,start_date=startDate
    ,max_active_runs=1
    ,schedule_interval='@once'
)

# define "tripDataSubdag" variable
tripDataSubdag = 'tripDataSubdag'

# NOTICE a SubDag is actually a Task, an instance of "SubDagOperator"
tripsSubdagTask = SubDagOperator(
     task_id=tripDataSubdag
    #--------------------------------------------------------------------------
    # ATTENTION! 
    #   - "subdag" is a mandatory "SubDagOperator" parameter
    #   - "subdag" receives a Python Factory function as input
    #--------------------------------------------------------------------------
    ,subdag=redshift_table_ddl_plus_bulk_load(
         parent_dag_id=parentDagId
        ,parent_task_id=tripDataSubdag
        ,redshift_conn_id="redshift"
        ,aws_credentials_id="aws_credentials"
        ,table='trips'
        ,create_table_statement=sql_statements.CREATE_TRIPS_TABLE_SQL
        ,s3_bucket='udacity-dend'
        ,s3_key='data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv'
        ,start_date=startDate
    )
    ,dag=mainDag
)

# define "stationDataSubdag" variable
stationDataSubdag = 'stationDataSubdag'

# Remember the "Subdag/Task Parity"
stationDataSubdagTask = SubDagOperator(
     task_id=stationDataSubdag
    ,subdag=redshift_table_ddl_plus_bulk_load(
         parent_dag_id=parentDagId
        ,parent_task_id=stationDataSubdag
        ,redshift_conn_id="redshift"
        ,aws_credentials_id="aws_credentials"
        ,table='stations'
        ,create_table_statement=sql_statements.CREATE_STATIONS_TABLE_SQL
        ,s3_bucket='udacity-dend'
        ,s3_key='data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv'
        ,start_date=startDate
    )
    ,dag=mainDag
)

# run quick data analysis
locationTrafficAnalysis = PostgresOperator(
     task_id='locationTrafficAnalysis'
    ,postgres_conn_id='redshift'
    ,sql=sql_statements.LOCATION_TRAFFIC_SQL
    ,dag=mainDag
)

#------------------------------------------------------------------------------
#   ATTENTION!
#
#   Remember DEPENDENCIES are set BETWEEN TASK INSTANCES ONLY.
#
#   Dependencies cannot be set between "SubDagOperator" instances. This mistake
# was made during the development of this script. Here's Airflow UI warning
# message:
#
#------------------------------------------------------------------------------
tripsSubdagTask >> locationTrafficAnalysis
stationDataSubdagTask >> locationTrafficAnalysis