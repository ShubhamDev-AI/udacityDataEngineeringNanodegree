
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (
     StageJsonToRedshiftOperator
    ,LoadFactOperator
    ,LoadDimensionOperator
    ,DataQualityOperator
)
from helpers import SqlQueries

defaultArgumentsDict = {
     'owner': 'Heder Santos'
    ,'start_date': datetime(2019, 1, 12)
    ,'catchup':False
    ,'depends_on_past':False
    ,'retries':3
    ,'retry_delay':timedelta(seconds=300)
    ,'email_on_retry':False
}

sparkifyPipeline = DAG(
         dag_id='sparkifyPipeline'
        ,default_args=defaultArgumentsDict
        ,description='Orchestrate data load and processing tasks for moving data from S3 to Redshift.'
        ,schedule_interval='@hourly'
        ,max_active_runs=1
        # set "graph" as DAG default UI view
        ,default_view='graph'
        # set "Top to Bottom" as graph default layout
        ,orientation='TB'
)

startExecution = DummyOperator(
     task_id='startExecution'
    ,dag=sparkifyPipeline
)

stageEventLogsToRedshift = StageJsonToRedshiftOperator(
     task_id='stageEventLogsToRedshift'
    ,aws_region="us-west-2"
    ,redshift_conn_id="redshift"
    ,aws_credentials_id="aws_credentials"
    ,bucket_name="udacity-dend"
    ,s3_key="log_data"
    ,schema="public"
    ,table="events"
    ,jsonpaths_file="s3://udacity-dend/log_json_path.json"
    ,truncate=False
    ,create_table_statement=SqlQueries.createTableStagingEvents
    ,dag=sparkifyPipeline
)

stageSongsToRedshift = StageJsonToRedshiftOperator(
     task_id='stageSongsToRedshift'
    ,aws_region="us-west-2"
    ,redshift_conn_id="redshift"
    ,aws_credentials_id="aws_credentials"
    ,bucket_name="udacity-dend"
    ,s3_key="song_data"
    ,schema="public"
    ,table="songs"
    ,jsonpaths_file="auto"
    ,truncate=False
    ,create_table_statement=SqlQueries.createTableStagingSongs
    ,dag=sparkifyPipeline
)

#------------------------------------------------------------------------------
#   REMINDER:
#
#   This DummyOperator instance had to be created because Airflow doesn't
# support chaining two list instances using bitshift operators.
#   Directly trying to chain "parallelStagingLoad" to "parallelDimensionLoad" 
# (lists created further down this code) threw the following error:
#
#   "airflow unsupported operand type(s) for >>: 'list' and 'list'"
#------------------------------------------------------------------------------
triggerParallelDimensionsLoad = DummyOperator(
     task_id='triggerParallelDimensionsLoad'
    ,dag=sparkifyPipeline
)

loadFactSongplays = LoadFactOperator(
     task_id='loadFactSongplays'
    ,redshift_conn_id='redshift'
    ,sql_insert_statement=SqlQueries.insertIntoFactSongplays
    ,create_table_statement=SqlQueries.createTableFactSongplays
    ,schema='public'
    ,table='fact_songplays'
    ,truncate=False
    ,dag=sparkifyPipeline
)

loadDimUsers = LoadDimensionOperator(
     task_id='loadDimUsers'
    ,redshift_conn_id='redshift'
    ,sql_insert_statement=SqlQueries.insertIntoDimUsers
    ,create_table_statement=SqlQueries.createTableDimUser
    ,schema='public'
    ,table='dim_users'
    ,truncate=True
    ,dag=sparkifyPipeline
)

loadDimSongs = LoadDimensionOperator(
     task_id='loadDimSongs'
    ,redshift_conn_id='redshift'
    ,sql_insert_statement=SqlQueries.insertIntoDimSongs
    ,create_table_statement=SqlQueries.createTableDimSongs
    ,schema='public'
    ,table='dim_songs'
    ,truncate=True
    ,dag=sparkifyPipeline
)

loadDimArtists = LoadDimensionOperator(
     task_id='loadDimArtists'
    ,redshift_conn_id='redshift'
    ,sql_insert_statement=SqlQueries.insertIntoDimArtists
    ,create_table_statement=SqlQueries.createTableDimArtists
    ,schema='public'
    ,table='dim_artists'
    ,truncate=True
    ,dag=sparkifyPipeline
)

loadDimTime = LoadDimensionOperator(
     task_id='loadDimTime'
    ,redshift_conn_id='redshift'
    ,sql_insert_statement=SqlQueries.insertIntoDimTime
    ,create_table_statement=SqlQueries.createTableDimTime
    ,schema='public'
    ,table='dim_time'
    ,truncate=True
    ,dag=sparkifyPipeline
)

checkFactTable = DataQualityOperator(
     task_id='checkFactTable'
    ,redshift_conn_id='redshift'
    # send simple row count query for DataQualityOperator to execute.
    ,single_valued_result_query='SELECT COUNT(*) FROM public.fact_songplays'
    # define result-range lower bound: one row at least
    ,query_result_range_start=1
    # set 10 billion minus 1 as interval upper bound
    ,query_result_range_end=9999999999
    ,dag=sparkifyPipeline
)

endExecution = DummyOperator(
     task_id='endExecution'
    ,dag=sparkifyPipeline)

# create list object containing all Staging Area tables load Tasks
parallelStagingLoad = [stageEventLogsToRedshift, stageSongsToRedshift]
# create list object containing all DW Dimensions load Tasks
parallelDimensionLoad = [
     loadDimSongs
    ,loadDimUsers
    ,loadDimArtists
    ,loadDimTime
]
#   TIP: Chain Task dependencies inside a tuple to leverage better row formatting
# thus improving script readability.
startExecution >> parallelStagingLoad >> triggerParallelDimensionsLoad

triggerParallelDimensionsLoad >> parallelDimensionLoad >> loadFactSongplays 

loadFactSongplays >> checkFactTable >> endExecution



