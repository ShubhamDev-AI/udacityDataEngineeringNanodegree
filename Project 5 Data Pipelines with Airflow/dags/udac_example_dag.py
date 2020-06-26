
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (
     StageJsonFromS3ToRedshift
    ,LoadFactOperator
    ,LoadDimensionOperator
    ,DataQualityOperator
)

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
     'owner': 'udacity'
    ,'start_date': datetime(2019, 1, 12)
    ,'catchup':False
    ,'depends_on_past':False
    ,'retries':3
    ,'retry_delay':timedelta(minutes=5)
    ,'email_on_retry':False
}

udacityDag = DAG('udac_example_dag',
         default_args=default_args
        ,description='Load and transform data in Redshift with Airflow'
        ,schedule_interval='0 * * * *'
        ,default_view='graph'
        ,orientation='TB'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=udacityDag)

stage_events_to_redshift = StageJsonFromS3ToRedshift(
     task_id='Stage_events'
    ,aws_region="us-west-2"
    ,redshift_conn_id="redshift"
    ,aws_credentials_id="aws_credentials"
    ,bucket_name="udacity-dend"
    ,s3_key=""
    ,schema_dot_table=""
    ,jsonpaths_file="auto"
    ,truncate=False
    ,dag=udacityDag
)

stage_songs_to_redshift = StageJsonFromS3ToRedshift(
    task_id='Stage_songs'
    ,aws_region="us-west-2"
    ,redshift_conn_id="redshift"
    ,aws_credentials_id="aws_credentials"
    ,bucket_name="udacity-dend"
    ,s3_key=""
    ,schema_dot_table=""
    ,jsonpaths_file="auto"
    ,truncate=False
    ,dag=udacityDag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=udacityDag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=udacityDag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=udacityDag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=udacityDag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=udacityDag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=udacityDag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=udacityDag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [
     load_song_dimension_table
    ,load_user_dimension_table
    ,load_artist_dimension_table
    ,load_time_dimension_table
] >> run_quality_checks >> end_operator


