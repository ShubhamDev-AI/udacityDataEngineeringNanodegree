import os
import configparser
from time import time
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.types as sqlTypes
from pyspark.sql.window import Window
import pyspark.sql.functions as sqlFunctions


"""----------------------------------------------------------------------------
    Use "configparser" to read AWS credentials from a configuration file.    
----------------------------------------------------------------------------"""

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS_CREDENTIALS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS_CREDENTIALS','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    """ Create a pyspark.sql.SparkSession() Class instance

    Use this instance to program Spark with the Dataset and DataFrame API.    
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    """Perform ETL steps on "song-data" JSON files.

    Args:
        spark: the SparkSession currently in use by the "main()" program

        input_data (string): path to where the data to ingest is located

        output_data (string): path to where the processed data will be saved.    
    """
    
    # standard schema for "Song" JSON files is set below
    songJsonSchema = sqlTypes.StructType([
         sqlTypes.StructField('artist_id',sqlTypes.StringType())
        ,sqlTypes.StructField('artist_latitude',sqlTypes.FloatType())
        ,sqlTypes.StructField('artist_location',sqlTypes.StringType())
        ,sqlTypes.StructField('artist_longitude',sqlTypes.FloatType())
        ,sqlTypes.StructField('artist_name',sqlTypes.StringType())
        ,sqlTypes.StructField('duration',sqlTypes.FloatType())
        ,sqlTypes.StructField('num_songs',sqlTypes.IntegerType())
        ,sqlTypes.StructField('song_id',sqlTypes.StringType())
        ,sqlTypes.StructField('title',sqlTypes.StringType())
        ,sqlTypes.StructField('year',sqlTypes.IntegerType())
    ])
    
    # point filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json' #<< BOTH LOCAL AND S3 MULTIPLE FILES PATH
    
    # Read "song-data" JSON Files
    logTime, startUnixEpoch, processName = getCurrentTime(processName='song_data JSON ingestion')
    print(f'{logTime} UTC: {processName} execution started.')

    song_df = spark.read.json(
         path=song_data
        ,multiLine=True
        ,schema=songJsonSchema
    )

    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')


    """------------------------------------------------------------------------
        create DIM_SONGS dimension
    ------------------------------------------------------------------------"""

    # select appropriate columns to create the songs table
    dim_songs_df = song_df.select(
         'song_id'
        ,'title'
        ,'artist_id'
        ,'year'
        ,'duration'
    ).dropDuplicates()

    # WRITE PARTITIONED DataFrame as per Udacity's requirements
    logTime, startUnixEpoch, processName = getCurrentTime(processName='partitioned dim_songs Parquet write')
    print(f'{logTime} UTC: {processName} execution started.')

    dim_songs_df.write.parquet(
         path=output_data+'dim_songs'
        ,mode='overwrite'
        ,partitionBy=['year','artist_id']
    )

    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')

    # WRITE NON-PARTITIONED DataFrame for better read performance
    logTime, startUnixEpoch, processName = getCurrentTime(processName='non-partitioned dim_songs Parquet write')
    print(f'{logTime} UTC: {processName} execution started.')

    dim_songs_df.write.parquet(
         path=output_data+'dim_songs_non_partitioned'
        ,mode='overwrite'
    )

    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')

    """------------------------------------------------------------------------
        create DIM_ARTISTS dimension
    ------------------------------------------------------------------------"""

    # Select appropriate columns to create the artists table
    dim_artists_df = song_df.select(
         'artist_id'
        ,'artist_name'
        ,'artist_location'
        ,'artist_latitude'
        ,'artist_longitude'
    ).dropDuplicates()
    
    logTime, startUnixEpoch, processName = getCurrentTime(processName='dim_artists Parquet write')
    print(f'{logTime} UTC: {processName} execution started.')

    dim_artists_df.write.parquet(
         path=output_data+'dim_artists'
        ,mode='overwrite'    
    )

    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')


def process_log_data(spark, input_data, output_data):
    """ Perform ETL steps on "log-data" JSON Files

    Args:
        spark: the SparkSession currently in use by the "main()" program;

        input_data (string): path to where the data to ingest is located;

        output_data (string): path to where the processed data will be saved; 
    """
    
    # standard schema for "Song" JSON files is set below
    logsJsonSchema = sqlTypes.StructType([
         sqlTypes.StructField('artist',sqlTypes.StringType())
        ,sqlTypes.StructField('auth',sqlTypes.StringType())
        ,sqlTypes.StructField('firstName',sqlTypes.StringType())
        ,sqlTypes.StructField('gender',sqlTypes.StringType())
        ,sqlTypes.StructField('itemInSession',sqlTypes.IntegerType())
        ,sqlTypes.StructField('lastName',sqlTypes.StringType())
        ,sqlTypes.StructField('length',sqlTypes.FloatType())
        ,sqlTypes.StructField('level',sqlTypes.StringType())
        ,sqlTypes.StructField('location',sqlTypes.StringType())
        ,sqlTypes.StructField('method',sqlTypes.StringType())
        ,sqlTypes.StructField('page',sqlTypes.StringType())
        ,sqlTypes.StructField('registration',sqlTypes.FloatType())
        ,sqlTypes.StructField('sessionId',sqlTypes.IntegerType())
        ,sqlTypes.StructField('song',sqlTypes.StringType())
        ,sqlTypes.StructField('status',sqlTypes.ByteType())
        ,sqlTypes.StructField('ts',sqlTypes.LongType())
        ,sqlTypes.StructField('userAgent',sqlTypes.StringType())
        ,sqlTypes.StructField('userId',sqlTypes.StringType())
    ])
    
    # UDF to create timestamp column from original unix epoch column
    @sqlFunctions.udf(sqlTypes.TimestampType())
    def epoch_to_timestamp(unix_epoch): 
        """Convert Unix Epoch values into "human-readable" timestamp format.
        
        Args:
            unix_epoch (int): Unix Epoch value to convert.
            
        Returns:
            timestamp (timestamp): Unix Epoch value conversion result.
        """

        # Unix-Epoch values are converted to human-readable timestamps
        try:
            timestamp = datetime.fromtimestamp(unix_epoch / 1000)

        # NULL values handling happens here
        except Exception:
            return None  

        return timestamp
    
    """------------------------------------------------------------------------
        Read "log-data" files from S3 Bucket
    ------------------------------------------------------------------------"""

    # get filepath to log data file
    log_data = input_data+'log_data/*/*/*.json' # <<-- S3 MULTIPLE FILES PATH

    # Read "log-data" JSON Files
    logTime, startUnixEpoch, processName = getCurrentTime(processName='log_data JSON ingestion')
    print(f'{logTime} UTC: {processName} execution started.')

    df_logs = spark.read.json(
         path=log_data
        ,schema=logsJsonSchema
    )

    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')
    
    #  The "ts" column is converted into human-readable timestamp;
    df_logs = df_logs.withColumn('event_timestamp',epoch_to_timestamp(df_logs['ts']))

    """----------------------------------------------------------------------------
        create DIM_USERS dimension
    ----------------------------------------------------------------------------"""

    """----------------------------------------------------------------------------
    
        NOTES: the "dim_users" table ETL follows the steps below:
    
        1. Select columns from the "log-data" DataFrame, along with the "ts" column
    (already transformed into "event_timestamp");
        2. Use a Window Function to chronologically order each user's logged event 
    row and bring the "level" option from the immediately preceding logged event to
    the current log row being evaluated. When a "previous" event is not available
    for the user, it means the current event being evaluated is the user's first
    ever logged event. In this case, the previous "level" attribute defaults to
    'userFirstEvent';
        3. Filter records to keep only rows where a user's current subscription 
    "level" is different from its previous subscription "level".
        4. Apply another Window Function on this filtered dataset. For each listed
    user, this Window Function will order the filtered events chronologically and
    fetch to the current evaluated row the "event timestamp" of the next event row.
    When a "next row" is not available for a given user, the Window Function will
    then default to a '9999-12-31 23:59:59' timestamp, indicating this is the 
    user's current "level" option.
     
    ----------------------------------------------------------------------------"""

    #  SET WINDOW FUNCTION SPECIFICATIONS FOR CHANGE DATA CAPTURE

    # user subscription "level" option changes tracking
    levelChangeWindowSpec = Window \
        .partitionBy(sqlFunctions.col('user_id')) \
        .orderBy(sqlFunctions.col('event_timestamp'))

    userPreviousLevelOption = sqlFunctions.lag(sqlFunctions.col('level'),1,'userFirstEvent').over(levelChangeWindowSpec)

    # user subscription "level" validity timespan
    levelValidUntilWindowSpec = Window \
        .partitionBy(sqlFunctions.col('user_id')) \
        .orderBy(sqlFunctions.col('event_timestamp'))

    # expression to calculate "subscription_level_valid_until" column
    userLevelValidUntilExpression = sqlFunctions.lead(
         sqlFunctions.col('event_timestamp')
        ,1
        ,'9999-12-31 23:59:59'
    ).over(levelValidUntilWindowSpec)

    dim_users_etl = df_logs.select(
         sqlFunctions.col('userId').alias('user_id')
        ,sqlFunctions.col('firstName').alias('first_name')
        ,sqlFunctions.col('lastName').alias('last_name')
        ,sqlFunctions.col('gender')
        ,sqlFunctions.col('level')
        ,sqlFunctions.col('event_timestamp')
    ) \
    .where("user_id IS NOT NULL AND user_id <> ''") \
    .withColumn('previous_event_subscription_level',userPreviousLevelOption) \
    .where("level <> previous_event_subscription_level") \
    .withColumn('subscription_level_valid_until',userLevelValidUntilExpression) \
    .withColumn(
         'is_current_user_level'
        ,sqlFunctions.when(
             sqlFunctions.col('subscription_level_valid_until') == '9999-12-31 23:59:59'
            ,True) \
        .otherwise(False))
    
    # Select final columns for "dim_users" table
    dim_users_df = dim_users_etl.select(
         sqlFunctions.col('user_id')
        ,sqlFunctions.col('first_name')
        ,sqlFunctions.col('last_name')
        ,sqlFunctions.col('gender')
        ,sqlFunctions.col('level').alias('subscription_level')
        ,sqlFunctions.col('event_timestamp').alias('subscription_level_valid_since')
        ,sqlFunctions.col('subscription_level_valid_until')
        ,sqlFunctions.col('is_current_user_level')
    )
    
    # Write "dim_users" DataFrame to Parquet files
    logTime, startUnixEpoch, processName = getCurrentTime(processName='dim_users Parquet write')
    print(f'{logTime} UTC: {processName} execution started.')

    dim_users_df.write.parquet(
         path=output_data+'dim_users'
        ,mode='overwrite'    
    )

    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')
    
    """------------------------------------------------------------------------
        create DIM_TIME dimension
    ------------------------------------------------------------------------"""

    dim_time_df = df_logs.select(
         df_logs['event_timestamp']
        ,sqlFunctions.hour(df_logs['event_timestamp']).alias('hour')
        ,sqlFunctions.dayofmonth(df_logs['event_timestamp']).alias('day_of_month')
        ,sqlFunctions.weekofyear(df_logs['event_timestamp']).alias('week_of_year')
        ,sqlFunctions.month(df_logs['event_timestamp']).alias('month')
        ,sqlFunctions.year(df_logs['event_timestamp']).alias('year')
        ,sqlFunctions.dayofweek(df_logs['event_timestamp']).alias('weekday')
    ).dropDuplicates()
    
    # WRITE PARTITIONED table as per Udacity's requirements
    logTime, startUnixEpoch, processName = getCurrentTime(processName='partitioned dim_time Parquet write')
    print(f'{logTime} UTC: {processName} execution started.')


    dim_time_df.write.parquet(
         path=output_data+'dim_time'
        ,mode='overwrite'
        ,partitionBy=['year','month']
    )

    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')

    # WRITE NON-PARTITIONED table for better performance
    logTime, startUnixEpoch, processName = getCurrentTime(processName='non-partitioned dim_time Parquet write')
    print(f'{logTime} UTC: {processName} execution started.')

    dim_time_df.write.parquet(
         path=output_data+'dim_time_non_partitioned'
        ,mode='overwrite'
    )

    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')

    """------------------------------------------------------------------------
        create FACT_SONGPLAYS fact table
    ------------------------------------------------------------------------"""

    #  "dim_songs" pre processed table is read back to memory, thus saving IO and
    # avoiding a repetition of ETL steps already performed.
    logTime, startUnixEpoch, processName = getCurrentTime(processName='dim_songs Parquet ingestion')
    print(f'{logTime} UTC: {processName} execution started.')

    dim_songs = spark.read.parquet(output_data+'dim_songs_non_partitioned')

    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')
    
    #  "dim_artists" pre processed table is read back to memory, thus saving IO and
    # avoiding a repetition of ETL steps already performed.
    logTime, startUnixEpoch, processName = getCurrentTime(processName='dim_artists Parquet ingestion')
    print(f'{logTime} UTC: {processName} execution started.')
    
    dim_artists = spark.read.parquet(output_data+'dim_artists')

    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')

    # "log_data" columns for the "fact_songplays" table are selected below
    # Add a "songplay_id" column by using a SQL function
    # 'NextSong' pages indicate a songplay event, so only these pages are kept.
    log_events = df_logs.select(
         sqlFunctions.col('event_timestamp')
        ,sqlFunctions.col('sessionId').alias('session_id')
        ,sqlFunctions.col('userId').alias('user_id')
        ,sqlFunctions.col('level')
        ,sqlFunctions.col('song')
        ,sqlFunctions.col('length')
        ,sqlFunctions.col('artist')
        ,sqlFunctions.col('location')
        ,sqlFunctions.col('userAgent').alias('user_agent')
    ) \
    .where("page = 'NextSong' AND user_id IS NOT NULL") \
    .withColumn('songplay_id',sqlFunctions.monotonically_increasing_id())

    # joins with multiple conditions must be passed as a list
    log_songs_join_conditions = [log_events['song'] == dim_songs['title'] , log_events['length'] == dim_songs['duration']]

    #  the "log_events" DataFrame is joined to both "songs" and "artists" Dimensions
    # to perform a lookup of needed attributes present in them.
    fact_songplays_df = log_events \
        .join(
             other=dim_songs
            ,on=log_songs_join_conditions
            ,how='inner'
        ) \
        .join(
             other=dim_artists
            ,on=log_events['artist'] == dim_artists['artist_name']
            ,how='inner'
        ) \
        .select(
             log_events['songplay_id']
            ,log_events['event_timestamp']
            ,log_events['user_id']
            ,log_events['level']
            ,dim_songs['song_id']
            ,dim_artists['artist_id']
            ,log_events['session_id']
            ,log_events['location']
            ,log_events['user_agent']
            ,sqlFunctions.year(log_events['event_timestamp']).alias('year')
            ,sqlFunctions.month(log_events['event_timestamp']).alias('month') 
        )

    # WRITE PARTITIONED table as per Udacity's requirements
    logTime, startUnixEpoch, processName = getCurrentTime(processName='partitioned fact_songplays Parquet write')
    print(f'{logTime} UTC: {processName} execution started.')

    fact_songplays_df.write.parquet(
         path=output_data+'fact_songplays'
        ,mode='overwrite'
        ,partitionBy=['year','month']
    )

    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')

    # WRITE NON-PARTITIONED table for better performance
    logTime, startUnixEpoch, processName = getCurrentTime(processName='non-partitioned fact_songplays Parquet write')
    print(f'{logTime} UTC: {processName} execution started.')
    
    fact_songplays_df.write.parquet(
         path=output_data+'fact_songplays_non_partitioned'
        ,mode='overwrite'
    )

    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')
    
    
def getCurrentTime(**kwargs):
    """Set variables containing timestamp and unix epoch at function call time.

    The function accepts an optional keyword argument, "processName".

    Args:

        kwargs:

            processName (string): an optional string passed by the user to identify
                the process or activity that'll have its duration measured. This 
                argument is sent back as additional output to be used by the calling
                application.
    
    Returns:

        currentTimestamp (string): a string containing a timestamp in the format 'YYYY-MM-DD HH:MM:SS'
        
        currentUnixEpoch (integer): an integer value containing a Unix Epoch value.

        kwargs['processName'] (string): the process name/description passed by the user in the
            "processName" keyword argument.
    
    Returns:
        tuple: (currentTimestamp, currentUnixEpoch)        
    """
    
    # check whether the "kwargs" dictionary is empty (false) or not (true)
    if bool(kwargs):
        
        # evaluate the presence of a "processName" argument and treat it accordingly
        if kwargs['processName']:

            currentTimestamp, currentUnixEpoch = datetime.now().strftime("%Y-%m-%d %H:%M:%S"), int(round(time() * 1000))

            return currentTimestamp, currentUnixEpoch, kwargs['processName']
    
    # no keyword arguments passed. Follow default behaviour
    else:        
        currentTimestamp, currentUnixEpoch = datetime.now().strftime("%Y-%m-%d %H:%M:%S"), int(round(time() * 1000))
        
        result = currentTimestamp, currentUnixEpoch
        
    return result


def main():
    """ Coordinate overall ETL process execution flow.

        This function takes no arguments. For simplicity's sake, a wrapper's
    not been written for the 3 main functions called by this function:

        * create_spark_session()
        * process_song_data()
        * process_log_data()

        Nevertheless, a pattern revolves around these functions that enables
    capturing their times of execution. This pattern uses the variables listed
    below to implement this functionality:

        * logTime (timestamp): step execution date and time;

        * mainStartUnixEpoch (integer): "main()" script execution start Unix 
            epoch;

        * mainCompletionUnixEpoch (integer): "main()" script execution completion
            Unix epoch;

        * processName (string): user input for the name of the process whose
            duration will be measured;

        * startUnixEpoch (integer): measured process execution start Unix epoch;

        * completionUnixEpoch (integer): measured process execution completion
            Unix epoch;
    
    """
    
    """------------------------------------------------------------------------
    Start "main()" function execution timing
    ------------------------------------------------------------------------"""

    logTime, mainStartUnixEpoch, processName = getCurrentTime(processName='main()')
    print(f'{logTime} UTC: {processName} execution started.')

    """------------------------------------------------------------------------
    create a SparkSession Instance
    ------------------------------------------------------------------------"""

    logTime, startUnixEpoch, processName = getCurrentTime(processName='create_spark_session()')
    print(f'{logTime} UTC: {processName} execution started.')
    
    spark = create_spark_session()
    
    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')

    """------------------------------------------------------------------------
    set both data source and destination root paths
    ------------------------------------------------------------------------"""
    
    input_data = 's3a://udacity-dend/'
    output_data='s3a://hds-dataeng/udacityNanodegreeDataLake/'

    # inform data source and destination    
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'UTC: reading from:',input_data)
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'UTC: writing to:',output_data)
    
    """------------------------------------------------------------------------
    Process "song-data" files
    ------------------------------------------------------------------------"""

    logTime, startUnixEpoch, processName = getCurrentTime(processName='process_song_data()')
    print(f'{logTime} UTC: {processName} execution started.')
    
    process_song_data(spark, input_data, output_data)
    
    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')
    
    """------------------------------------------------------------------------
    Process Event Logs
    ------------------------------------------------------------------------"""
    
    logTime, startUnixEpoch, processName = getCurrentTime(processName='process_log_data()')
    print(f'{logTime} UTC: {processName} execution started.')
    
    process_log_data(spark, input_data, output_data)
    
    logTime, completionUnixEpoch = getCurrentTime()
    print(f'{logTime} UTC: {processName} took {completionUnixEpoch - startUnixEpoch} ms to execute.')
    
    """------------------------------------------------------------------------
    Calculate "main()" function execution duration
    ------------------------------------------------------------------------"""
    logTime, mainCompletionUnixEpoch, processName = getCurrentTime(processName='main()')
    print(f'{logTime} UTC: {processName} took {mainCompletionUnixEpoch - mainStartUnixEpoch} ms to execute.')
    
if __name__ == "__main__":

    main()

