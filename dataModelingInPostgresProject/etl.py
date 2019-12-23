import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

# data quality default values are set as CONSTANTS:
FLOAT_COLS_DEFAULT_VALUE = 9999999.99
STRING_COLS_DEFAULT_VALUE = 'Unknown'
SMALLINT_COLS_DEFAULT_VALUE = 9999
INTEGER_COLS_DEFAULT_VALUE = 9999999

def process_song_file(cur, filepath):
    """
    Processes the song files present in the received filepath directory.

    Parameters:
    cur (psycopg2 cursor object): the database connection cursor received.

    filepath (string): target directory to operate upon.
    """
    # open song file
    df = pd.read_json( 
         filepath
        ,lines=True
    )

    song_float_attributes = [ 
         'artist_latitude'
        ,'artist_longitude'
        ,'duration'
    ]

    song_integer_attributes = ['year']

    song_string_attributes = [
         'artist_location'
        ,'artist_name'
        ,'title'
    ]
    
    # default values are set for:
    #  missing data in float type columns
    df[song_float_attributes] = df[song_float_attributes].fillna(value=FLOAT_COLS_DEFAULT_VALUE)

    # missing values in string type columns
    df[song_string_attributes] = df[song_string_attributes] \
        .fillna(value=STRING_COLS_DEFAULT_VALUE) \
        .replace( to_replace='',value=STRING_COLS_DEFAULT_VALUE)

    # missing values in integer type columns
    df['year'] = df['year'].fillna(value=SMALLINT_COLS_DEFAULT_VALUE)
    

    # insert song record
    song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].iloc[0].values)        
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes the log files present in the received filepath directory.

    Parameters:
    cur (psycopg2 cursor object): the database connection cursor received.

    filepath (string): target directory to operate upon.
    """
    
    
    # open log file
    df = pd.read_json(
         filepath
        ,lines=True
     )
    
    # string attributes in the file are listed
    log_string_attributes = [ 'artist'
                             ,'auth'
                             ,'firstName'
                             ,'gender'
                             ,'lastName'
                             ,'level'
                             ,'location'
                             ,'method'
                             ,'page'
                             ,'song'
                             ,'userAgent'
                            ]

    # decimal number attributes in the file are listed
    log_float_attributes = [ 'length'
                            ,'registration'
                           ]

    # integer attributes in the file are listed
    log_int_attributes = [ 'itemInSession'
                          ,'status'
                         ]
    
    
    # replaces missing values in string type columns    
    df[log_string_attributes] = df[log_string_attributes] \
        .fillna(value=STRING_COLS_DEFAULT_VALUE) \
        .replace(to_replace='',value=STRING_COLS_DEFAULT_VALUE)
    
    # replace missing values in float type columns    
    df[log_float_attributes] = df[log_float_attributes] \
        .fillna(value=FLOAT_COLS_DEFAULT_VALUE)
    
    # replace missing integers    
    df[log_int_attributes] = df[log_int_attributes].fillna(value=SMALLINT_COLS_DEFAULT_VALUE)
    
    
    # boolean indexing is used to filter "NextSong" events
    next_song_criteria = df['page'] == 'NextSong'

    # filter by NextSong action
    df = df.loc[next_song_criteria]

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'],unit='ms')
    
    # "dim_time" column labels are specified
    column_labels = ( 'start_time'
                     ,'hour'
                     ,'day'
                     ,'week'
                     ,'month'
                     ,'year'
                     ,'weekday'
                    )
    
    # Timestamps are separated into a single Series object for further handling
    time_series = df['ts']

    # a tuple of Series objects holds the time dimension attributes
    time_data = ( time_series
                 ,time_series.dt.hour
                 ,time_series.dt.day
                 ,time_series.dt.week
                 ,time_series.dt.month
                 ,time_series.dt.year
                 ,time_series.dt.weekday
                )

    #  a DataFrame is created by zipping the column_labels and time_data tuples
    # into a Dictionary.
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    #  Data is inserted into the "dim_time" table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    
    # target user table attributes are listed based on the log file attributes
    target_user_attributes = [ 'userId'
                              ,'firstName'
                              ,'lastName'
                              ,'gender'
                              ,'level'
                             ]

    # user attributes are extracted from the log file
    user_df = df.loc[:,target_user_attributes]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        try:
            songplay_data = (row.ts, row.userId, row.level, songid, artistid)
            cur.execute(songplay_table_insert, songplay_data)
            
        except psycopg2.Error as error:
            print("Couldn't insert data")
            print(error)
            conn.rollback()


def process_data(cur, conn, filepath, func):
    """
    Iterates through all files in a given folder.

    Parameters:
    cur (psycopg2 cursor object): the database connection cursor received.

    conn (psycopg2 connection object): received database connection object.

    filepath (string): directory path over which the function iterates.

    func (custom function): the function called upon each file listed in 
    the target directory.
    """

    #  Get all files matching extension from directory
    #  An empty list ("all_files") is set up to receive the filenames
    # at each iteration cycle.

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    ETL process handling.

    Connects to the 'sparkifydb' database and calls functions to perform
    transformations on the song files and log files, respectively.
    """

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()