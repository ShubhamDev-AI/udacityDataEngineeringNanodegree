import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

FLOAT_COLS_DEFAULT_VALUE = 9999999.99
STRING_COLS_DEFAULT_VALUE = 'Unknown'
INTEGER_COLS_DEFAULT_VALUE = 9999

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json( 
         filepath
        ,lines=True
    )

    float_cols = [ 
         'artist_latitude'
        ,'artist_longitude'
        ,'duration'
    ]

    integer_cols = ['year']

    string_attribute_cols = [
         'artist_location'
        ,'artist_name'
        ,'song_id'
        ,'title'
    ]

    # default values are set for:
    #  missing data in float type columns
    df[float_cols] = df[float_cols].fillna(value=FLOAT_COLS_DEFAULT_VALUE)

    # missing values in string type columns
    df[string_attribute_cols] = df[string_attribute_cols] \
        .fillna(value=STRING_COLS_DEFAULT_VALUE) \
        .replace( to_replace='',value=STRING_COLS_DEFAULT_VALUE)

    # missing values in integer type columns
    df['year'] = df['year'].fillna(value=INTEGER_COLS_DEFAULT_VALUE)

    # insert song record
    song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].iloc[0].values)
        
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(
         filepath
        ,lines=True
     )
    
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
                             ,'userId'
                            ]

    log_float_attributes = [ 'length'
                            ,'registration'
                           ]

    log_int_attributes = [ 'itemInSession'
                          ,'sessionId'
                          ,'status'
                          ,'ts'
                         ]
    
    # boolean indexing is used to filter "NextSong" events
    next_song_criteria = df['page'] == 'NextSong'

    # filter by NextSong action
    df = df.loc[next_song_criteria]

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    time_data = []
    
    time_data.append(df['ts'].dt.strftime('%Y%m%d %H:%M:%S.%f').values[0])

    time_data.append(df['ts'].dt.day.values[0])

    time_data.append(df['ts'].dt.hour.values[0])

    time_data.append(df['ts'].dt.week.values[0])

    time_data.append(df['ts'].dt.month.values[0])

    time_data.append(df['ts'].dt.year.values[0])

    time_data.append(df['ts'].dt.weekday_name.values[0])
    
    column_labels = [ 'start_time'
                     ,'hour'
                     ,'day'
                     ,'week'
                     ,'month'
                     ,'year'
                     ,'weekday'
                    ]
    
    time_df = pd.DataFrame( data=[time_data]
                           ,columns=column_labels
                          )

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    
    # load user table
    target_user_attributes = [ 'userId'
                              ,'firstName'
                              ,'lastName'
                              ,'gender'
                              ,'level'
                             ]

    user_df = df.loc[:,target_user_attributes]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        songid, artistid = results if results else None, None

        # insert songplay record
        try:
            songplay_data = (row.ts, row.userId, row.level, songid, artistid)
            cur.execute(songplay_table_insert, songplay_data)
            
        except psycopg2.Error as error:
            print("Couldn't insert data")
            print(error)
            conn.rollback()


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()