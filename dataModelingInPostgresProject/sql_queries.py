# DROP TABLES

songplay_table_drop = """
    DROP TABLE IF EXISTS
        public.fact_songplay
    ;
"""

user_table_drop = """
    DROP TABLE IF EXISTS
        public.dim_users
    ;
"""

song_table_drop = """
    DROP TABLE IF EXISTS
        public.dim_songs
    ;
"""

artist_table_drop = """
    DROP TABLE IF EXISTS
        public.dim_artists
    ;
"""

time_table_drop = """
    DROP TABLE IF EXISTS
        public.dim_time
    ;
"""

# CREATE TABLES

songplay_table_create = """
    CREATE TABLE
        public.fact_songplays(
             songplay_id SERIAL PRIMARY KEY
            ,start_time TIMESTAMPTZ REFERENCES public.dim_time(start_time)
            ,user_id INTEGER REFERENCES public.dim_users(user_id)
            ,level TEXT NOT NULL
            ,song_id TEXT REFERENCES public.dim_songs(song_id)
            ,artist_id TEXT REFERENCES public.dim_artists(artist_id)
        )
    ;
"""

user_table_create = """
    CREATE TABLE
        public.dim_users(
             user_id INTEGER PRIMARY KEY
            ,first_name TEXT NOT NULL
            ,last_name TEXT NOT NULL
            ,gender TEXT NOT NULL
            ,level TEXT NOT NULL
        )
    ;
"""

song_table_create = """
    CREATE TABLE
        public.dim_songs(
             song_id TEXT PRIMARY KEY
            ,title TEXT NOT NULL
            ,artist_id TEXT NOT NULL
            ,year SMALLINT NOT NULL
            ,duration FLOAT8
        )
    ;
"""

artist_table_create = """
    CREATE TABLE
        public.dim_artists(
             artist_id TEXT PRIMARY KEY
            ,name TEXT NOT NULL
            ,location TEXT NOT NULL
            ,latitude FLOAT8 NOT NULL
            ,longitude FLOAT8 NOT NULL
        )
    ;
"""

time_table_create = """
    CREATE TABLE
        public.dim_time(
             start_time TIMESTAMPTZ PRIMARY KEY
            ,hour SMALLINT NOT NULL
            ,day SMALLINT NOT NULL
            ,week SMALLINT NOT NULL
            ,month SMALLINT NOT NULL
            ,year SMALLINT NOT NULL
            ,weekday TEXT NOT NULL
        )
    ;
"""

# INSERT RECORDS

songplay_table_insert = """
    INSERT INTO
        public.fact_songplays(
             start_time
            ,user_id
            ,level
            ,song_id
            ,artist_id
        )

    VALUES(
         %s
        ,%s
        ,%s
        ,%s
        ,%s
    )
    ;
"""

user_table_insert = """
    INSERT INTO
        public.dim_users(
             user_id
            ,first_name
            ,last_name
            ,gender
            ,level
        )
        
    VALUES(
         %s
        ,%s
        ,%s
        ,%s
        ,%s
    )
    
    ON CONFLICT
        (user_id)
        
    DO UPDATE
        SET level = dim_users.level
    ;
"""

song_table_insert = """
    INSERT INTO
        public.dim_songs(
             song_id
            ,title
            ,artist_id
            ,year
            ,duration
        )

    VALUES(
         %s
        ,%s
        ,%s
        ,%s
        ,%s
    )
    
    ON CONFLICT(
        song_id
    )
    
    DO NOTHING    
    ;
"""

artist_table_insert = """
    INSERT INTO
        public.dim_artists(
             artist_id
            ,name
            ,location
            ,latitude
            ,longitude
        )
        
    VALUES(
         %s
        ,%s
        ,%s
        ,%s
        ,%s
    )
    
    ON CONFLICT(
        artist_id
    )
        
    DO NOTHING
    ;
"""


time_table_insert = """
    INSERT INTO
        public.dim_time(
             start_time
            ,hour
            ,day
            ,week
            ,month
            ,year
            ,weekday
        )

    VALUES (
         %s
        ,%s
        ,%s
        ,%s
        ,%s
        ,%s
        ,%s
    )
    
    ON CONFLICT
        (start_time)
        
    DO NOTHING    
    ;
"""

# FIND SONGS

song_select = """
    SELECT
         dim_songs.song_id
        ,dim_songs.artist_id
        
    FROM
        public.dim_songs AS dim_songs
        
    LEFT JOIN
        public.dim_artists AS dim_artists
        
    ON
        dim_songs.artist_id
        =
        dim_artists.artist_id
        
    WHERE
        dim_songs.title = (%s)
        AND
        dim_artists.name = (%s)
        AND
        dim_songs.duration = (%s)        
    ;
"""

# QUERY LISTS

# "songplay" table's been moved to the end
# because its creation depends on foreign keys
# being previously available.
create_table_queries = [     
     user_table_create
    ,artist_table_create
    ,song_table_create    
    ,time_table_create
    ,songplay_table_create
]

drop_table_queries = [
     songplay_table_drop
    ,user_table_drop
    ,song_table_drop
    ,artist_table_drop
    ,time_table_drop
]