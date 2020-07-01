
class SqlQueries:
    """
    """
    createTableStagingEvents = f"""
        CREATE TABLE public.events(
            event_id INTEGER IDENTITY(0,1)
            ,artist TEXT
            ,auth TEXT
            ,first_name TEXT
            ,gender TEXT
            ,item_in_session INTEGER
            ,last_name TEXT
            ,length FLOAT8
            ,level TEXT
            ,location TEXT
            ,method TEXT
            ,page TEXT
            ,registration FLOAT8
            ,session_id INTEGER
            ,song TEXT
            ,status SMALLINT
            ,ts BIGINT
            ,user_agent TEXT
            ,user_id TEXT
            ,PRIMARY KEY(event_id)
        );
    """

    createTableStagingSongs = f"""
        CREATE TABLE public.songs(
            song_id TEXT PRIMARY KEY
            ,songs INTEGER
            ,title TEXT
            ,artist_name TEXT
            ,artist_latitude FLOAT8
            ,year INTEGER
            ,duration FLOAT8
            ,artist_id TEXT
            ,artist_longitude FLOAT8
            ,artist_location TEXT
        );
    """



    createTableFactSongplays = f"""
        CREATE TABLE public.fact_songplays(
            songplay_key INTEGER IDENTITY (1,1) PRIMARY KEY
            ,start_time TIMESTAMPTZ REFERENCES public.dim_time(start_time)
            ,user_key INTEGER REFERENCES public.dim_users(user_key)
            ,level TEXT
            ,song_key TEXT REFERENCES public.dim_songs(song_key)
            ,artist_key TEXT REFERENCES public.dim_artists(artist_key)
            ,session_id INTEGER
            ,location TEXT
            ,user_agent TEXT
        )
        DISTSTYLE EVEN
        SORTKEY(start_time)
        ;
    """

    createTableDimUser = f"""
        CREATE TABLE public.dim_users(
            user_key INTEGER IDENTITY(1,1) PRIMARY KEY
            ,user_id INTEGER UNIQUE
            ,first_name TEXT
            ,last_name TEXT
            ,gender TEXT
            ,level TEXT
            ,level_valid_since TIMESTAMPTZ
            ,level_valid_until TIMESTAMPTZ
            ,is_current_user_level BOOLEAN
        )
        DISTSTYLE ALL
        SORTKEY(
            user_id
            ,level_valid_since
        )
        ;
    """

    createTableDimSongs = f"""
        CREATE TABLE public.dim_songs(
            song_key INTEGER IDENTITY(1,1) PRIMARY KEY
            ,song_id TEXT UNIQUE
            ,title TEXT
            ,artist_id TEXT
            ,year INTEGER
            ,duration FLOAT8
        )
        DISTSTYLE ALL
        SORTKEY(song_id)
        ;
    """

    createTableDimArtists = f"""
        CREATE TABLE public.dim_artists(
            artist_key INTEGER IDENTITY(1,1) PRIMARY KEY
            ,artist_id TEXT UNIQUE
            ,name TEXT
            ,location TEXT
            ,latitude FLOAT8
            ,longitude FLOAT8
        )
        DISTSTYLE ALL
        SORTKEY(artist_id, name)
        ;
    """

    createTableDimTime = f"""
        CREATE TABLE public.dim_time(
            start_time TIMESTAMPTZ PRIMARY KEY
            ,hour SMALLINT
            ,day SMALLINT
            ,week SMALLINT
            ,month SMALLINT
            ,year INTEGER
            ,weekday TEXT        
        )
        DISTSTYLE ALL
        SORTKEY(start_time)
        ;
    """

    # FINAL TABLES
    insertIntoFactSongplays = """
        /*----------------------------------------------------------------------------
            Use a CTE for event data preparation: convert the "ts" attribute from
        Unix Epoch values to human-friendly Timestamp values.
            Get rid of rows having no User ID's and narrow the query down to 'NextSong'
        pages only.
        ----------------------------------------------------------------------------*/
        INSERT INTO public.fact_songplays(
            start_time
            ,user_key
            ,"level"
            ,song_key
            ,artist_key
            ,session_id
            ,location
            ,user_agent
        )

        WITH cte_timestamp_conversion AS (
            SELECT
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time
                ,session_id
                ,user_id
                ,song
                ,"length"	    
                ,artist
                ,location
                ,user_agent	    
            FROM
                public.events
            WHERE
                page = 'NextSong'
                AND
                user_id IS NOT NULL
        )

        SELECT
            start_time
            ,dim_users.user_key
            ,dim_users."level"
            ,dim_songs.song_key
            ,dim_artists.artist_key
            ,events.session_id
            ,events.location
            ,events.user_agent
        FROM
            cte_timestamp_conversion AS events
        INNER JOIN
            public.dim_songs AS dim_songs
        ON
            events.song = dim_songs.title
            AND
            events."length" = dim_songs.duration
        INNER JOIN
            public.dim_artists AS dim_artists
        ON
            events.artist = dim_artists.name
        INNER JOIN
            public.dim_users AS dim_users
        ON
            events.user_id = dim_users.user_id
            AND
            events.start_time >= dim_users.level_valid_since
            AND
            events.start_time < dim_users.level_valid_until
    ;
    """

    insertIntoDimUsers = """
    /*------------------------------------------------------------------------------

        TYPE 2 SLOWLY CHANGING DIMENSION HANDLING: "level" attribute

        A CTE is used to generate an ordered list of user events and user
    descriptive data.

        For each user row, the attribute "previous_level" shows what level this
    same user had when the imediately preceding event occurred. If a user has no 
    previous event it means the row points to a user's first recorded event and this 
    is explicited by the "user first event" text contained in the "previous_level" 
    attribute in these cases.

        The "level" and "previous_level" attributes are then compared in a subsequent
    step to detect when the user changed from "free" to "paid" subscription and
    vice versa.

        The "level_valid_since" and "level_valid_until" attributes enable us to
    determine whether a given event happened during a "paid" or "free" subscription
    timespan for each user. The "is_current_user_level" boolean attribute enables
    convenient retrieval of a user's latest subscription option.

    ------------------------------------------------------------------------------*/

        INSERT INTO public.dim_users(
            user_id
            ,first_name
            ,last_name
            ,gender
            ,"level"
            ,level_valid_since
            ,level_valid_until
            ,is_current_user_level
        )

        WITH cte_level_detection AS (
            SELECT
                user_id::INTEGER AS user_id
                ,COALESCE(first_name,'Unknown') AS first_name
                ,COALESCE(last_name,'Unknown') AS last_name
                ,COALESCE(gender,'Unknown') AS gender
                ,"level"
                ,COALESCE(
                    LAG("level",1) OVER (
                        PARTITION BY 
                            user_id
                        ORDER BY
                            ts
                    )
                ,'user first event'
                ) AS previous_level
                ,TIMESTAMP 'epoch' + events.ts/1000 * interval '1 second' AS event_timestamp

            FROM
                public.events
            WHERE
                user_id IS NOT NULL
            ORDER BY
                user_id
                ,event_timestamp
        )

        SELECT
            user_id
            ,first_name
            ,last_name
            ,gender
            ,"level"
            ,event_timestamp AS level_valid_since
            ,COALESCE(
                LEAD(event_timestamp,1) OVER (
                    PARTITION BY
                        user_id
                    ORDER BY
                        event_timestamp ASC
                )
            ,'99991231 23:59:59'
            ) AS level_valid_until

            ,CASE
                WHEN
                    LEAD(event_timestamp,1) OVER (
                        PARTITION BY
                            user_id
                        ORDER BY
                            event_timestamp ASC
                    )
                    IS NULL
                THEN
                    TRUE
                ELSE
                    FALSE
            END AS is_current_user_level
        FROM
            cte_level_detection
        --  records solely having divergent subscription options are
        -- relevant for the current query.
        WHERE
            "level" <> previous_level    
        ORDER BY
            user_id
            ,level_valid_since
        ;
    """

    insertIntoDimSongs = """
        INSERT INTO public.dim_songs (
            song_id
            ,title
            ,artist_id
            ,year
            ,duration
        )

        SELECT DISTINCT
            COALESCE(stage_songs.song_id,'Unknown') AS song_id
            ,COALESCE(stage_songs.title,'Unkown') AS title
            ,COALESCE(stage_songs.artist_id,'Unknown') AS artist_id
            ,COALESCE(stage_songs.year,9999) AS year
            ,COALESCE(stage_songs.duration,9999999.99) AS duration
        FROM
            public.songs AS stage_songs
        LEFT JOIN
            public.dim_songs AS dim_songs
        ON
            stage_songs.song_id = dim_songs.song_id
        AND
            dim_songs.title IS NULL
        ;
    """

    insertIntoDimArtists = """
        INSERT INTO public.dim_artists(
            artist_id
            ,name
            ,location
            ,latitude
            ,longitude
        )

        SELECT DISTINCT
             COALESCE(stage_songs.artist_id,'Unkown') AS artist_id
            ,COALESCE(stage_songs.artist_name,'Unknwon') AS name
            ,COALESCE(stage_songs.artist_location,'Unkown') AS location
            ,COALESCE(stage_songs.artist_latitude,9999999.99) AS latitude
            ,COALESCE(stage_songs.artist_longitude,9999999.99) AS longitude
        FROM
            public.songs AS stage_songs
        LEFT JOIN
            public.dim_artists AS dim_artists
        ON
            stage_songs.artist_id = dim_artists.artist_id
        WHERE
            dim_artists.name IS NULL
        ;
    """

    insertIntoDimTime = """
        INSERT INTO public.dim_time(
            start_time
            ,hour
            ,day
            ,week
            ,month
            ,year
            ,weekday
        )

        /*----------------------------------------------------------
            A CTE is used to fetch unique values of Unix Epoch
        and convert them to proper Timestamp format.
        ----------------------------------------------------------*/
        WITH cte_unique_timestamps AS (
            SELECT DISTINCT
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS timestamp
            FROM
                public.events
        )

        SELECT
             cte.timestamp AS start_time
            ,EXTRACT(HOUR FROM cte.timestamp) AS hour
            ,EXTRACT(DAY FROM cte.timestamp) AS day
            ,EXTRACT(WEEK FROM cte.timestamp) AS week
            ,EXTRACT(MONTH FROM cte.timestamp) AS month
            ,EXTRACT(YEAR FROM cte.timestamp) AS year
            ,EXTRACT(DOW FROM cte.timestamp) AS weekday
        FROM
            cte_unique_timestamps AS cte
        LEFT JOIN
            public.dim_time AS dim_time
        ON
            cte.timestamp = dim_time.start_time
        WHERE
            dim_time.hour IS NULL
        ;
    """

    dummyInsertIntoFactSongplays = """
        INSERT INTO public.fact_songplays(
             start_time
            ,user_key
            ,"level"
            ,song_key
            ,artist_key
            ,session_id
            ,location
            ,user_agent
        )

        SELECT
             GETDATE() AS start_time
            ,1 AS user_key
            ,'paid' AS "level"
            ,'TRAAAAK128F9318786' AS song_key
            ,'zehDasCouves' AS artist_key
            ,1 AS session_id
            ,'brotas' AS location
            ,'fairi fox' AS user_agent
        ;
    """
