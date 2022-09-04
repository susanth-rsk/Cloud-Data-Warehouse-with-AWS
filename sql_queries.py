import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSONPATH")
LOG_DATA = config.get("S3", "LOG_DATA")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist        VARCHAR,
        auth          VARCHAR,
        firstName     VARCHAR,
        gender        CHAR(1),
        itemInSession INT,
        lastName      VARCHAR,
        length        FLOAT,
        level         VARCHAR,
        location      TEXT,
        method        VARCHAR,
        page          VARCHAR,
        registration  VARCHAR,
        sessionId     INT SORTKEY DISTKEY,
        song          VARCHAR,
        status        INT,
        ts            BIGINT,
        userAgent     VARCHAR,
        userId        INT
    );
        )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id           VARCHAR(20) SORTKEY DISTKEY,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     TEXT,
        artist_name         VARCHAR,
        duration            FLOAT,
        num_songs           INT,
        song_id             VARCHAR(20),
        title               VARCHAR,
        year                INT
    );
""")

songplay_table_create = ("""
      CREATE TABLE IF NOT EXISTS songplays (
        songplay_id      INT IDENTITY(0,1) NOT NULL SORTKEY,
        start_time       TIMESTAMP NOT NULL,
        user_id          INT NOT NULL DISTKEY,
        level            VARCHAR(6) NOT NULL,
        song_id          VARCHAR(20),
        artist_id        VARCHAR(20),
        session_id       INT NOT NULL,
        location         TEXT,
        user_agent       TEXT
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id        INT NOT NULL DISTKEY SORTKEY,
        first_name     VARCHAR,
        last_name      VARCHAR,
        gender         VARCHAR(3),
        level          VARCHAR(6) NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id        VARCHAR(20) NOT NULL,
        title          VARCHAR NOT NULL,
        artist_id      VARCHAR(20) NOT NULL,
        year           INT NOT NULL,
        duration       FLOAT NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id      VARCHAR(20) NOT NULL SORTKEY,
        name           VARCHAR NOT NULL,
        location       TEXT,
        latitude       FLOAT,
        longitude      FLOAT
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time     TIMESTAMP NOT NULL SORTKEY,
        hour           INT,
        day            INT,
        week           INT,
        month          INT,
        year           INT DISTKEY,
        weekday        VARCHAR
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON {}
    TIMEFORMAT as 'epochmillisecs'
    COMPUPDATE OFF region 'us-west-2'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, ARN, LOG_JSON_PATH)


staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto'
    COMPUPDATE OFF region 'us-west-2' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT 
        timestamp with time zone 'epoch' + se.ts/1000 * interval '1 second', 
        se.userId, 
        se.level, 
        ss.song_id, 
        ss.artist_id, 
        se.sessionId, 
        se.location, 
        se.userAgent
    FROM staging_events se
    JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE page = 'NextSong' AND userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time,
                    EXTRACT(hour from start_time),
                    EXTRACT(day from start_time),
                    EXTRACT(week from start_time),
                    EXTRACT(month from start_time),
                    EXTRACT(year from start_time),
                    EXTRACT(weekday from start_time)
    FROM staging_events
    WHERE page = 'NextSong' AND userId IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
