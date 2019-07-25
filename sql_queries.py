import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN                    = config.get("IAM_ROLE", "ARN")
S3_LOG_DATA            = config.get("S3", "LOG_DATA")
S3_LOG_JSONPATH        = config.get("S3", "LOG_JSONPATH")
S3_SONG_DATA           = config.get("S3", "SONG_DATA")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist              text,
    auth                text,
    first_name          text,
    gender              text,
    item_in_session     int,
    last_name           text,
    length              numeric,
    level               text,
    location            text,
    method              text,
    page                text,
    registeration       numeric,
    session_id          int,
    song                text,
    status              int,
    ts                  bigint,
    user_agent          text,
    user_id             int
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
        num_songs           int,
        artist_id           text,
        artist_latitude     numeric,
        artist_longitude    numeric,
        artist_location     text,
        artist_name         text,
        song_id             text,
        title               text,
        duration            numeric,
        year                int
    )
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id   int IDENTITY(0, 1) PRIMARY KEY,
    start_time    date not null,
    user_id       int not null,
    level         text,
    song_id       text,
    artist_id     text,
    session_id    int,
    location      text,
    user_agent    text
)
""")

user_table_create = ("""
CREATE TABLE users (
    user_id    int PRIMARY KEY,
    first_name text not null,
    last_name  text not null,
    gender     text not null,
    level      text not null
)
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id     text PRIMARY KEY,
    title       text not null,
    artist_id   text not null,
    year        int not null,
    duration    numeric not null
)
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id   text PRIMARY KEY,
    name        text not null,
    location    text,
    latitude    text,
    longitude   text
)
""")

time_table_create = ("""
CREATE TABLE time(
    start_time   timestamp PRIMARY KEY,
    hour         int not null,
    day          int not null,
    week         int not null,
    month        int not null,
    year         int not null,
    weekday      int not null
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
iam_role {}
JSON {}
""").format(S3_LOG_DATA, ARN, S3_LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
iam_role {}
FORMAT as JSON 'auto'
""").format(S3_SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' AS start_time,
       e.user_id as user_id,
       e.level as level,
       s.song_id as song_id,
       s.artist_id as artist_id,
       e.session_id as session_id,
       e.location as location,
       e.user_agent as user_agent
FROM staging_events e
LEFT JOIN staging_songs s ON e.song = s.title
WHERE e.page = 'NextSong'
AND e.artist = s.artist_name
AND e.length = s.duration
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT start_time,
       EXTRACT(hour from start_time) AS hour,
       EXTRACT(day from start_time) AS day,
       EXTRACT(week from start_time) AS week,
       EXTRACT(month from start_time) AS month,
       EXTRACT(year from start_time) AS year,
       EXTRACT(dayofweek from start_time) AS weekday
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
