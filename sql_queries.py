import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS song_play"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
)
""")

staging_songs_table_create = ("""
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id   IDENTITY(0, 1) PRIMARY KEY,
    start_time    date not null,
    user_id       int not null,
    level         text not null,
    song_id       text not null,
    artist_id     text not null,
    session_id    int not null,
    location      text not null,
    user_agent    text not null
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
    year        text not null,
    duration    float not null
)
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id   text PRIMARY KEY,
    name        text not null,
    location    text not null,
    latitude    text,
    longitude   text
)
""")

time_table_create = ("""
CREATE TABLE time(
    start_time   timestamp PRIMARY KEY,
    hour         text not null,
    day          text not null,
    week         text not null,
    month        text not null,
    year         text not null,
    weekday      text not null
)
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
