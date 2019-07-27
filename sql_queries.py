# DROP TABLES
# ----------------------------------------------------------------------
log_staging_table_drop = "DROP TABLE IF EXISTS log_staging CASCADE"
song_staging_table_drop = "DROP TABLE IF EXISTS song_staging CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

drop_table_queries = [log_staging_table_drop,
                      song_staging_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]


# CREATE TABLES
# ----------------------------------------------------------------------
log_staging_table_create = """
 CREATE TABLE IF NOT EXISTS log_staging (
    artist TEXT, 
    auth TEXT, 
    firstName TEXT, 
    gender TEXT, 
    ItemInSession INT, 
    lastName TEXT, 
    length FLOAT8, 
    level TEXT, 
    location TEXT, 
    method TEXT, 
    page TEXT, 
    registration TEXT, 
    sessionId INT, 
    song TEXT, 
    status INT, 
    ts timestamp, 
    userAgent TEXT, 
    userId INT);    
"""

song_staging_table_create = """
 CREATE TABLE IF NOT EXISTS song_staging (
    song_id TEXT PRIMARY KEY, 
    artist_id TEXT, 
    artist_latitude FLOAT8, 
    artist_location TEXT, 
    artist_longitude FLOAT8, 
    artist_name TEXT, 
    duration FLOAT8, 
    num_songs INT, 
    title TEXT, 
    year INT);    
"""

# facts ----------------------------------------------------------------
songplay_table_create = """
 CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY PRIMARY KEY, 
    start_time timestamp NOT NULL REFERENCES time(start_time) sortkey, 
    user_id INT NOT NULL REFERENCES users(user_id), 
    level TEXT NOT NULL, 
    song_id TEXT NOT NULL REFERENCES songs(song_id), 
    artist_id TEXT NOT NULL REFERENCES artists(artist_id) distkey, 
    session_id INT NOT NULL, 
    location TEXT NOT NULL, 
    user_agent TEXT NOT NULL);
"""

# dimensions -----------------------------------------------------------
user_table_create = """
 CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY sortkey, 
    first_name TEXT NOT NULL, 
    last_name TEXT NOT NULL, 
    gender TEXT NOT NULL, 
    level TEXT NOT NULL)
    diststyle ALL;
"""

song_table_create = """
 CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT PRIMARY KEY sortkey, 
    title TEXT NOT NULL, 
    artist_id TEXT NOT NULL, 
    year INT NOT NULL, 
    duration NUMERIC NOT NULL)
    diststyle ALL;
"""

artist_table_create = """
 CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY distkey, 
    name TEXT NOT NULL, 
    location TEXT NOT NULL, 
    latitude FLOAT8 NOT NULL, 
    longitude FLOAT8 NOT NULL)
"""

time_table_create = """
 CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY sortkey, 
    hour INT NOT NULL, 
    day INT NOT NULL, 
    week INT NOT NULL, 
    month INT NOT NULL, 
    year INT NOT NULL, 
    weekday INT NOT NULL)
"""

create_table_queries = [log_staging_table_create,
                        song_staging_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create,
                        songplay_table_create]
