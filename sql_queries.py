# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id bigint PRIMARY KEY,
    start_time timestamp,
    user_id varchar(10),
    level varchar(10),
    song_id varchar(100),
    artist_id varchar(100),
    session_id int,
    location varchar(200),
    user_agent varchar(200),
    FOREIGN KEY (start_time) REFERENCES time (start_time),
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id varchar(10) PRIMARY KEY,
    first_name varchar(200), 
    last_name varchar(200), 
    gender char(1), 
    level varchar(10)
);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id varchar(100) PRIMARY KEY, 
    title varchar(100),
    artist_id varchar(100), 
    year int, 
    duration float,
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id varchar(100) PRIMARY KEY, 
    name varchar(200),
    location varchar(200),
    latitude varchar(20),
    longitude varchar(20)
);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time timestamp PRIMARY KEY,
    hour smallint, 
    day smallint, 
    week smallint, 
    month smallint, 
    year int, 
    weekday varchar(10)
);
""")

# INSERT RECORDS

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

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]