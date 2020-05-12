# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time timestamp,
    user_id varchar(10),
    level varchar(10),
    song_id varchar(100),
    artist_id varchar(100),
    session_id int,
    location varchar(200),
    user_agent varchar(200),
    FOREIGN KEY (start_time) REFERENCES time (start_time),
    FOREIGN KEY (user_id) REFERENCES users (user_id)
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
INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    values (%s, %s, %s, %s, %s, %s, %s, %s)  
""")

user_table_insert = (""" 
INSERT INTO users (
    user_id, first_name, last_name, gender, level)
    values (%s, %s, %s, %s, %s)
ON CONFLICT (user_id)
DO NOTHING    
""")

song_table_insert = (""" 
INSERT INTO songs (song_id, title, artist_id, year, duration) values (%s, %s, %s, %s, %s)
ON CONFLICT (song_id)
DO NOTHING
""")

artist_table_insert = (""" 
INSERT INTO artists (artist_id, name, location, latitude, longitude) values (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id)
DO NOTHING
""")


time_table_insert = (""" 
INSERT INTO time (
    start_time, hour, day, week, month, year, weekday)
    values (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time)
DO NOTHING
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, s.artist_id 
FROM songs s
INNER JOIN artists a
ON a.artist_id = s.artist_id
WHERE s.title = %s AND a.name = %s AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]