import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    num_songs int,
    artist_id varchar(100),
    artist_latitude varchar(10),
    artist_longitude varchar(10),
    artist_location varchar(200),
    artist_name varchar(200),
    song_id varchar(100),
    title varchar(100),
    duration float,
    year int
);    
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    artist varchar(200),
    auth varchar(20),
    first_name varchar(200),
    gender char(1),
    item_in_session int,
    last_name varchar(200),
    length float,
    level varchar(10),
    location varchar(200),
    method varchar(10),
    page varchar(100),
    registration varchar(20),
    session_id int,
    song varchar(100),
    status smallint,
    ts timestamp,
    user_agent varchar(200),
    user_id varchar(10)
);
""")


# To match with the table time we have start time as distkey
songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id IDENTITY(0,1),
    start_time timestamp NOT NULL DISTKEY,
    user_id varchar(10) NOT NULL,
    level varchar(10) NOT NULL,
    song_id varchar(100) NOT NULL,
    artist_id varchar(100) NOT NULL,
    session_id int NOT NULL,
    location varchar(200) NOT NULL,
    user_agent varchar(200) NOT NULL
);
""")


# Users table has very less records as compared to songplays table that's why we can have user_id
# as SORTKEY and DISTTYLE as ALL, we want it to be loaded to all slices without distributing when 
# performing analytics
user_table_create = ("""
CREATE TABLE users (
    user_id varchar(10) NOT NULL SORTKEY,
    first_name varchar(200) NOT NULL,
    last_name varchar(200) NOT NULL,
    gender char(1) NOT NULL,
    level varchar(10) NOT NULL
);
""")


# Songs table has very less records as compared to songplays table that's why we can have song_id
# as SORTKEY and DISTTYLE as ALL, we want it to be loaded to all slices without distributing when 
# performing analytics
song_table_create = ("""
CREATE TABLE songs (
    song_id varchar(100) NOT NULL SORTKEY,
    title varchar(100) NOT NULL,
    artist_id varchar(100) NOT NULL,
    year int NOT NULL,
    duration float NOT NULL
)
DISTSTYLE ALL;
""")


# Since the number of artists as compared to songs and song play tables will be very less
# we have artist_id as SORTKEY with DISTSTYLE as ALL, we want it to be loaded to all slices 
# without distributing when performing analytics
artist_table_create = ("""
CREATE TABLE artists (
    artist_id varchar(100) NOT NULL SORTKEY,
    name varchar(200) NOT NULL,
    location varchar(200),
    latitude varchar(20),
    longitude varchar(20),
)
DISTSTYLE ALL;
""")


# The time table has same records as songplays table that's why we have start time as sortkey and 
# distkey (because we need to distribute it among various slices to perform parallel execution)
# with DISTSTYLE set to Auto (let redshift decide which one is better optimization technique)
time_table_create = ("""
CREATE TABLE time (
    start_time timestamp NOT NULL SORTKEY DISTKEY,
    hour smallint NOT NULL,
    day smallint NOT NULL, 
    week smallint NOT NULL,
    month smallint NOT NULL,
    year int NOT NULL,
    weekday int NOT NULL
)
DISTSTYLE AUTO;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2';
format as JSON {}
timeformat as 'epochmillisecs'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT to_timestamp(ts), user_id, level, song_id, artist_id, session_id, location, user_agent 
FROM staging_songs s
LEFT OUTER JOIN staging_events e
ON e.title = s.song 
AND e.name = s.artist 
AND e.duration = s.length
""")

user_table_insert = ("""
INSERT INTO users (
    user_id, first_name, last_name, gender, level)
SELECT user_id, first_name, last_name, gender, level 
FROM staging_songs;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id, title, artist_id, year, duration) 
SELECT song_id, title, artist_id, year, duration 
FROM staging_events;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id, name, location, latitude, longitude) 
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_location
FROM staging_events;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time, hour, day, week, month, year, weekday)
SELECT start_time,
EXTRACT (hour FROM start_time),
EXTRACT (day FROM start_time),
EXTRACT (week FROM start_time),
EXTRACT (month FROM start_time),
EXTRACT (year FROM start_time),
EXTRACT (dow FROM start_time)
FROM (
    SELECT to_timestamp(ts) as start_time FROM staging_events
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
