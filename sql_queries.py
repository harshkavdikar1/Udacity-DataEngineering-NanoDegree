import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar(200),
    auth varchar(20),
    first_name varchar(200),
    gender char(1),
    item_in_session int,
    last_name varchar(200),
    length decimal,
    level varchar(10),
    location varchar(200),
    method varchar(10),
    page varchar(100),
    registration varchar(20),
    session_id int,
    song varchar(200),
    status smallint,
    ts bigint,
    user_agent varchar(200),
    user_id varchar(10)
);    
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int,
    artist_id varchar(100),
    artist_latitude decimal,
    artist_longitude decimal,
    artist_location varchar(200),
    artist_name varchar(200),
    song_id varchar(100),
    title varchar(100),
    duration decimal,
    year int
);
""")


# To match with the table time we have start time as distkey
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id bigint IDENTITY(0,1),
    start_time timestamp NOT NULL,
    user_id varchar(10) NOT NULL,
    level varchar(10) NOT NULL,
    song_id varchar(100),
    artist_id varchar(100),
    session_id int NOT NULL,
    location varchar(200),
    user_agent varchar(200)
);
""")


# Users table has very less records as compared to songplays table that's why we can have user_id
# as SORTKEY and DISTTYLE as ALL, we want it to be loaded to all slices without distributing when 
# performing analytics
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar(10) NOT NULL SORTKEY,
    first_name varchar(200),
    last_name varchar(200),
    gender char(1),
    level varchar(10) NOT NULL
);
""")


# Songs table has very less records as compared to songplays table that's why we can have song_id
# as SORTKEY and DISTTYLE as ALL, we want it to be loaded to all slices without distributing when 
# performing analytics
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar(100) NOT NULL SORTKEY,
    title varchar(100) NOT NULL,
    artist_id varchar(100) NOT NULL,
    year int NOT NULL,
    duration decimal NOT NULL
)
DISTSTYLE ALL;
""")


# Since the number of artists as compared to songs and song play tables will be very less
# we have artist_id as SORTKEY with DISTSTYLE as ALL, we want it to be loaded to all slices 
# without distributing when performing analytics
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar(100) NOT NULL SORTKEY,
    name varchar(200) NOT NULL,
    location varchar(200),
    latitude decimal,
    longitude decimal
)
DISTSTYLE ALL;
""")


# The time table has same records as songplays table that's why we have start time as sortkey and 
# distkey (because we need to distribute it among various slices to perform parallel execution)
# with DISTSTYLE set to Auto (let redshift decide which one is better optimization technique)
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp NOT NULL DISTKEY,
    hour smallint NOT NULL,
    day smallint NOT NULL, 
    week smallint NOT NULL,
    month smallint NOT NULL,
    year int NOT NULL,
    weekday varchar(10) NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-east-2'
format as JSON {}
timeformat as 'epochmillisecs';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-east-2'
json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second', user_id, level, song_id, artist_id, session_id, location, user_agent 
FROM staging_events as events
LEFT OUTER JOIN staging_songs as songs   
ON events.song = songs.title
AND events.artist = songs.artist_name
AND events.length = songs.duration
WHERE events.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (
    user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level 
FROM staging_events as events
WHERE events.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id, title, artist_id, year, duration) 
SELECT song_id, title, artist_id, year, duration 
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id, name, location, latitude, longitude) 
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time, hour, day, week, month, year, weekday)
SELECT 
    start_time,
    EXTRACT (hour FROM start_time),
    EXTRACT (day FROM start_time),
    EXTRACT (week FROM start_time),
    EXTRACT (month FROM start_time),
    EXTRACT (year FROM start_time),
    EXTRACT (weekday FROM start_time)
FROM (
    SELECT DISTINCT(TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') as start_time 
    FROM staging_events as events
    WHERE events.page = 'NextSong'
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
