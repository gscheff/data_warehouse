import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
REGION = config.get("S3", "REGION")


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"


# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE events_staging (
    artist VARCHAR,
    auth VARCHAR NOT NULL,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INT NOT NULL,
    lastName VARCHAR,
    length DECIMAL,
    level VARCHAR NOT NULL,
    location VARCHAR,
    method VARCHAR NOT NULL,
    page VARCHAR NOT NULL,
    registration DECIMAL,
    sessionId INT NOT NULL,
    song VARCHAR,
    status INT NOT NULL,
    ts BIGINT NOT NULL,
    userAgent VARCHAR,
    userId int
)
""")


staging_songs_table_create = ("""
CREATE TABLE songs_staging (
    song_id CHAR(18) NOT NULL,
    num_songs INT NOT NULL,
    title VARCHAR NOT NULL,
    artist_name VARCHAR NOT NULL,
    artist_latitude VARCHAR,
    year INT NOT NULL,
    duration DECIMAL NOT NULL,
    artist_id CHAR(18) NOT NULL,
    artist_longitude VARCHAR,
    artist_location VARCHAR
)
""")


songplay_table_create = ("""
CREATE TABLE songplays
(
    songplay_id BIGINT IDENTITY (0, 1) PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL,
    user_id     INT       NOT NULL,
    level       VARCHAR   NOT NULL,
    song_id     CHAR(18)  NOT NULL,
    artist_id   CHAR(18)  NOT NULL,
    session_id  INT       NOT NULL,
    location    VARCHAR,
    user_agent  VARCHAR   NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
);
""")


user_table_create = ("""
CREATE TABLE users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender CHAR(1) NOT NULL,
    level VARCHAR NOT NULL
)
""")


song_table_create = ("""
CREATE TABLE songs (
    song_id CHAR(18) PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id CHAR(18) NOT NULL,
    year INT NOT NULL,
    duration DECIMAL NOT NULL
)
""")


artist_table_create = ("""
CREATE TABLE artists (
    artist_id CHAR(18) PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude DECIMAL,
    longitude DECIMAL
)
""")


time_table_create = ("""
CREATE TABLE times (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
)
""")


# STAGING TABLES
staging_events_copy = ("""
    copy events_staging from {}
    iam_role '{}'
    format as json {}
    region {}
""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSONPATH, REGION)


staging_songs_copy = ("""
    copy songs_staging from {}
    iam_role '{}'
    json 'auto'
    region {}
""").format(SONG_DATA, IAM_ROLE_ARN, REGION)


# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id,
                       session_id, location, user_agent)
SELECT timestamp 'epoch' + event.ts / 1000 * interval '1 second' AS start_time,
       event.userId                                              AS user_id,
       event.level,
       song.song_id,
       song.artist_id,
       event.sessionId                                           AS session_id,
       event.location,
       event.userAgent                                           AS user_agent
FROM events_staging AS event
         JOIN songs_staging AS song
              ON event.song = song.title AND event.artist = song.artist_name
WHERE event.page = 'NextSong';

""")


user_table_insert = ("""
INSERT INTO users
SELECT event.userId    AS user_id,
       event.firstName AS first_name,
       event.lastName  AS last_name,
       event.gender,
       event.level
FROM events_staging AS event
         JOIN (
    SELECT max(ts) AS ts,
           userId
    FROM events_staging
    WHERE page = 'NextSong'
    GROUP BY userId
) AS u ON event.userId = u.userId AND event.ts = u.ts;
""")


song_table_insert = ("""
INSERT INTO songs
SELECT song_id,
       title,
       artist_id,
       year,
       duration
FROM songs_staging;
""")


artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT artist_id,
                artist_name      AS name,
                artist_location  AS location,
                artist_latitude  AS latitude,
                artist_longitude AS longitude
FROM songs_staging;
""")


time_table_insert = ("""
INSERT INTO times
SELECT ts.start_time,
       date_part(hour, ts.start_time)  AS hour,
       date_part(day, ts.start_time)   AS day,
       date_part(week, ts.start_time)  AS week,
       date_part(month, ts.start_time) AS month,
       date_part(year, ts.start_time)  AS year,
       date_part(dow, ts.start_time)   AS weekday
FROM (
         SELECT DISTINCT timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time
         FROM events_staging
         WHERE page = 'NextSong'
     ) AS ts;
""")


drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
    ]


create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
    ]


copy_table_queries = [
    staging_songs_copy,
    staging_events_copy,
    ]


insert_table_queries = [
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    songplay_table_insert,
    ]
