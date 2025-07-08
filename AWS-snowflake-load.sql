CREATE OR REPLACE DATABASE AWS_S3_LOAD;

CREATE OR REPLACE SCHEMA AWS_S3_PIPELINE;

CREATE OR REPLACE STORAGE INTEGRATION s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::126623035604:role/s3_snowflake_conn'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-sannu')

DESC INTEGRATION s3_init;

CREATE OR REPLACE FILE FORMAT csv_fileformat
    TYPE = csv
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE STAGE AWS_spotify_stage
    URL='s3://spotify-etl-project-sannu/transformed_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_fileformat;

CREATE OR REPLACE TABLE album(
    album_id VARCHAR(255),
    name_ VARCHAR(255),
    release_date DATE,
    total_tracks VARCHAR(255),
    url_ VARCHAR(255)
);

CREATE OR REPLACE TABLE artist(
    artist_id VARCHAR(255),
    artist_name VARCHAR(255),
    external_url VARCHAR(255)
);

CREATE OR REPLACE TABLE songs(
    song_id VARCHAR(255),
    song_name VARCHAR(255),
    duration_ms INT,
    url_ VARCHAR(255),
    popularity INT,
    song_added TIMESTAMP_TZ,
    album_id VARCHAR(255),
    artist_id VARCHAR(255)
);

CREATE OR REPLACE PIPE AWS_spotify_album_pipe
AUTO_INGEST = True
AS
COPY INTO album
FROM @AWS_spotify_stage/album/;

CREATE OR REPLACE PIPE AWS_spotify_artist_pipe
AUTO_INGEST = True
AS
COPY INTO artist
FROM @AWS_spotify_stage/artist/;

CREATE OR REPLACE PIPE AWS_spotify_songs_pipe
AUTO_INGEST = True
AS
COPY INTO songs
FROM @AWS_spotify_stage/songs/;

SHOW pipes;

DESC pipe AWS_spotify_album_pipe;

DESC pipe AWS_spotify_artist_pipe;

DESC pipe AWS_spotify_songs_pipe;

SELECT count(*) FROM album;

SELECT count(*) FROM artist;

SELECT count(*) FROM songs;