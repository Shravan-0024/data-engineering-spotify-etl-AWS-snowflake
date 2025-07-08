# Import necessary libraries
import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd 

# Function to extract album information from Spotify data
def album(data):
    album_list = []
    for row in data:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {
            'album_id': album_id,
            'name': album_name,
            'release_date': album_release_date,
            'total_tracks': album_total_tracks,
            'url': album_url
        }
        album_list.append(album_element)
    return album_list

# Function to extract artist information from Spotify data
def artist(data):
    artist_list = []
    for row in data:
        for key, value in row.items():
            if key == "track":
                for artist in value['artists']:
                    artist_dict = {
                        'artist_id': artist['id'],
                        'artist_name': artist['name'],
                        'external_url': artist['href']
                    }
                    artist_list.append(artist_dict)
    return artist_list

# Function to extract song information from Spotify data
def songs(data):
    song_list = []
    for row in data:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {
            'song_id': song_id,
            'song_name': song_name,
            'duration_ms': song_duration,
            'url': song_url,
            'popularity': song_popularity,
            'song_added': song_added,
            'album_id': album_id,
            'artist_id': artist_id
        }
        song_list.append(song_element)
    return song_list

# Main Lambda function handler
def lambda_handler(event, context):
    s3 = boto3.client('s3')  # Create S3 client
    Bucket = "spotify-etl-project-sannu"  # S3 bucket name
    Key = "raw_data/to_process/"  # Folder path for raw input JSON files

    spotify_data = []  # To hold parsed JSON data
    spotify_keys = []  # To hold processed file keys

    # Loop through objects in the raw input folder
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":  # Process only JSON files
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())  # Convert to Python dict
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)

    # Loop through each JSON object and extract + transform
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = songs(data)

        # Create dataframes and drop duplicates
        album_df = pd.DataFrame.from_dict(album_list).drop_duplicates(subset=['album_id'])
        artist_df = pd.DataFrame.from_dict(artist_list).drop_duplicates(subset=['artist_id'])
        song_df = pd.DataFrame.from_dict(song_list)

        # Convert string dates to datetime objects
        album_df['release_date'] = pd.to_datetime(album_df['release_date'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])

        # Transform and upload song data to S3
        songs_key = "transformed_data/songs/songs_transformed_" + str(datetime.now()) + ".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=songs_key, Body=song_content)

        # Transform and upload album data to S3
        album_key = "transformed_data/album/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)

        # Transform and upload artist data to S3
        artist_key = "transformed_data/artist/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)

    # Move processed raw files to another folder and delete originals
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        # Copy file to 'processed' folder
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])
        # Delete original file from 'to_process' folder
        s3_resource.Object(Bucket, key).delete()
