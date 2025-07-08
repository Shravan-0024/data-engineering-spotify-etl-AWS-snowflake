# Import required libraries
import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime
import urllib.parse
import requests

# AWS Lambda handler function
def lambda_handler(event, context):
    
    # Fetch client credentials from environment variables (secure way)
    cilent_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    # Use Spotify's Client Credentials Flow to get an access token
    client_credentials_manager = SpotifyClientCredentials(client_id=cilent_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    
    # Get the access token directly (as a string, not dictionary)
    access_token = sp.auth_manager.get_access_token(as_dict=False)
    
    # Define the search query (playlist name you're looking for)
    search_query = "Top 50 - India"
    
    # Encode the query string to be safe for use in a URL
    q = urllib.parse.quote(search_query)
    
    # Spotify search endpoint URL (searching for a playlist)
    url = f"https://api.spotify.com/v1/search?q={q}&type=playlist"

    # Add the Bearer token in the request headers
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    # Make the API call to search for the playlist
    response = requests.get(url, headers=headers)
    playlist_data = response.json()

    # Get the playlist ID (here, you're selecting the 6th item in results — index 5)
    playlist_id = playlist_data['playlists']['items'][5]['id']
    
    # Use the playlist ID to fetch all track data
    playlist_url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    
    # Send GET request to retrieve playlist track details
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(playlist_url, headers=headers)
    track_data = response.json()

    # Extract the top 50 tracks (in case playlist has more)
    top_50_songs = track_data['items'][0:50]

    # Connect to AWS S3
    client = boto3.client('s3')
    
    # Define filename with timestamp to avoid overwriting
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    # Upload the track data as JSON file to S3 in raw_data/to_process folder
    client.put_object(
        Bucket="spotify-etl-project-sannu",
        Key="raw_data/to_process/" + filename,
        Body=json.dumps(top_50_songs)  # Convert Python list to JSON string
    )