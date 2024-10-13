import os
from ytmusicapi import YTMusic
import yt_dlp
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env file
# AWS S3 Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_S3_REGION = os.getenv("AWS_S3_REGION")

# Initialize YTMusic
ytmusic = YTMusic()

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_S3_REGION
)

def search_and_get_song_info(song_name):
    search_results = ytmusic.search(song_name, filter='songs')
    if search_results:
        song = search_results[0]
        return {
            'title': song['title'],
            'artist': song['artists'][0]['name'],
            'album': song['album']['name'] if 'album' in song else 'N/A',
            'videoId': song['videoId']
        }
    else:
        return None

def download_song(video_id, song_title):
    # Create 'songs' directory if it doesn't exist
    if not os.path.exists('songs'):
        os.makedirs('songs')

    url = f"https://www.youtube.com/watch?v={video_id}"
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': f'songs/{song_title}.%(ext)s'
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])

    return f'songs/{song_title}.webm'  # Return the file name after downloading

def upload_to_s3(file_name, bucket, s3_key):
    try:
        # Adding log to check if function is called
        print(f"Attempting to upload {file_name} to bucket {bucket}")
        
        # Uploading file to S3
        s3_client.upload_file(file_name, bucket, s3_key)
        print(f"Uploaded {file_name} to {bucket}/{s3_key}")
    except FileNotFoundError:
        print(f"File {file_name} not found.")
    except NoCredentialsError:
        print("Credentials not available for AWS.")
    except Exception as e:
        print(f"An error occurred: {e}")


def main():
    song_name = input("Enter the song name: ")
    song_info = search_and_get_song_info(song_name)
    
    if song_info:
        print(f"Downloading '{song_info['title']}' by {song_info['artist']}")
        print(f"Album: {song_info['album']}")
        
        # Download the song
        song_file = download_song(song_info['videoId'], song_info['title'])
        print(f"Download complete! Saved as {song_file}")
        
        # Upload the song to S3
        s3_key = f'{song_file}'  # Path inside the bucket
        upload_to_s3(song_file, AWS_S3_BUCKET, s3_key)

        print("Song uploaded to S3 successfully!")
    else:
        print("Song not found!")

if __name__ == "__main__":
    main()
