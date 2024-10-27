import os
import uuid
from ytmusicapi import YTMusic
import yt_dlp
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS S3 Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_S3_REGION = os.getenv("AWS_S3_REGION")

# Initialize YTMusic and S3 client
ytmusic = YTMusic()
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_S3_REGION
)


def search_and_get_song_info(song_name):
    """Search for a song on YouTube Music and return its details."""
    search_results = ytmusic.search(song_name, filter='songs')
    if search_results:
        song = search_results[0]
        return {
            'title': song['title'],
            'artist': song['artists'][0]['name'],
            'videoId': song['videoId']
        }
    else:
        return None


def download_song(video_id, song_title):
    """Download the song from YouTube."""
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


def upload_to_s3(file_name, bucket):
    """Upload the file to AWS S3."""
    try:
        uuid_key = str(uuid.uuid4())
        s3_key = f"{uuid_key}/{os.path.basename(file_name)}"

        print(f"Uploading {file_name} to bucket {bucket} as {s3_key}")
        s3_client.upload_file(file_name, bucket, s3_key)

        print(f"Uploaded {file_name} to {bucket}/{s3_key}")
        return s3_key
    except FileNotFoundError:
        print(f"File {file_name} not found.")
    except NoCredentialsError:
        print("Credentials not available for AWS.")
    except Exception as e:
        print(f"An error occurred: {e}")


def download_and_upload_hardcoded_songs():
    """Download hardcoded songs and upload them to S3."""
    hardcoded_songs = [

    ]

    for song_name in hardcoded_songs:
        song_info = search_and_get_song_info(song_name.strip())

        if song_info:
            title = song_info['title']
            video_id = song_info['videoId']

            print(f"Downloading '{title}'...")
            song_file = download_song(video_id, title)
            print(f"Downloaded '{title}' successfully!")

            print(f"Uploading '{title}' to S3...")
            upload_to_s3(song_file, AWS_S3_BUCKET)
            if os.path.exists(song_file):
                os.remove(song_file)
                print(f"Deleted local file: {song_file}")
        else:
            print(f"Song '{song_name}' not found! Continuing to the next song.")


def main():
    download_and_upload_hardcoded_songs()


if __name__ == "__main__":
    main()
