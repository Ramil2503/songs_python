from elasticsearch import Elasticsearch
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

# Initialize YTMusic, S3 client, and Elasticsearch client
ytmusic = YTMusic()
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_S3_REGION
)
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])


def create_index_if_not_exists(index_name):
    """Create the Elasticsearch index if it does not exist."""
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body={
            "settings": {
                "number_of_shards": 5
            },
            "mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "artist": {"type": "text"},
                    "s3_path": {"type": "text"}
                }
            }
        })
        print(f"Index '{index_name}' created.")
    else:
        print(f"Index '{index_name}' already exists.")



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


def search_and_get_songs_by_artist(artist_name):
    """Search for songs by a given artist on YouTube Music and return their details."""
    search_results = ytmusic.search(artist_name, filter='artists')

    # Ensure that there is at least one result and it has a 'browseId' for further lookup
    if search_results and 'browseId' in search_results[0]:
        artist = search_results[0]
        artist_id = artist['browseId']  # Use browseId as the channelId for get_artist

        # Retrieve artist data, ensuring it has 'songs' content if present
        artist_data = ytmusic.get_artist(artist_id)

        if 'songs' in artist_data and 'results' in artist_data['songs']:
            # Extract song information
            return artist_data['songs']['results']
        else:
            print(f"No songs found for artist '{artist_name}' with ID '{artist_id}'")
            return []
    else:
        print(f"Artist '{artist_name}' not found or missing 'browseId' field.")
        return []


def download_song(video_id, song_title):
    """Download the song from YouTube."""
    if not os.path.exists('songs'):
        os.makedirs('songs')

    url = f"https://www.youtube.com/watch?v={video_id}"
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': f'songs/{song_title}.%(ext)s'
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])

    return f'songs/{song_title}.webm'


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


def save_to_elasticsearch(song_info, s3_key):
    """Save song metadata to Elasticsearch."""
    index_name = "songs_sharded"
    create_index_if_not_exists(index_name)  # Ensure the index exists
    print(song_info)
    doc = {
        'id': s3_key,
        'title': song_info['title'],
        'artist': [artist['name'] for artist in song_info['artists']] if song_info.get('artists') else song_info['artist'],
        's3_path': f"s3://{AWS_S3_BUCKET}/{s3_key}"
    }
    es.index(index=index_name, body=doc)
    print(f"Saved metadata to Elasticsearch: {doc}")

def download_and_upload_song(song_title):
    song_info = search_and_get_song_info(song_title)
    file_name = download_song(song_info["videoId"],song_info["title"])
    s3_key = upload_to_s3(file_name, AWS_S3_BUCKET)
    if s3_key:
        save_to_elasticsearch(song_info, s3_key)
    else:
        print("error")

def download_and_upload_songs_by_artist(artist_name):
    """Download all songs by the specified artist and upload to S3."""
    songs = search_and_get_songs_by_artist(artist_name)

    if not songs:
        print(f"No songs found for artist '{artist_name}'")
        return

    for song in songs:
        title = song['title']
        video_id = song['videoId']

        print(f"Downloading '{title}'...")
        song_file = download_song(video_id, title)
        print(f"Downloaded '{title}' successfully!")

        print(f"Uploading '{title}' to S3...")
        s3_key = upload_to_s3(song_file, AWS_S3_BUCKET)

        # Save metadata to Elasticsearch
        if s3_key:
            save_to_elasticsearch(song, s3_key)

        # Delete the downloaded file
        if os.path.exists(song_file):
            os.remove(song_file)
            print(f"Deleted local file: {song_file}")


def fetch_and_index_songs_from_bucket():
    """Fetch all songs from the S3 bucket and index them in Elasticsearch."""
    try:
        response = s3_client.list_objects_v2(Bucket=AWS_S3_BUCKET)

        if 'Contents' in response:
            for item in response['Contents']:
                s3_key = item['Key']
                song_title = os.path.splitext(os.path.basename(s3_key))[0]  # Get the title without extension

                # Fetch metadata using the song title
                song_info = search_and_get_song_info(song_title)

                # Save metadata to Elasticsearch
                if song_info:
                    save_to_elasticsearch(song_info, s3_key)
                else:
                    print(f"No metadata found for song '{song_title}'.")
        else:
            print("No songs found in the bucket.")
    except Exception as e:
        print(f"Error fetching songs from S3: {e}")


def main():
    while True:
        print("\nOptions:")
        print("1. Download all songs by an artist")
        print("2. Fetch all songs from S3 bucket and index in Elasticsearch")
        print("3. Download song by name")
        print("Type 'exit' to quit.")

        choice = input("Choose an option: ")

        if choice.lower() == 'exit':
            print("Exiting program.")
            break

        if choice == '1':
            artist_name = input("Enter an artist name to download all their songs: ")
            download_and_upload_songs_by_artist(artist_name)
        elif choice == '2':
            fetch_and_index_songs_from_bucket()
        elif choice =='3':
            song = input("Enter name of song")
            download_and_upload_song(song)
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
