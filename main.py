import os
from ytmusicapi import YTMusic
import yt_dlp

# Initialize YTMusic
ytmusic = YTMusic()

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

def download_song(video_id):
    url = f"https://www.youtube.com/watch?v={video_id}"
    ydl_opts = {
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'outtmpl': '%(title)s.%(ext)s'
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])

def main():
    song_name = input("Enter the song name: ")
    song_info = search_and_get_song_info(song_name)
    
    if song_info:
        print(f"Downloading '{song_info['title']}' by {song_info['artist']}")
        print(f"Album: {song_info['album']}")
        download_song(song_info['videoId'])
        print("Download complete!")
    else:
        print("Song not found!")

if __name__ == "__main__":
    main()
