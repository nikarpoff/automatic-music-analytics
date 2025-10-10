import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REDIRECT_URI = "http://127.0.0.1:8443"

# Scope нужен, чтобы иметь доступ к плейлистам
SCOPE = "playlist-read-private"

# Инициализация Spotipy с OAuth
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    scope=SCOPE
))

# ID топ-плейлиста Spotify
TOP_50_GLOBAL_PLAYLIST_ID = "37i9dQZEVXbLiRSasKsNU9"

# Получаем данные плейлиста
playlist = sp.playlist(TOP_50_GLOBAL_PLAYLIST_ID)

# Преобразуем в DataFrame
tracks = playlist['tracks']['items']
data = []
for item in tracks:
    track = item['track']
    data.append({
        'name': track['name'],
        'artist': ', '.join([a['name'] for a in track['artists']]),
        'popularity': track['popularity'],
        'spotify_url': track['external_urls']['spotify']
    })

df = pd.DataFrame(data)
print(df.head())
