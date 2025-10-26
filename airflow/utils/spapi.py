import os
from dotenv import load_dotenv

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


class SpotifyAPI:
    def __init__(self):
        load_dotenv()

        self.client = spotipy.Spotify(
            auth_manager=SpotifyClientCredentials(
                client_id=os.environ.get("SPOTIFY_CLIENT_ID"),
                client_secret=os.environ.get("SPOTIFY_CLIENT_SECRET")
            )
        )

    def search_track(self, track_title, author):
        return self.client.search(
            q=f"track: {track_title}, author: {author}",
            type="track",
            limit=1
        )
