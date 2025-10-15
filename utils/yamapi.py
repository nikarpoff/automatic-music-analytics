import io

from yandex_music import Client

from utils.datamodel import *


CHART_ID = 'world'


class YaMusicAPI:
    """
    Provides methods to interact with Yandex Music API.
    """
    def __init__(self):
        self.client = Client(None).init()

    def load_chart(self, chart_id=CHART_ID) -> Chart:
        """
        Loads chart (list of popular tracks)
        Each track is only meta information about track, not audio.
        """
        chart = self.client.chart(chart_id).chart
        tracks = []

        for track_short in chart.tracks:
            track = track_short.track

            title = track.title
            track_id = track.id
            duration_ms = track.duration_ms
            artists = []

            for artist in track.artists:
                artists.append(
                    Artist(artist.id, artist.name)
                )

            album = Album(
                track.albums[0].id,
                track.albums[0].title,
                track.albums[0].genre,
            )

            track = Track(
                track_id,
                title,
                duration_ms,
                artists,
                album
            )

            tracks.append(track)

        return Chart(tracks)

    def load_audio(self, track_id: int):
        """
        Loads raw audio and reads it by librosa. 
        :param track_id: integer id of track in ya.music
        :returns np.ndarray audio, int/float sample rate
        """
        import librosa

        track_download_info = self.client.tracksDownloadInfo(track_id)[0]
        audio_bytes = track_download_info.download_bytes()

        io_audio_bytes = io.BytesIO(audio_bytes)
        audio, sr = librosa.load(io_audio_bytes)

        return audio, sr
    