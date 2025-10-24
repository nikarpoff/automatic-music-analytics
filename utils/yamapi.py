import io

from yandex_music import Client

from utils.datamodel import Artist, TrackMeta, Album


CHART_ID = 'world'


class YaMusicAPI:
    """
    Provides methods to interact with Yandex Music API.
    """
    def __init__(self):
        self.client = Client(None).init()

    def load_chart(self, chart_id=CHART_ID) -> list[TrackMeta]:
        """
        Loads chart (list of popular tracks)
        Each track is only meta information about track, not audio.
        """
        chart = self.client.chart(chart_id).chart
        tracks = []

        for track_short in chart.tracks:
            track = track_short.track

            listeners = track_short.chart.listeners
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

            track = TrackMeta(
                track_id,
                title,
                duration_ms,
                listeners,
                artists,
                album
            )

            tracks.append(track)

        return tracks

    def load_audio_bytes(self, track_id: int):
        """
        Loads raw audio bytes from Yandex Music by track id. 
        :param track_id: integer id of track
        :returns io.BytesIO audio_bytes
        """
        track_download_info = self.client.tracksDownloadInfo(track_id)[0]
        audio_bytes = track_download_info.download_bytes()

        io_audio_bytes = io.BytesIO(audio_bytes)

        return io_audio_bytes
    