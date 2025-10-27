# THIS IS ONLY EXAMPLE FOR LOCAL TESTS. THIS CODE CAN NOT BE USED IN AIRFLOW PIPELINES.

from utils.yamapi import YaMusicAPI
from utils.database import *
from utils.featapi import FeaturesExtractorAPI

from utils.datamodel import build_track_features_from_dict

from datetime import datetime
from math import log10


yamapi = YaMusicAPI()
featapi = FeaturesExtractorAPI()
db = TracksDatabaseAdapter()
adb = ChartDatabaseAdapter()

def load_chart():
    chart = yamapi.load_chart()
    tracks_features = []

    for track in chart:
        track_db = db.get_track(track.id)

        if track_db:
            track_db = track_db[0]
            print(f"Track {track_db} already in database!")

            features_dict = dict(track_db)
            track_features = build_track_features_from_dict(features_dict)

            db.update_last_hit_date(track.id)
        else:
            audio_bytes = yamapi.load_audio_bytes(track.id)
            features_dict = featapi.extract_features(audio_bytes)
            track_features = build_track_features_from_dict(features_dict)

            db.write_track(track, track_features)

        tracks_features.append(track_features)
        break
    
    tracks_meta = [chart[0]]

    tracks = []
    chart = []
    authors = []
    authors_tracks = []

    for place, (track_meta, track_features) in enumerate(zip(tracks_meta, tracks_features)):
        authors.extend([
            [
                int(artist.id),
                artist.name
            ] for artist in track_meta.artists
        ])  # append authors

        authors_tracks.extend(
        [    
            [
                int(track_meta.id),
                int(artist.id)
            ] for artist in track_meta.artists
        ]
        )  # append relation M:M authors<->tracks
        
        tracks.append([
            int(track_meta.id),
            track_meta.title,
            track_meta.album.title,
            track_meta.album.genre,
            track_meta.duration,
            track_features.tempo,
            track_features.happyness,
            track_features.energetic,
            track_features.rms_mean,
            track_features.rms_max,
            track_features.loudness_db,
            track_features.true_peak_db,
            track_features.key,
            track_features.mode
        ])  # append track

        score = 1 - log10(place + 1)

        chart.append([
            int(track_meta.id),
            datetime.now().date(),
            place + 1,
            score,
            track_meta.listeners
        ])  # append chart entry

        print(tracks)
        print(authors)
        print(authors_tracks)
        print(chart)

    adb.insert_authors(authors)
    adb.insert_tracks(tracks)
    adb.insert_authors_tracks(authors_tracks)
    adb.insert_chart(chart)

if __name__ == "__main__":
    load_chart()