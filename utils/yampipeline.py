# THIS IS ONLY EXAMPLE FOR LOCAL TESTS. THIS CODE CAN NOT BE USED IN AIRFLOW PIPELINES.

from utils.yamapi import YaMusicAPI
from utils.feature_extractor import extract_features
from utils.database import *

yamapi = YaMusicAPI()
db = DatabaseAdapter()

def load_chart():
    chart = yamapi.load_chart().get_tracks()

    for track in chart:
        track_db = db.get_track(track.id)
        print(track_db)

        if track_db:
            print(f"Track {track_db[0]} already in database!")
        else:
            audio, sr = yamapi.load_audio(track.id)
            track_features = extract_features(audio, sr)

            db.write_track(track, track_features)

        break

if __name__ == "__main__":
    load_chart()