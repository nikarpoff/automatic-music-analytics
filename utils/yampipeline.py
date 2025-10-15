# THIS IS ONLY EXAMPLE FOR LOCAL TESTS. THIS CODE CAN NOT BE USED IN AIRFLOW PIPELINES.

from utils.yamapi import YaMusicAPI
from utils.feature_extractor import extract_features
from utils.database import *

yamapi = YaMusicAPI()

def load_chart():
    chart = yamapi.load_chart().get_tracks()

    for track in chart:
        track_db = read_track_from_db(track.id)
        if track_db:
            print(f"Track {track_db} already in database!")
        else:
            audio, sr = yamapi.load_audio(track.id)
            features = extract_features(audio, sr)

            write_track_into_db(track.id, features)

        break

    show_db()

if __name__ == "__main__":
    load_chart()