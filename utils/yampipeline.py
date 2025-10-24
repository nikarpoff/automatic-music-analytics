# THIS IS ONLY EXAMPLE FOR LOCAL TESTS. THIS CODE CAN NOT BE USED IN AIRFLOW PIPELINES.

from utils.yamapi import YaMusicAPI
from utils.database import *

yamapi = YaMusicAPI()
db = TracksDatabaseAdapter()

def load_chart():
    chart = yamapi.load_chart()

    for track in chart:
        track_db = db.get_track(track.id)

        if track_db:
            track_db = track_db[0]
            print(f"Track {track_db} already in database!")

            data = dict(track_db)

            print(data)

            track_id = data.get("id")
            db.update_last_hit_date(track_id)
        else:
            from utils.feature_extractor import extract_features

            audio_bytes = yamapi.load_audio_bytes(track.id)
            track_features = extract_features(audio_bytes)

            db.write_track(track, track_features)

        break

if __name__ == "__main__":
    load_chart()