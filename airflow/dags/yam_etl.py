from datetime import datetime, timedelta
from airflow.sdk import dag, task
import logging

from utils.datamodel import TrackFeatures, TrackMeta, build_track_features_from_dict

@dag(
    dag_id="yam_etl",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 10, 25),
    catchup=False,
    tags=["yandex", "music", "etl", "api"],
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
)
def yam_charts_taskflow():
    """
    ETL DAG to extract, transform, and load Yandex Music charts data.
    """
    from utils.yamapi import YaMusicAPI
    from utils.database import TracksDatabaseAdapter, ChartDatabaseAdapter
    from utils.featapi import FeaturesExtractorAPI

    logger = logging.getLogger("airflow.task")

    tdb = TracksDatabaseAdapter()
    yamapi = YaMusicAPI()
    featapi = FeaturesExtractorAPI()

    @task(retries=3)
    def extract_chart() -> list[TrackMeta]:
        chart = yamapi.load_chart()
        logger.info(f"Extracted Chart with {len(chart)} tracks...")
        return chart
    
    @task(retries=2)
    def extract_features_for_new_tracks(chart: list[TrackMeta]) -> list[TrackFeatures]:
        """
        For each tracks tryies to get it from database.
        If track already exists, only update last_hit_date.
        Else extract features and writes track into db.

        Args:
            chart (list[TrackMeta]): a list of meta information about tracks for which features need to be extracted
        
        Returns:
            tracks_features (list[TrackFeatures]): a list of features for each of tracks meta that were extracted or loaded from db
        """
        logger.info(f"Start processing chart!")

        new_tracks_count = 0
        tracks_features = []

        for track in chart:
            features_db = tdb.get_track(track.id)

            if features_db:
                # Track features already in db 
                logger.info(f"Track {track.id} already in database!")
                features_dict = dict(features_db[0])
                track_features = build_track_features_from_dict(features_dict)

                # Don't forget update dynamic information about track!
                tdb.update_last_hit_date(track.id)
            else:
                # Track features wasn't found -> lets extract features
                audio_bytes = yamapi.load_audio_bytes(track.id)
                features_dict = featapi.extract_features(audio_bytes)
                track_features = build_track_features_from_dict(features_dict)

                tdb.write_track(track, track_features)
                new_tracks_count += 1
            
            tracks_features.append(track_features)
        
        logger.info(f"There {new_tracks_count} tracks were loaded in database!")
        return tracks_features

    @task(retries=3)
    def transform_chart(tracks_meta: list[TrackMeta], tracks_features: list[TrackFeatures]):
        """
        Makes transformation of chart that ready to be loaded in analitycs database
        """
        from datetime import datetime
        from math import log10

        logger.info(f"Start transforming chart!")
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
        
        logger.info(f"Chart transformed with {len(tracks)} tracks,  {len(authors)} authors and {len(chart)} chart records!")
        return chart, tracks, authors, authors_tracks

    @task(retries=3)
    def load_data(chart, tracks, authors, authors_tracks):
        """
        Loads chart, tracks, authors and authors_tracks into database
        """
        adb = ChartDatabaseAdapter()

        logger.info(f"Start loading data into database!")

        adb.insert_authors(authors)
        adb.insert_tracks(tracks)
        adb.insert_authors_tracks(authors_tracks)
        adb.insert_chart(chart)

        adb.close()

        logger.info(f"Data loaded into database successfully!")

    chart = extract_chart()
    tracks_features = extract_features_for_new_tracks(chart)
    chart, tracks, authors, authors_tracks = transform_chart(chart, tracks_features)
    load_data(chart, tracks, authors, authors_tracks)

    logger.info("DAG tasks done!")

yam_charts_taskflow()