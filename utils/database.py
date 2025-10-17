import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

from utils.datamodel import Track, TrackFeatures


class DatabaseAdapter:
    def __init__(self):
        load_dotenv()
        self.connection_string = os.getenv("POSTGRES_CONNECTION_URL")
        self._create_tables_if_not_exists()

    def _get_connection(self):
        return psycopg2.connect(self.connection_string, cursor_factory=RealDictCursor)

    def _execute_query(self, query: str, params: tuple = None, fetch: bool = False):
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)

                if fetch:
                    return cursor.fetchall()
                
            conn.commit()
    
    def _create_tables_if_not_exists(self):
        self._execute_query(
            query="""
            CREATE TABLE IF NOT EXISTS tracks (
                id SERIAL PRIMARY KEY,
                title VARCHAR(64) NOT NULL,
                duration INTEGER,
                tempo INTEGER,
                energetic DOUBLE PRECISION,
                happyness DOUBLE PRECISION,
                rms_mean DOUBLE PRECISION,
                rms_max DOUBLE PRECISION,
                loudness_db DOUBLE PRECISION,
                true_peak_db DOUBLE PRECISION,
                key INTEGER,
                mode INTEGER,
                last_hit DATE DEFAULT CURRENT_DATE
            )
        """
        )

    def get_track(self, track_id):
        return self._execute_query(
            query="SELECT * FROM tracks WHERE id=%s", params=(track_id,),
            fetch=True
        )
    
    def write_track(self, track: Track, features: TrackFeatures):
        self._execute_query(
            query="""
            INSERT INTO tracks (
                id,
                title,
                duration,
                tempo,
                energetic,
                happyness,
                rms_mean,
                rms_max,
                loudness_db,
                true_peak_db,
                key,
                mode
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            params=(
                track.id,
                track.title,
                track.duration,
                features.tempo,
                features.energetic,
                features.happyness,
                features.rms_mean,
                features.rms_max,
                features.loudness_db,
                features.true_peak_db,
                features.key,
                features.mode
            )
        )
