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
                bpm INTEGER,
                energetic FLOAT,
                happyness FLOAT,
                last_hit DATE DEFAULT CURRENT_DATE
            )
        """
        )

    def get_track(self, track_id):
        return self._execute_query(
            query=f"SELECT * FROM tracks WHERE id={track_id}",
            fetch=True
        )
    
    def write_track(self, track: Track, features: TrackFeatures):
        self._execute_query(
            query=f"""
            INSERT INTO tracks (
                id,
                title,
                duration,
                bpm,
                energetic,
                happyness
            ) VALUES (
                {track.id},
                '{track.title}',
                {track.duration},
                {features.bpm},
                {features.energetic},
                {features.happyness}
            )
            """
        )
