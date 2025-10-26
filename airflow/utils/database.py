import os
import psycopg2
import clickhouse_connect

from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

from utils.datamodel import Track, TrackMeta, TrackFeatures


class TracksDatabaseAdapter:
    """
    Database Adapter for PostgreSQL DB with some tracks features.
    Every query creates new connection, cursor and closes them both.
    """
    def __init__(self):
        load_dotenv()
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        host = os.getenv("POSTGRES_HOST")
        port = os.getenv("POSTGRES_PORT")
        self.connection_string = f"postgresql://{user}:{password}@{host}:{port}/tracks"
        self._create_tables_if_not_exists()

    def _get_connection(self):
        return psycopg2.connect(self.connection_string, cursor_factory=RealDictCursor)

    def _execute_query(self, query: str, params: tuple = None, fetch: bool = False):
        """
        Method that gets connection, cursor, executes query, returns result and closes connection, cursor.
        
        Args:
            query (str): query to be executed, values are masked as %s and will be replaced by params in order.
            params (tuple): params that can replace variables in query
            fetch (bool): if true, it will return the query result.
        """
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
                first_hit DATE DEFAULT CURRENT_DATE,
                last_hit DATE DEFAULT CURRENT_DATE
            )
        """
        )

    def get_track(self, track_id):
        return self._execute_query(
            query="SELECT * FROM tracks WHERE id=%s", params=(track_id,),
            fetch=True
        )
    
    def write_track(self, track: TrackMeta, features: TrackFeatures):
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

    def update_last_hit_date(self, track_id):
        """
        Update last hit date of track with track_id with current date
        """
        self._execute_query(
            query="""
            UPDATE tracks
            SET last_hit = CURRENT_DATE
            WHERE id = %s
            """,
            params=(
                track_id,
            )
        )

class ChartDatabaseAdapter:
    def __init__(self):
        load_dotenv()
        host = os.environ.get("CLICKHOUSE_HOST")
        port = os.environ.get("CLICKHOUSE_PORT")
        user = os.environ.get("CLICKHOUSE_USER")
        password = os.environ.get("CLICKHOUSE_PASSWORD")

        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database="charts"
        )

        self._create_tables_if_not_exists()
    
    def _create_tables_if_not_exists(self):
        self.client.command("""
        CREATE TABLE IF NOT EXISTS charts.tracks_authors (
            track_id UInt32,
            author_id UInt32,
        ) ENGINE = MergeTree()
        ORDER BY (track_id, author_id)
        """
        )

        self.client.command("""
        CREATE TABLE IF NOT EXISTS charts.tracks (
            track_id UInt32,
            title LowCardinality(String),
            album LowCardinality(String),
            genre LowCardinality(String),
            duration UInt32,
            tempo UInt8,
            happyness Float32,
            energetic Float32,
            rms_mean Float32,
            rms_max Float32,
            loudness Float32,
            true_peak_db Float32,
            key UInt8,
            mode UInt8
        ) ENGINE = MergeTree()
        ORDER BY (track_id)
        """
        )

        self.client.command("""
        CREATE TABLE IF NOT EXISTS charts.authors (
            author_id UInt32,
            author String,
        ) ENGINE = MergeTree()
        ORDER BY (author_id)
        """
        )

        self.client.command("""
        CREATE TABLE IF NOT EXISTS charts.charts (
            track_id UInt32,
            date Date,
            place UInt8,
            score Float32,
            listeners UInt32,
        ) ENGINE = MergeTree()
        ORDER BY (track_id, date)
        """
        )

        # if not any(idx['name'] == 'idx_genre' for idx in self.client.query('SHOW INDEXES FROM charts.tracks').result_rows):
        #     self.client.command("ALTER TABLE charts.tracks ADD INDEX idx_genre genre TYPE set(100) GRANULARITY 1")
        
        # if not any(idx['name'] == 'idx_track' for idx in self.client.query('SHOW INDEXES FROM charts.tracks').result_rows):
        #     self.client.command("ALTER TABLE charts.tracks ADD INDEX idx_track track_id TYPE minmax GRANULARITY 1")

        # if not any(idx['name'] == 'idx_track' for idx in self.client.query('SHOW INDEXES FROM charts.chart').result_rows):
        #     self.client.command("ALTER TABLE charts.charts ADD INDEX idx_track track_id TYPE minmax GRANULARITY 1")
        
        # if not any(idx['name'] == 'idx_date' for idx in self.client.query('SHOW INDEXES FROM charts.chart').result_rows):
        #     self.client.command("ALTER TABLE charts.charts ADD INDEX idx_date date TYPE minmax GRANULARITY 1")
    
    def close(self):
        self.client.close()

    def insert_chart(self, chart: list[list]):
        self.client.insert(
            table="charts",
            data=chart,
            column_names=[
                "track_id",
                "date",
                "place",
                "score",
                "listeners"
            ]
        )
    
    def insert_authors(self, authors: list[list]):
        self.client.command("CREATE TEMPORARY TABLE temp_authors AS charts.authors")

        self.client.insert(
            table="temp_authors",
            data=authors,
            column_names=[
                "author_id",
                "author"
            ]
        )

        self.client.command("""
        INSERT INTO charts.authors (author_id, author)
        SELECT author_id, author
        FROM temp_authors
        LEFT ANTI JOIN charts.authors AS existing USING (author_id)
        """)

    def insert_tracks(self, tracks: list[list]):
        self.client.command("CREATE TEMPORARY TABLE temp_tracks AS charts.tracks")

        self.client.insert(
            table="temp_tracks",
            data=tracks,
            column_names=[
                "track_id",
                "title",
                "album",
                "genre",
                "duration",
                "tempo",
                "happyness",
                "energetic",
                "rms_mean",
                "rms_max",
                "loudness",
                "true_peak_db",
                "key",
                "mode"
            ]
        )

        self.client.command("""
        INSERT INTO charts.tracks (track_id, title, album, genre, duration, tempo, happyness, energetic, rms_mean, rms_max, loudness, true_peak_db, key, mode)
        SELECT track_id, title, album, genre, duration, tempo, happyness, energetic, rms_mean, rms_max, loudness, true_peak_db, key, mode
        FROM temp_tracks
        LEFT ANTI JOIN charts.tracks AS existing USING (track_id)
        """)

    def insert_authors_tracks(self, authors_tracks: list[list]):
        self.client.command("CREATE TEMPORARY TABLE temp_tracks_authors AS charts.tracks_authors")

        self.client.insert(
            table="temp_tracks_authors",
            data=authors_tracks,
            column_names=[
                "track_id",
                "author_id"
            ]
        )

        self.client.command("""
        INSERT INTO charts.tracks_authors (track_id, author_id)
        SELECT track_id, author_id
        FROM temp_tracks_authors
        LEFT ANTI JOIN charts.tracks_authors AS existing USING (track_id, author_id)
        """)
