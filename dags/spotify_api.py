import os
import requests
from dotenv import load_dotenv

from datetime import datetime, timedelta
from airflow.sdk import dag, task


def get_access_token(client_id, client_secret):
    auth_url = "https://accounts.spotify.com/api/token"

    auth_response = requests.post(auth_url, {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    })

    print(auth_response)

    auth_response_data = auth_response.json()
    return auth_response_data.get('access_token', None)


@dag(
    dag_id="spotify_api",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 10, 10),
    catchup=False,
    tags=["spotify", "api"],
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
def spotify_charts_taskflow():
    """
    ETL DAG to extract, transform, and load Spotify charts data.
    """
    @task(retries=1)
    def extract_chart():
        # Import high-cost libraries
        import pandas as pd

        load_dotenv()
        CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
        CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

        print(CLIENT_ID)

        # Try to get an access token
        access_token = get_access_token(CLIENT_ID, CLIENT_SECRET)

        if not access_token:
            raise Exception("Failed to obtain access token from Spotify API")

        # Try to get chart data
        url = "https://spotifycharts.com/regional/global/daily/latest/download"
        response = requests.get(url, {
            "Authorization": f"Bearer {access_token}"
        })

        data = response.content.decode("utf-8")
        df = pd.read_csv(pd.compat.StringIO(data), skiprows=1)
        
        print("Data extracted successfully.")
        print(df.head())
        return df
    
    @task()
    def transform_chart(df):
        df.columns = [col.lower().replace(" ", "_") for col in df.columns]
        df['position'] = df['position'].astype(int)
        df['streams'] = df['streams'].str.replace(',', '').astype(int)
        df['date'] = datetime.now().date()
        return df
    
    @task()
    def load_chart(df):
        from sqlalchemy import create_engine

        engine = create_engine('sqlite:///spotify_charts.db')
        df.to_sql('spotify_charts', con=engine, if_exists='append', index=False)
        print("Data loaded successfully.")

    df = extract_chart()


spotify_charts_taskflow()