import os
import requests
import io
from dotenv import load_dotenv

class FeaturesExtractorAPI:
    """
    Provides methods to interact with Features Extractor API.
    """
    def __init__(self):
        load_dotenv()
        self.api_url = os.environ.get("FEATURES_EXTRACTOR_API_URL", "http://audioworker:8000/api/v1/features/extract")

    def extract_features(self, audio_bytes: io.BytesIO) -> dict:
        """
        Extract features from audio bytes using Features Extractor API.
        """
        files = {"file": ("audio.mp3", audio_bytes, "audio/mp3")}
        response = requests.post(self.api_url, files=files)
        response.raise_for_status()
        return response.json()
