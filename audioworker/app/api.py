import os
import io
import uuid
from datetime import datetime
from fastapi import FastAPI, File, UploadFile
import logging

import app.utils as utils

app = FastAPI()

@app.post("/api/v1/features/extract")
async def predict(file: UploadFile = File(...)):
    request_uuid = uuid.uuid4()

    # Load audio in bytes.
    audio_bytes = await file.read()

    # Log current time.
    start_time = datetime.now()
    logging.info(f"POST: audio bytes length: {len(audio_bytes)}; uuid: {request_uuid}")

    # Extract features.
    audio_bytes = io.BytesIO(audio_bytes)
    features = utils.extract_features(audio_bytes, uuid=request_uuid)

    end_time = datetime.now()
    logging.info(f"All features were extracted! extraction time: {end_time - start_time}; uuid: {request_uuid}")

    return features