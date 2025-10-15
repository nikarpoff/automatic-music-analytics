import librosa


def extract_features(audio, sr=22050):
    # TODO THIS IS ONLY EXAMPLE!
    tempo, _ = librosa.beat.beat_track(y=audio, sr=sr)
    return tempo[0]