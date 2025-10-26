# PLEASE, USE LAZY IMPORT FOR THIS MODULE IN CRITICAL POINTS 'CAUSE IT HAS MANY DEPENDENCIES ON HEAVY LIBRARIES
import os
import io
from datetime import date
import librosa
import numpy as np
import musan
import logging

# LOGGING
if not os.path.exists("logs"):
    os.makedirs("logs")

DATE_FORMAT = "%d-%m-%Y"
file_log = logging.FileHandler(f"logs/audioworker-{date.today().strftime(DATE_FORMAT)}.log")
console_out = logging.StreamHandler()

logging.basicConfig(handlers=(file_log, console_out), level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Тоники (храним только индексы, где 0 = C, 1 = C#, ..., 11 = B)
keys = ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B']

# Усложненные профили для мажорной и минорной тональностей (требуют нормализации)
major_profile = [6.35, 2.23, 3.48, 2.33, 4.38, 4.09, 2.52, 5.19, 2.39, 3.66, 2.29, 2.88]
minor_profile = [6.33, 2.68, 3.52, 5.38, 2.60, 3.53, 2.54, 4.75, 3.98, 2.69, 3.34, 3.17]

# Нормализуем профили
major_profile = np.array(major_profile) / np.sum(major_profile)
minor_profile = np.array(minor_profile) / np.sum(minor_profile)

# Упрощённые профили для мажорной и минорной тональностей (как пример, не используется)
simple_major_profile = [1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1]
simple_minor_profile = [1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 0]

logging.info("Loading Musan pre-trained models...")
happy_sad_classifier, relaxing_energetic_classifier = musan.load_pretraned_models()
logging.info("Musan pre-trained models loaded.")

def extract_features(audio_bytes: io.BytesIO, uuid=None) -> dict:
    logging.info(f"Loading audio for uuid: {uuid}")
    audio, sr = librosa.load(audio_bytes)

    logging.info(f"Extracting tempo for uuid: {uuid}")
    tempo, _ = librosa.beat.beat_track(y=audio, sr=sr)
    logging.info(f"Extracted {tempo} for uuid: {uuid}")
    
    logging.info(f"Extracting loudness for uuid: {uuid}")
    loudness = extract_loudness(audio, sr)
    logging.info(f"Extracted loudness for uuid: {uuid}")

    logging.info(f"Extracting key and mode for uuid: {uuid}")
    key, mode = extract_key_mode(audio, sr)
    logging.info(f"Extracted key: {keys[key]}, mode: {'major' if mode == 1 else 'minor'} for uuid: {uuid}")

    logging.info(f"Extracting happyness and energetic for uuid: {uuid}")
    audio_bytes.seek(0)
    happyness, energetic = extract_happyness_energetic(audio_bytes)
    logging.info(f"Extracted happyness: {happyness}, energetic: {energetic} for uuid: {uuid}")

    if not happyness:
        logging.warning(f"Happyness extraction failed for uuid: {uuid}, setting default value 0.5")
        happyness = 0.5

    if not energetic:
        logging.warning(f"Energetic extraction failed for uuid: {uuid}, setting default value 0.5")
        energetic = 0.5

    track_features = {
        "tempo": int(tempo[0]),
        "happyness": float(happyness),
        "energetic": float(energetic),
        "rms_mean": float(loudness['rms_mean']),
        "rms_max": float(loudness['rms_max']),
        "loudness_db": float(loudness['loudness_db']),
        "true_peak_db": float(loudness['true_peak_db']),
        "key": int(key),
        "mode": int(mode)
    }

    return track_features

def extract_loudness(audio, sr=22050):
    # RMS (Root Mean Square) - простая мера громкости
    rms = librosa.feature.rms(y=audio)
    rms_mean = np.mean(rms)
    rms_max = np.max(rms)
    
    # LUFS-подобная громкость - используем оконное преобразование
    frame_length = 2048
    hop_length = 512
    
    S = np.abs(librosa.stft(y=audio, n_fft=frame_length, hop_length=hop_length))
    power = S**2
    
    # Имитируем K-weighting как в LUFS
    loudness = librosa.perceptual_weighting(power, frequencies=librosa.fft_frequencies(sr=sr))
    loudness_db = np.mean(loudness)
    
    # Пиковая громкость (True Peak)
    peak = np.max(np.abs(audio))
    peak_db = 20 * np.log10(peak) if peak > 0 else -np.inf
    
    return {
        'rms_mean': float(rms_mean),
        'rms_max': float(rms_max),
        'loudness_db': float(loudness_db),
        'true_peak_db': float(peak_db)
    }

def extract_key_mode(audio, sr=22050):
    """
    Определяет тональность и режим (мажор/минор) трека с помощью анализа хроматических коэффициентов.
    :returns: Tuple[key (0-11), mode (0 = минор, 1 = мажор)]
    """
    chroma = librosa.feature.chroma_stft(y=audio, sr=sr)    # Вычисляем хроматические коэффициенты (12 тонов)
    chroma_mean = np.mean(chroma, axis=1)                   # Усредняем амплитуды по времени
    key = np.argmax(chroma_mean)                            # Полагаем, что тональность - максимальный по средней амплитуде коэффициент

    correlation_major = []
    correlation_minor = []

    # Умножаем имеющиеся хроматические коэффициенты на циклически сдвинутые профили
    # Полагаем, что наибольшая корреляция укажет на мажор/минор
    for i in range(12):
        # Циклический сдвиг профилей
        shifted_major = np.roll(major_profile, i)
        shifted_minor = np.roll(minor_profile, i)

        # Вычисляем корреляцию
        corr_major = np.corrcoef(chroma_mean, shifted_major)[0, 1]
        corr_minor = np.corrcoef(chroma_mean, shifted_minor)[0, 1]

        correlation_major.append(corr_major)
        correlation_minor.append(corr_minor)

    mode = 1 if np.max(correlation_major) >= np.max(correlation_minor) else 0

    print("Analyzed key:", keys[key], '' if mode == 1 else 'm')

    return key, mode


def extract_happyness_energetic(audio_bytes):
    result = musan.predict(
        audio_bytes,
        happy_sad_classifier,
        relaxing_energetic_classifier
    )

    return result["happy"], result["energetic"]
