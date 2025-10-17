import librosa
import numpy as np

from utils.datamodel import TrackFeatures


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


def extract_features(audio, sr=22050):
    tempo, _ = librosa.beat.beat_track(y=audio, sr=sr)

    loudness = extract_loudness(audio, sr)
    key, mode = extract_key_mode(audio, sr)
    happyness, energetic = extract_happyness_energetic(audio, sr)

    track_features = TrackFeatures(
        int(tempo[0]),
        float(happyness),
        float(energetic),
        float(loudness['rms_mean']),
        float(loudness['rms_max']),
        float(loudness['loudness_db']),
        float(loudness['true_peak_db']),
        int(key),
        int(mode)
    )

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

def extract_happyness_energetic(audio, sr=22050):
    # TODO: extract happyness and energetic
    return 0.5, 0.5
