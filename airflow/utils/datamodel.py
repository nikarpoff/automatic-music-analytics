class Artist:
    def __init__(self, id, name):
        self.id = id
        self.name = name
    
    def __repr__(self):
        return f"Author '{self.name}' with id: {self.id}"

class Album:
    def __init__(self, id, title, genre):
        self.id = id
        self.title = title
        self.genre = genre
    
    def __repr__(self):
        return f"Album '{self.title}' in genre {self.genre} with id: {self.id}"

class TrackMeta:
    """
    Структура для хранения мета-информации о треке и некоторых характеристик аудио
    """
    def __init__(self, id, title, duration, listeners, artists: list[Artist], album: Album):
        self.id = id
        self.title = title
        self.duration = duration
        self.listeners = listeners
        self.artists = artists
        self.album = album

    def __repr__(self):
        return f"Track '{self.title}' with id: {self.id} and duration {self.duration} ms\n\t Artists: {self.artists}\n\t Album: {self.album}"

class TrackFeatures:
    """
    Структура для хранения характеристик аудио
    """
    def __init__(self, tempo: int, happyness: float, energetic: float, rms_mean: float, rms_max: float,
                 loudness_db: float, true_peak_db: float, key: int, mode: int):
        """
        :param tempo: Темп трека (BPM)
        :param happyness: Уровень "позитивности" трека (от 0 до 1)
        :param energetic: Уровень "энергичности" трека (от 0 до 1)
        :param rms_mean: Максимальный Root Mean Square сигнала трека
        :param rms_max: Максимальный Root Mean Square сигнала трека
        :param loudness: Воспринимаемая громкость трека в дБ (подобие LUFS)
        :param true_peak_db: Пик громкости трэка в дБ
        :param key: Тоника трека (от 0 до 11, где 0 = C, 1 = C#, ..., 11 = B)
        :param mode: Окрас трека (0 = минор, 1 = мажор)
        """
        self.tempo = tempo
        self.happyness = happyness
        self.energetic = energetic
        self.rms_mean = rms_mean
        self.rms_max = rms_max
        self.loudness_db = loudness_db
        self.true_peak_db = true_peak_db
        self.key = key
        self.mode = mode

def build_track_features_from_dict(features: dict) -> TrackFeatures:
    return TrackFeatures(
        tempo=features.get("tempo"),
        happyness=features.get("happyness"),
        energetic=features.get("energetic"),
        rms_mean=features.get("rms_mean"),
        rms_max=features.get("rms_max"),
        loudness_db=features.get("loudness_db"),
        true_peak_db=features.get("true_peak_db"),
        key=features.get("key"),
        mode=features.get("mode"),
    )
