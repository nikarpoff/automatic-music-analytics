import numpy as np
import pandas as pd

from datetime import date


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

class Track:
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


class Chart:
    def __init__(self, tracks: list[Track], chart_date=None):
        self.date = chart_date
        
        if not self.date:
            self.date = date.today()

        self.tracks = tracks
    
        self.places = np.array(list(range(1, len(self.tracks) + 1)))
        assert len(self.places) == len(self.tracks)

        # Константа для нормировки мест (по умолчанию места чарта в пределах [0; 4])
        k = 4
        self.scores = np.exp(-(self.places / len(tracks)) * k)
    
    def get_tracks(self):
        return self.tracks

    def get_chart_dataframe(self):
        chart_dict = {
            "place": [],
            "track_id": [],
            "title": [],
            "duration": [],
            "genre": [],
            "score": [],
        }
        
        for i, track in enumerate(self.tracks):
            chart_dict["place"].append(i + 1)
            chart_dict["track_id"].append(track.id)
            chart_dict["title"].append(track.title)
            chart_dict["duration"].append(track.duration)
            chart_dict["genre"].append(track.album.genre)
            chart_dict["score"].append(self.scores[i])
        
        return pd.DataFrame.from_dict(chart_dict)

    def get_genres_scores(self):
        df = self.get_chart_dataframe()

        genres_scores = df.groupby("genre")["score"].agg([
            ('total_score', 'sum'),      # сумма score для каждого жанра
            ('tracks_count', 'count')    # количество треков для каждого жанра
        ]).reset_index()

        return genres_scores
