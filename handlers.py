from datetime import timedelta
import pandas as pd


class PandasHandlers:
    @staticmethod
    def extend_with_start_date(series):
        return pd.Series([time_start.date() for time_start in series.session_start])

    @staticmethod
    def extend_with_end_time(series):
        return pd.Series([time_start + timedelta(minutes=film_duration)
                          for time_start, film_duration in zip(series.session_start, series.film_duration)])

    @staticmethod
    def check_mistakes_in_sessions(series):
        for current_start, end in zip(series.session_start, series.session_end):
            if len(series.session_start) != len(set(series.session_start)):
                return False
            actual_starts = [start for start in series.session_start if start > current_start]
            if not actual_starts:
                return True
            return min(actual_starts) >= end
