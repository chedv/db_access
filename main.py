from database import engine, cinema, cinema_session, film

from sqlalchemy import select
import pandas as pd

import datetime


def get_cinema_id(connection, cinema_name):
    result = connection.execute(cinema.select().where(cinema.c.cinema_name == cinema_name))
    cinema_row = result.fetchone()
    return cinema_row[0] if cinema_row else None


def extend_with_start_date(series):
    return pd.Series([time_start.date() for time_start in series.session_start])


def extend_with_end_time(series):
    return pd.Series([time_start + datetime.timedelta(minutes=film_duration)
                      for time_start, film_duration in zip(series.session_start, series.film_duration)])


def check_mistakes_in_sessions(series):
    for current_start, end in zip(series.session_start, series.session_end):
        actual_starts = [start for start in series.session_start if start > current_start]
        if not actual_starts:
            return True
        return min(actual_starts) >= end


def show_mistakes_in_sessions(cinema_name):
    with engine.connect() as connection:
        cinema_id = get_cinema_id(connection, cinema_name)
        if not cinema_id:
            raise ValueError(f'Cinema with name "{cinema_name}" was not found')

        output_columns = [
            cinema_session.c.session_place,
            cinema_session.c.session_start,
            film.c.film_duration
        ]

        join_stmt = cinema_session.join(film, cinema_session.c.film_id == film.c.id)
        select_stmt = select(output_columns).select_from(join_stmt)

        result = connection.execute(select_stmt)
        rows = result.fetchall()

        session_film_df = pd.DataFrame(rows, columns=[col.key for col in output_columns])
        session_film_df = session_film_df.assign(session_start_date=extend_with_start_date)
        session_film_df = session_film_df.assign(session_end=extend_with_end_time)

        session_film_df = session_film_df.groupby(
            ['session_place', 'session_start_date']
        ).apply(check_mistakes_in_sessions).to_frame('is_valid')

        return all(session_film_df.is_valid)


if __name__ == '__main__':
    cinema_name = input('Input cinema name: ')

    result_msg = 'valid' if show_mistakes_in_sessions(cinema_name) else 'invalid'
    print(f'All sessions for cinema "{cinema_name}" are {result_msg}')
