import pandas as pd

from database import engine, cinema, cinema_session, film
from dao import CinemaDAO, CinemaSessionDAO
from handlers import PandasHandlers


def get_session_films_rows(cinema_name, output_columns):
    rows = []

    with engine.connect() as connection:
        cinema_dao = CinemaDAO(cinema, connection)
        cinema_session_dao = CinemaSessionDAO(cinema_session, connection)

        cinema_id = cinema_dao.get_cinema_id(cinema_name)
        if not cinema_id:
            raise ValueError(f'Cinema with name "{cinema_name}" was not found')

        rows = cinema_session_dao.get_cinema_sessions(cinema_id, output_columns, to_join=(film, 'film_id', 'id'))

    return rows


def get_mistakes_count_in_sessions(cinema_name):
    output_columns = [
        cinema_session.c.session_place,
        cinema_session.c.session_start,
        film.c.film_duration
    ]

    rows = get_session_films_rows(cinema_name, output_columns)

    session_film_df = pd.DataFrame(rows, columns=[col.key for col in output_columns])
    session_film_df = session_film_df.assign(session_start_date=PandasHandlers.extend_with_start_date)
    session_film_df = session_film_df.assign(session_end=PandasHandlers.extend_with_end_time)

    session_film_df = session_film_df.groupby(['session_place', 'session_start_date'])
    session_film_df = session_film_df.apply(PandasHandlers.check_mistakes_in_sessions).to_frame('is_valid')

    return len(session_film_df) - session_film_df.is_valid.sum()


if __name__ == '__main__':
    input_name = input('Input cinema name: ')
    mistakes_count = get_mistakes_count_in_sessions(input_name)

    if mistakes_count > 0:
        print(f'Invalid sessions for cinema "{input_name}" count: {mistakes_count}')
    else:
        print(f'All sessions for cinema "{input_name}" are valid')
