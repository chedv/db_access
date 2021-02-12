from sqlalchemy import create_engine, Table, MetaData, Column, ForeignKey, Integer, String, DateTime, Numeric
from config import db_config


metadata = MetaData()

engine = create_engine('postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'.format(**db_config))

cinema = Table('cinema', metadata,
               Column('id', Integer, primary_key=True),
               Column('cinema_name', String, unique=True, nullable=False),
               Column('cinema_address', String, unique=True, nullable=False))

film = Table('film', metadata,
             Column('id', Integer, primary_key=True),
             Column('film_name', String, unique=True, nullable=False),
             Column('film_duration', Integer, nullable=False))

cinema_session = Table('cinema_session', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('session_place', String, nullable=False),
                       Column('session_start', DateTime, nullable=False),
                       Column('session_price', Numeric(5, 2), nullable=False),
                       Column('cinema_id', Integer, ForeignKey('cinema.id')),
                       Column('film_id', Integer, ForeignKey('film.id')))

client = Table('client', metadata,
               Column('id', Integer, primary_key=True),
               Column('first_name', String, unique=True, nullable=False),
               Column('last_name', String, unique=True, nullable=False),
               Column('email', String, unique=True, nullable=False))

session_client = Table('session_client', metadata,
                       Column('session_id', Integer, ForeignKey('cinema_session.id'), primary_key=True),
                       Column('client_id', Integer, ForeignKey('client.id'), primary_key=True))
