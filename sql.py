import os
import typing
from contextlib import asynccontextmanager

import sqlalchemy as sa
from aiopg.sa import create_engine, SAConnection
from sqlalchemy import Table, Column, Integer, Float, String, Boolean, DateTime, MetaData, \
    ForeignKey

metadata = MetaData()

stations = Table(
    'stations', metadata,
    Column('id', String, primary_key=True),
    Column('name', String, nullable=False),
    Column('latitude', Float, nullable=False),
    Column('longitude', Float, nullable=False),
    Column('region_name', String),
    Column('capacity', Integer),
    Column('has_kiosk', Boolean),
)

trips = Table(
    'trips', metadata,
    Column('id', String, primary_key=True),
    Column('trip_duration', Float, nullable=False),
    Column('predicted_trip_duration', Float, nullable=True),
    Column('start_station_id', None, ForeignKey('stations.id')),
    Column('end_station_id', None, ForeignKey('stations.id')),
    Column('start_time', DateTime, nullable=False),
    Column('stop_time', DateTime, nullable=False),
    Column('start_time_normalized', DateTime, nullable=True),
    Column('stop_time_normalized', DateTime, nullable=True),
    Column('bike_id', Integer, nullable=False),
    Column('user_type', String, nullable=False),
    Column('user_birth_year', Integer, nullable=False),
    Column('user_gender', String, nullable=False),
    Column('submitted_actual', Boolean, nullable=False, default=False),
)


def create_database():
    try:
        conn = DatabaseMixin().create_engine(database='postgres').connect()
        conn.execute("commit")
        conn.execute("CREATE DATABASE blue_bike")
        conn.close()
    except Exception:
        pass


def create_tables():
    metadata.create_all(DatabaseMixin().create_engine())


class DatabaseMixin:
    hostname = os.getenv('POSTGRES_HOSTNAME')
    port = os.getenv('POSTGRES_PORT')
    username = os.getenv('POSTGRES_USERNAME')
    password = os.getenv('POSTGRES_PASSWORD')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not all([self.hostname, self.port, self.username, self.password]):
            raise ValueError(
                'Missing POSTGRES environment variables',
                self.hostname, self.port, self.username, self.password
            )

    def _dsn(self, database: str = 'blue_bike'):
        return f'postgresql://{self.username}:{self.password}' \
               f'@{self.hostname}:{self.port}/{database}'

    def create_engine(self, database: str = 'blue_bike') -> sa.engine.Engine:
        return sa.create_engine(self._dsn(database))

    @asynccontextmanager
    async def conn(self, database: str = 'blue_bike') -> typing.AsyncContextManager[SAConnection]:
        async with create_engine(self._dsn(database)) as engine:
            async with engine.acquire() as conn:
                yield conn
