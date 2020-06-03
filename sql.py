from contextlib import asynccontextmanager

import sqlalchemy as sa
from aiopg.sa import create_engine, SAConnection
from sqlalchemy import Table, Column, Integer, Float, String, Boolean, MetaData, ForeignKey
import typing

metadata = MetaData()

stations = Table(
    'stations', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, nullable=False),
    Column('short_name', String, nullable=False),
    Column('latitudes', Float, nullable=False),
    Column('longitudes', Float, nullable=False),
    Column('region_id', Integer, nullable=False),
    Column('region_name', String, nullable=False),
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
    Column('bike_id', Integer, nullable=False),
    Column('user_type', String, nullable=False),
    Column('user_birth_year', Integer, nullable=False),
    Column('user_gender', Integer, nullable=False),
    Column('submitted_actual', Boolean, nullable=False, default=False),
)


def initialize():
    conn = DatabaseMixin.create_engine(database='postgres').connect()
    conn.execute("commit")
    conn.execute("CREATE DATABASE blue_bike")
    conn.close()

    metadata.create_all(DatabaseMixin.create_engine())


class DatabaseMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def create_engine(database: str = 'blue_bike') -> sa.engine.Engine:
        return sa.create_engine(f'postgresql://postgres:password@localhost/{database}')

    @asynccontextmanager
    async def conn(self, database: str = 'blue_bike') -> typing.AsyncContextManager[SAConnection]:
        async with create_engine(f'postgresql://postgres:password@localhost/{database}') as engine:
            async with engine.acquire() as conn:
                yield conn
