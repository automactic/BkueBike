import asyncio
import dataclasses
import logging
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pandas
import sqlalchemy as sa
from aiohttp import ClientSession

import sql
from entities import Region, Station, Trip
from sql import DatabaseMixin
from .base import HTTPSessionMixin

logger = logging.getLogger(__name__)


class StationDataImporter(DatabaseMixin, HTTPSessionMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def run(self):
        while True:
            async with self.create_session() as session:
                stations = await self._fetch_stations(session)
            await self._upsert_stations(stations)
            await asyncio.sleep(timedelta(days=1).total_seconds())

    @staticmethod
    async def _fetch_regions(session: ClientSession) -> {str, Region}:
        url = 'https://gbfs.bluebikes.com/gbfs/en/system_regions.json'
        async with session.get(url) as response:
            response_data = await response.json()

        regions = {}
        for item in response_data.get('data', {}).get('regions', []):
            try:
                region = Region(**{
                    'id': item['region_id'],
                    'name': item['name'],
                })
                regions[region.id] = region
            except (KeyError, TypeError):
                continue
        return regions

    async def _fetch_stations(self, session: ClientSession) -> {str, Station}:
        url = 'https://gbfs.bluebikes.com/gbfs/en/station_information.json'
        async with session.get(url) as response:
            response_data = await response.json()

        regions = await self._fetch_regions(session)
        stations = {}
        for item in response_data.get('data', {}).get('stations', []):
            try:
                region = regions[item['region_id']]
                station = Station(**{
                    'id': item['station_id'],
                    'name': item['name'],
                    'short_name': item['short_name'],
                    'latitude': item['lat'],
                    'longitude': item['lon'],
                    'region_id': region.id,
                    'region_name': region.name,
                    'capacity': item['capacity'],
                    'has_kiosk': item['has_kiosk'],
                })
                stations[station.id] = station
            except (KeyError, TypeError):
                continue
        return stations

    async def _upsert_stations(self, stations: {str, Station}):
        # retrieve ids for all existing stations
        async with self.conn() as conn:
            result = await conn.execute(sa.select([sql.stations.c.id]))
            existing_station_ids = [row.id async for row in result]

        # figure out which stations should be inserted
        new_station_ids = set(stations) - set(existing_station_ids)
        new_stations = {
            station_id: station for station_id, station in stations.items()
            if station_id in new_station_ids
        }

        # insert new station in database
        async with self.conn() as conn:
            for station in new_stations.values():
                statement = sql.stations.insert().values(**dataclasses.asdict(station))
                await conn.execute(statement)

        # logging
        if new_stations:
            logger.info(
                f'Station -- found {len(new_stations)} new stations: {new_stations.keys()}.'
            )
        else:
            logger.info('Station -- no new station found.')


class TripDataCSVColumn:
    TRIP_DURATION = 'tripduration'
    START_STATION_ID = 'start station id'
    END_STATION_ID = 'end station id'
    START_TIME = 'starttime'
    STOP_TIME = 'stoptime'
    BIKE_ID = 'bikeid'
    USER_TYPE = 'usertype'
    USER_BIRTH_YEAR = 'birth year'
    USER_GENDER = 'gender'


class TripDataImporter(DatabaseMixin):
    def __init__(self, path: Path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_name = path.name
        self.data_frame = pandas.read_csv(path)

    async def run(self):
        if await self.is_already_imported():
            return

        logger.info(f'Trip[{self.file_name}] -- Import Started.')
        await self.insert_trips()
        logger.info(f'Trip[{self.file_name}] -- Import Finished.')

    async def is_already_imported(self):
        start_time = self.data_frame[TripDataCSVColumn.START_TIME]
        min_start_time, max_start_time = start_time.min(), start_time.max()

        # get count of trips between start and end date
        async with self.conn() as conn:
            statement = sa.select([
                sa.func.count(sql.trips.c.id).label('count')
            ]).where(sa.and_(
                sql.trips.c.start_time >= min_start_time,
                sql.trips.c.start_time <= max_start_time,
            ))
            result = await conn.execute(statement)
            count = await result.scalar()

        return count >= len(self.data_frame)

    async def insert_trips(self):
        gender_map = {0: 'Male', 1: 'Female'}
        async with self.conn() as conn:
            next_milestone = 0.1
            total_count = len(self.data_frame)
            for index, row in self.data_frame.iterrows():
                trip = Trip(**{
                    'id': str(uuid4()),
                    'trip_duration': row[TripDataCSVColumn.TRIP_DURATION],
                    'start_station_id': row[TripDataCSVColumn.START_STATION_ID],
                    'end_station_id': row[TripDataCSVColumn.END_STATION_ID],
                    'start_time': row[TripDataCSVColumn.START_TIME],
                    'stop_time': row[TripDataCSVColumn.STOP_TIME],
                    'bike_id': row[TripDataCSVColumn.BIKE_ID],
                    'user_type': row[TripDataCSVColumn.USER_TYPE],
                    'user_birth_year': row[TripDataCSVColumn.USER_BIRTH_YEAR],
                    'user_gender': gender_map.get(row[TripDataCSVColumn.USER_GENDER], 'Other'),
                })
                statement = sql.trips.insert().values(**dataclasses.asdict(trip))
                await conn.execute(statement)

                progress = (index + 1) / total_count
                if progress > next_milestone:
                    logger.info((
                        f'Trip[{self.file_name}] -- '
                        f'Import in Progress: {progress:.0%}({index + 1}/{total_count})'
                    ))
