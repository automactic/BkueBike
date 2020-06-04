import dataclasses
import logging
from pathlib import Path
from uuid import uuid4

import pandas
import sqlalchemy as sa
from aiohttp import ClientSession

import sql
from entities import Region, Station
from sql import DatabaseMixin
from .base import HTTPSessionMixin

logger = logging.getLogger(__name__)


class StationDataImporter(DatabaseMixin, HTTPSessionMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def run(self):
        async with self.create_session() as session:
            stations = await self._fetch_stations(session)
        await self._upsert_stations(stations)

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
                    'latitude': item['lat'],
                    'longitude': item['lon'],
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
        new_station_ids = set(stations.keys()) - set(existing_station_ids)
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
    START_STATION_NAME = 'start station name'
    START_STATION_LATITUDE = 'start station latitude'
    START_STATION_LONGITUDE = 'start station longitude'
    END_STATION_ID = 'end station id'
    END_STATION_NAME = 'end station name'
    END_STATION_LATITUDE = 'end station latitude'
    END_STATION_LONGITUDE = 'end station longitude'
    START_TIME = 'starttime'
    STOP_TIME = 'stoptime'
    BIKE_ID = 'bikeid'
    USER_TYPE = 'usertype'
    USER_BIRTH_YEAR = 'birth year'
    USER_GENDER = 'gender'


class TripDataImporter(StationDataImporter):
    def __init__(self, path: Path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_name = path.name
        self.data_frame = pandas.read_csv(path)

    async def run(self):
        if await self.is_already_imported():
            logger.info(f'Trip[{self.file_name}] -- Already Imported.')
            return

        logger.info(f'Trip[{self.file_name}] -- Import Started.')
        await self.insert_stations()
        self.insert_trips()
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

    def _extract_stations(self, id_column, name_column, latitude_column, longitude_column) -> {str, Station}:
        grouped = self.data_frame.groupby([id_column]).first()
        return {
            str(station_id): Station(**{
                'id': str(station_id),
                'name': data[name_column],
                'latitude': data[latitude_column],
                'longitude': data[longitude_column],
            }) for station_id, data in grouped.iterrows()
        }

    async def insert_stations(self):
        stations = self._extract_stations(
            id_column=TripDataCSVColumn.START_STATION_ID,
            name_column=TripDataCSVColumn.START_STATION_NAME,
            latitude_column=TripDataCSVColumn.START_STATION_LATITUDE,
            longitude_column=TripDataCSVColumn.START_STATION_LONGITUDE,
        )
        stations.update(self._extract_stations(
            id_column=TripDataCSVColumn.END_STATION_ID,
            name_column=TripDataCSVColumn.END_STATION_NAME,
            latitude_column=TripDataCSVColumn.END_STATION_LATITUDE,
            longitude_column=TripDataCSVColumn.END_STATION_LONGITUDE,
        ))
        await self._upsert_stations(stations)

    def insert_trips(self):
        gender_map = {0: 'Male', 1: 'Female'}
        total_count = len(self.data_frame)
        chunck_size = 1000

        for offset in range(0, total_count, chunck_size):
            # convert the chunch to a list of Trip
            trips = []
            for index, row in self.data_frame[offset:offset + chunck_size].iterrows():
                trip = {
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
                }
                trips.append(trip)

            # upsert into database
            conn = self.create_engine().connect()
            conn.execute(sql.trips.insert(), trips)

            # logging
            progress = (offset + chunck_size) / total_count
            logger.info((
                f'Trip[{self.file_name}] -- '
                f'Import in Progress: {progress:.2%}({offset+chunck_size}/{total_count})'
            ))