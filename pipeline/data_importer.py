import dataclasses
import logging

import aiohttp
import sqlalchemy as sa

import sql
from entities import Region, Station
from sql import DatabaseMixin

logger = logging.getLogger(__name__)


class DataImporter(DatabaseMixin):
    def __init__(self, session: aiohttp.ClientSession, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cwd = 'data'
        self.session = session

    async def run(self):
        while True:
            stations = await self._fetch_stations()
            await self._upsert_stations(stations)

    async def _fetch_regions(self) -> {str, Region}:
        url = 'https://gbfs.bluebikes.com/gbfs/en/system_regions.json'
        async with self.session.get(url) as response:
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

    async def _fetch_stations(self) -> {str, Station}:
        url = 'https://gbfs.bluebikes.com/gbfs/en/station_information.json'
        async with self.session.get(url) as response:
            response_data = await response.json()

        regions = await self._fetch_regions()
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

        if new_stations:
            logger.info(f'DataImporter -- found {len(new_stations)}: {new_stations.keys()}.')
        else:
            logger.info(f'DataImporter -- no new station found.')