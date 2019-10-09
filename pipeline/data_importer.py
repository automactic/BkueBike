from os import listdir
from os.path import isfile, join
import logging
from data_sources import Stations, Regions, Trips
from database import Database


logger = logging.getLogger(__name__)


class DataImporter:
    def __init__(self, dir_path='data'):
        self.dir_path = dir_path

    def _get_file_paths(self):
        files = [join(self.dir_path, file) for file in listdir(self.dir_path)]
        return [file for file in files if isfile(file) and file.endswith('.csv')]

    def scan_and_update(self):
        logger.info('Data Import - starting...')

        stations = Stations()
        regions = Regions()
        with Database() as database:
            database.update_stations(stations, regions)
            logger.info('Data Import - stations updated.')

            file_paths = self._get_file_paths()
            logger.debug(f'Data Import - discovered {len(file_paths)} files, {file_paths}')
            for file_path in file_paths:
                trips = Trips(file_path)
                database.update_trip_data(trips)

        logger.info('Data Import - done!')
