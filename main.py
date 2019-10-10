import asyncio
import logging

from data_sources import Stations, Regions, Trips
from database import Database
from pipeline import TrainingData, Scoring, Actuals

logger = logging.getLogger(__name__)


def export_training_data():
    TrainingData('data/201907-bluebikes-tripdata.csv').process()


async def import_and_update_database():
    while True:
        logger.info('Database update, starting...')

        stations = Stations()
        regions = Regions()
        trip_csv_paths = ['data/201908-bluebikes-tripdata.csv']
        with Database() as database:
            database.update_stations(stations, regions)
            for trip_csv_path in trip_csv_paths:
                trips = Trips(trip_csv_path)
                database.update_trip_data(trips)

        logger.info('Database update, done!')

        await asyncio.sleep(120)


async def score():
    scoring = Scoring()
    while True:
        scoring.predict()
        await asyncio.sleep(10)


async def actual_submit():
    actuals = Actuals()
    while True:
        actuals.upload()
        await asyncio.sleep(60)


if __name__ == '__main__':
    # initialization
    Database.create_table()
    Database.create_index()

    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

    # start run loop
    loop = asyncio.get_event_loop()
    loop.create_task(import_and_update_database())
    loop.create_task(score())
    loop.create_task(actual_submit())
    loop.run_forever()
    loop.close()
