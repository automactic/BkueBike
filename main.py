import asyncio
import logging

from data_sources import Stations, Regions, Trips
from database import Database
from pipeline import TrainingData, Scoring, Actuals, DataImporter

logger = logging.getLogger(__name__)


def export_training_data():
    TrainingData().process()


async def import_and_update_database():
    importer = DataImporter()
    while True:
        importer.scan_and_update()
        await asyncio.sleep(600)


async def score():
    scoring = Scoring()
    while True:
        scoring.predict()
        await asyncio.sleep(10)


async def actual_submit():
    actuals = Actuals()
    while True:
        actuals.upload()
        await asyncio.sleep(600)


if __name__ == '__main__':
    export_training_data()

    # # initialization
    # Database.create_table()
    # Database.create_index()
    #
    # logging.basicConfig()
    # logging.getLogger().setLevel(logging.DEBUG)
    #
    # # start run loop
    # loop = asyncio.get_event_loop()
    # loop.create_task(import_and_update_database())
    # loop.create_task(score())
    # loop.create_task(actual_submit())
    # loop.run_forever()
    # loop.close()
