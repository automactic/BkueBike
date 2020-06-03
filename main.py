import asyncio
import logging

import aiohttp

import sql
from pipeline import TrainingData, Scoring, Actuals, StationDataImporter, TripDataImporter

logger = logging.getLogger(__name__)


def export_training_data():
    TrainingData().process()


async def score():
    async with aiohttp.ClientSession() as session:
        scoring = Scoring(session)
        while True:
            try:
                await scoring.predict()
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(e)
                await asyncio.sleep(100)


async def actual_submit():
    async with aiohttp.ClientSession() as session:
        actuals = Actuals(session)
        while True:
            try:
                await actuals.upload()
                await asyncio.sleep(600)
            except Exception as e:
                logger.error(e)
                await asyncio.sleep(100)


if __name__ == '__main__':
    # initialization
    sql.create_tables()

    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

    # start run loop
    loop = asyncio.get_event_loop()
    loop.create_task(StationDataImporter().run())
    loop.create_task(TripDataImporter('data/202001-bluebikes-tripdata.csv').run())
    loop.run_forever()
    loop.close()
