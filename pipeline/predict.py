import asyncio
import logging
from datetime import datetime, timedelta

import pytz
import sqlalchemy as sa
from aiohttp import ClientSession, client_exceptions

import sql
from pipeline.base import HTTPSessionMixin, DataRobotEnvMixin
from sql import DatabaseMixin

logger = logging.getLogger(__name__)


class Predict(DatabaseMixin, HTTPSessionMixin, DataRobotEnvMixin):
    timezone = pytz.timezone('US/Eastern')

    async def run(self):
        request_headers = {
            'Authorization': f'Bearer {self.api_token}',
            'DataRobot-Key': self.pred_server_key,
        }

        while True:
            data = await self.select_data()
            async with self.create_session(request_headers) as session:
                predictions = await self.make_predictions(session, data)
            await self.save_predictions(predictions)

            if predictions:
                logging.info(
                    f'Made prediction of {len(predictions)} trips at {datetime.now(tz=pytz.utc)}'
                )

            await asyncio.sleep(60)

    async def select_data(self) -> [dict]:
        end_time = datetime.now(tz=pytz.utc).replace(year=2020, month=1)
        start_time = end_time - timedelta(minutes=15)

        async with self.conn() as conn:
            statement = sa.select([
                sql.trips.c.id.label('trip_id'),
                sql.trips.c.start_time,
                sql.stations.c.id.label('start_station_id'),
                sql.stations.c.name.label('start_station_name'),
                sql.stations.c.region_name.label('start_station_region_name'),
                sql.stations.c.latitude.label('start_station_latitude'),
                sql.stations.c.longitude.label('start_station_longitude'),
                sql.stations.c.capacity.label('start_station_capacity'),
                sql.trips.c.bike_id,
                sql.trips.c.user_type,
                sql.trips.c.user_birth_year,
                sql.trips.c.user_gender,
                sa.func.date_part('day', sql.trips.c.start_time),
            ]).select_from(sa.join(
                sql.trips, sql.stations, sql.trips.c.start_station_id == sql.stations.c.id,
            )).where(sa.and_(
                sql.trips.c.start_time_normalized >= start_time,
                sql.trips.c.stop_time_normalized <= end_time,
                sql.trips.c.predicted_trip_duration == None,
            )).order_by(sql.trips.c.start_time)

            results = await conn.execute(statement)
            rows = await results.fetchall()

        trips = []
        for row in rows:
            trip = dict(row)
            start_time = row.start_time.astimezone(self.timezone).replace(tzinfo=None)
            trip['start_time'] = start_time.strftime('%Y-%m-%d %H:%M:%S.%f')
            trips.append(trip)
        return trips

    async def make_predictions(self, session: ClientSession, data: [dict]) -> {str: float}:
        url = f'{self.pred_server}/deployments/{self.deployment_id}/predictions'
        async with session.post(url, json=data) as response:
            try:
                response_json = await response.json()
                response_data = response_json.get('data', [])

                predictions = {}
                for item in response_data:
                    trip_id = item['passthroughValues']['trip_id']
                    predictions[trip_id] = item['prediction']
                return predictions
            except client_exceptions.ContentTypeError as e:
                response_text = await response.text()
                logger.error('Unexpected prediction response content type', response_text)

    async def save_predictions(self, predictions: {str, float}):
        async with self.conn() as conn:
            for trip_id, predicted_value in predictions.items():
                statement = sa.update(
                    sql.trips
                ).where(
                    sql.trips.c.id == trip_id
                ).values(
                    predicted_trip_duration=predicted_value
                )
                await conn.execute(statement)


if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

    loop = asyncio.get_event_loop()
    loop.create_task(Predict().run())
    loop.run_forever()
    loop.close()
