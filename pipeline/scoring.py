import logging
import os
from datetime import datetime, timedelta

import aiohttp
import pytz
from aiohttp import BasicAuth

from database import Database, Trip

logger = logging.getLogger(__name__)


class Scoring:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def predict(self):
        # get prediction payload
        payload = self.select_prediction_payload()
        if len(payload) == 0:
            return

        # make predictions
        response = await self._make_prediction_request(payload)
        response_data = response.get('data', [])

        trip_ids = [trip['trip_id'] for trip in payload]
        predictions = [prediction['prediction'] for prediction in response_data]
        predicted_values = dict(zip(trip_ids, predictions))

        # save predicted values
        with Database() as database:
            database.update_predicted_trip_duration(predicted_values)

        logger.info(f'{len(predicted_values)} rows were scored')

    def select_prediction_payload(self) -> [dict]:
        with Database() as database:
            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            start = now.replace(2020, 1, second=0, microsecond=0)
            end = start + timedelta(minutes=1)
            range = (start, end)
            trips = database.get_trip_data(range, without_predictions=True)
            return [self._assemble_prediction_payload(trip) for trip in trips]

    @staticmethod
    def _assemble_prediction_payload(trip: Trip):
        return {
            'trip_id': trip.id,
            'bike_id': trip.bike_id,
            'birth_year': trip.birth_year,
            'gender': trip.gender,
            'start_station_id': trip.start_station.id,
            'start_time': trip.start_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
            'station_capacity': trip.start_station.capacity,
            'station_has_kiosk': trip.start_station.has_kiosk,
            'station_region_id': trip.start_station.region_id,
            'user_type': trip.user_type,
        }

    async def _make_prediction_request(self, payload: list):
        username = os.getenv('DATAROBOT_USERNAME')
        api_endpoint = os.getenv('DATAROBOT_PRED_ENDPOINT')
        api_token = os.getenv('DATAROBOT_API_TOKEN')
        deployment_id = os.getenv('DEPLOYMENT_ID')
        datarobot_key = os.getenv('DATAROBOT_KEY')

        headers = {'datarobot-key': datarobot_key}
        url = f'{api_endpoint}/deployments/{deployment_id}/predictions'

        auth = BasicAuth(username, api_token)
        async with self.session.post(url, auth=auth, headers=headers, json=payload) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                logger.error(f'Error making predictions: {await resp.json()}')
                return []
