import logging
import os
from datetime import datetime, timedelta

import pytz
import requests

from database import Database, Trip

logger = logging.getLogger(__name__)


class Scoring:
    def select_prediction_payload(self) -> [dict]:
        with Database() as database:
            now = datetime.now().replace(tzinfo=pytz.utc)
            start = now.replace(2019, 8, second=0, microsecond=0)
            end = start + timedelta(minutes=1)
            range = (start, end)
            trips = database.get_trip_data_without_predictions(range)
            return [self._assemble_prediction_payload(trip) for trip in trips]

    def predict(self):
        # get prediction payload
        payload = self.select_prediction_payload()
        if len(payload) == 0:
            return

        # make predictions
        response = self._make_prediction_request(payload)
        response_data = response.get('data', [])
        predicted_values = {
            item['passthroughValues']['trip_id']: item['prediction']
            for item in response_data
        }

        # save predicted values
        with Database() as database:
            database.update_predicted_trip_duration(predicted_values)

        logger.info(f'Predictions made: {len(predicted_values)} rows')

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

    @staticmethod
    def _make_prediction_request(payload: list):
        username = os.getenv('DATAROBOT_USERNAME')
        api_endpoint = os.getenv('DATAROBOT_PRED_ENDPOINT')
        api_token = os.getenv('DATAROBOT_API_TOKEN')
        deployment_id = os.getenv('DEPLOYMENT_ID')
        datarobot_key = os.getenv('DATAROBOT_KEY')

        headers = {'datarobot-key': datarobot_key}
        url = f'{api_endpoint}/deployments/{deployment_id}/predictions'
        response = requests.post(url, auth=(username, api_token), json=payload, headers=headers)
        return response.json()