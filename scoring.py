import os
from datetime import datetime, timedelta
import pytz

import requests

from database import Database, Trip


class Scoring:
    def select_data(self):
        with Database() as database:
            now = datetime.now().replace(tzinfo=pytz.utc)
            start = now.replace(2019, 8, second=0, microsecond=0)
            end = start + timedelta(minutes=1)
            range = (start, end)
            trips = database.get_trip_data_without_predictions(range)
            print(trips[0].start_station)

    def _assemble_prediction_payload(self, trip: Trip):
        return {
            'trip_id': trip.id,
            'bike_id': trip.bike_id,
            'birth_year': trip.birth_year,
            'gender': trip.gender,
            'start_station_id': trip.start_station.id,
            'start_time': trip.start_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
            # 'station_capacity': trip.start_station.
        }

    @staticmethod
    def predict(data: list):
        username = os.getenv('DATAROBOT_USERNAME')
        api_endpoint = os.getenv('DATAROBOT_PRED_ENDPOINT')
        api_token = os.getenv('DATAROBOT_API_TOKEN')
        deployment_id = os.getenv('DEPLOYMENT_ID')

        headers = {
            'Content-Type': 'application/json; charset=UTF-8',
            'datarobot-key': '27782',
            # 'X-DataRobot-Prediction-Timestamp':
        }
        url = f'{api_endpoint}/deployments/{deployment_id}/predictions'
        response = requests.post(url, auth=(username, api_token), data=data, headers=headers)
        return response.json()