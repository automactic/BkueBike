import logging
import os

from datarobot import Deployment

from database import Database

logger = logging.getLogger(__name__)


class Actuals:
    def __init__(self, deployment_id=None):
        deployment_id = deployment_id or os.getenv('DEPLOYMENT_ID')
        self.deployment = Deployment.get(deployment_id)

    def upload(self):
        with Database() as database:
            trips = database.get_actuals()
            trip_ids = [trip.id for trip in trips]
            actuals = [{
                'association_id': trip.id, 'actual_value': trip.trip_duration
            } for trip in trips]

        if not actuals:
            return

        logger.info(f'Actuals - gathering {len(actuals)} actual values to submit.')
        logger.debug(f'Actuals - trip_ids: {trip_ids}')
        self.deployment.submit_actuals(actuals)

        with Database() as database:
            database.mark_actuals_submitted(trip_ids)

        logger.info(f'Actuals - submitted {len(actuals)} actual values.')
