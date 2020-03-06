import logging
import os

import aiohttp

from database import Database

logger = logging.getLogger(__name__)


class Actuals:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def upload(self):
        with Database() as database:
            trips = database.get_actuals()
            trip_ids = [trip.id for trip in trips]
            actuals = [{
                'associationId': trip.id,
                'actualValue': trip.trip_duration
            } for trip in trips]

        if not actuals:
            return

        logger.info(f'Actuals - gathering {len(actuals)} actual values to submit.')
        logger.debug(f'Actuals - trip_ids: {trip_ids}')

        await self._make_request(actuals)

        with Database() as database:
            database.mark_actuals_submitted(trip_ids)


    async def _make_request(self, payload: list):
        api_endpoint = os.getenv('DATAROBOT_ENDPOINT')
        api_token = os.getenv('DATAROBOT_API_TOKEN')
        deployment_id = os.getenv('DEPLOYMENT_ID')

        headers = {'Authorization': f'Token {api_token}'}
        url = f'{api_endpoint}/deployments/{deployment_id}/actuals/fromJSON/'

        async with self.session.post(url, headers=headers, json={'data': payload}) as resp:
            if resp.status >= 200 and resp.status < 300:
                logger.info(f'Actuals - submitted {len(payload)} actual values.')
            else:
                logger.error(f'Error submitting actuals: {resp}')
