import os
import typing
from contextlib import asynccontextmanager

from aiohttp import ClientSession


class HTTPSessionMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @asynccontextmanager
    async def create_session(self, headers: dict = None) -> typing.AsyncContextManager[ClientSession]:
        headers = headers if headers else {}
        async with ClientSession(headers=headers) as session:
            yield session


class DataRobotEnvMixin:
    api_endpoint = os.getenv('DATAROBOT_ENDPOINT')
    api_token = os.getenv('DATAROBOT_API_TOKEN')
    pred_server = os.getenv('DATAROBOT_PRED_ENDPOINT')
    pred_server_key = os.getenv('DATAROBOT_KEY')
    username = os.getenv('DATAROBOT_USERNAME')
    deployment_id = os.getenv('DEPLOYMENT_ID')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        fields = [
            self.api_endpoint,
            self.api_token,
            self.pred_server,
            self.pred_server_key,
            self.username,
            self.deployment_id
        ]
        if not all(fields):
            raise ValueError('Missing DataRobot environment variables', *fields)
