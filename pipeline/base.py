from contextlib import asynccontextmanager
from aiohttp import ClientSession
import typing


class HTTPSessionMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @asynccontextmanager
    async def create_session(self) -> typing.AsyncContextManager[ClientSession]:
        async with ClientSession() as session:
            yield session
