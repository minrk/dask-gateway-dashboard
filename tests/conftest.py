from unittest import mock

import pytest
from httpx import ASGITransport, AsyncClient

from dask_gateway_dashboard import app

from .utils_test import temp_gateway


@pytest.fixture
async def gateway():
    async with temp_gateway() as g:
        with g.gateway_client() as gateway:
            with mock.patch("dask_gateway_dashboard.Gateway", g.gateway_client):
                yield gateway


@pytest.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as test_client:
        yield test_client
