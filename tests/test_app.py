from unittest import mock

import pytest
from httpx import ASGITransport, AsyncClient

from dask_gateway_dashboard import app, list_clusters

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


async def test_list_clusters(gateway):
    clusters = await list_clusters()
    assert clusters == []
    cluster = await gateway.new_cluster()
    clusters = await list_clusters()
    assert len(clusters) == 1
    c = clusters[0]
    assert c["name"] == cluster.name
    assert c["workers"] == 0
    assert c["cores"] == 0


async def test_get_page(client):
    page = await client.get("/")
    assert page.status_code == 200


async def test_get_clusters(client, gateway):
    page = await client.get("/clusters")
    assert page.status_code == 200
    cluster_list = page.json()
    assert cluster_list == []
