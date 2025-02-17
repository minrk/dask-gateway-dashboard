from dask_gateway_dashboard import list_clusters


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
