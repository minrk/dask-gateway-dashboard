import logging
from pathlib import Path
from typing import TypedDict

from dask_gateway import Gateway
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

app_dir = Path(__file__).parent.resolve()
index_html = app_dir / "index.html"
app = FastAPI()

log = logging.getLogger("uvicorn.error")


class ClusterModel(TypedDict):
    name: str
    dashboard_link: str
    workers: int
    cores: float


def make_cluster_model(cluster) -> ClusterModel:
    """Make a single cluster model"""
    info = cluster.scheduler_info
    cores = sum(d["nthreads"] for d in info["workers"].values())
    workers = len(info["workers"])
    return {
        "name": cluster.name,
        "dashboard_link": cluster.dashboard_link,
        "workers": workers,
        "cores": cores,
    }


async def list_clusters() -> list[ClusterModel]:
    """List of cluster models"""
    clusters: list[ClusterModel] = []
    async with Gateway(asynchronous=True) as gateway:
        for cluster_info in await gateway.list_clusters():
            cluster_name = cluster_info.name
            async with gateway.connect(cluster_name) as cluster:
                cluster_model = make_cluster_model(cluster)
            clusters.append(cluster_model)
    return clusters


@app.get("/")
async def get():
    with index_html.open() as f:
        return HTMLResponse(f.read())


@app.get("/clusters")
async def get_clusters():
    clusters = await list_clusters()
    return JSONResponse(clusters)
