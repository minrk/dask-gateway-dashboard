import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict

from dask.utils import format_bytes
from dask_gateway import Gateway
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

app_dir = Path(__file__).parent.resolve()
index_html = app_dir / "index.html"
log = logging.getLogger("uvicorn.error")
app = FastAPI()


class ClusterModel(TypedDict):
    name: str
    dashboard_link: str
    workers: int
    cores: float
    memory: str
    started: float


def make_cluster_model(cluster) -> ClusterModel:
    """Make a single cluster model"""
    # derived from dask-labextension Manager
    info = cluster.scheduler_info
    cores = sum(d["nthreads"] for d in info["workers"].values())
    workers = len(info["workers"])
    memory = format_bytes(sum(d["memory_limit"] for d in info["workers"].values()))
    return {
        "name": cluster.name,
        "dashboard_link": cluster.dashboard_link,
        "workers": workers,
        "cores": cores,
        "memory": memory,
        "started": info["started"],
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


@dataclass
class _MockCluster:
    """Mock cluster for UI development"""

    name: str
    workers: int
    cores_per_worker: int = 2

    @property
    def dashboard_link(self) -> str:
        return f"http://localhost:8000/cluster/{self.name}"

    @property
    def scheduler_info(self) -> dict:
        return {
            "started": time.time() - 3600,
            "workers": {
                f"id-{n}": {
                    "nthreads": self.cores_per_worker,
                    "memory_limit": 2 << 30,
                }
                for n in range(self.workers)
            },
        }


async def _mock_list_clusters() -> list[ClusterModel]:
    """mock cluster list for UI development"""
    clusters = []
    for i in range(3):
        cluster = _MockCluster(workers=i, name=f"cluster {i}")
        clusters.append(make_cluster_model(cluster))
    return clusters


if os.environ.get("_GATEWAY_MOCK_CLUSTERS"):
    list_clusters = _mock_list_clusters


@app.get("/")
async def get():
    """serve the HTML page"""
    with index_html.open() as f:
        return HTMLResponse(f.read())


@app.get("/clusters")
async def get_clusters():
    """Return list of clusters as JSON"""
    clusters = await list_clusters()
    return JSONResponse(clusters)
