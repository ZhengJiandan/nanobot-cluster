"""Cluster orchestration primitives for server + worker deployment."""

from nanobot.cluster.interfaces import ClusterTaskExecutor
from nanobot.cluster.models import (
    ClusterRequest,
    ClusterTask,
    ClusterTaskResult,
    WorkerRegistration,
)
from nanobot.cluster.server import ClusterServer
from nanobot.cluster.state_store import FileSessionStateStore, SessionStateMeta, SessionStateStore
from nanobot.cluster.worker import ClusterWorker, ClusterWorkerConfig
from nanobot.cluster.http_api import ClusterHTTPService
from nanobot.cluster.http_client import ClusterHTTPClient, ClusterHTTPClientConfig


def __getattr__(name: str):
    """Lazily import heavy executor components only when needed."""
    if name in {"NanobotExecutorConfig", "NanobotTaskExecutor"}:
        from nanobot.cluster.executor import NanobotExecutorConfig, NanobotTaskExecutor

        mapping = {
            "NanobotExecutorConfig": NanobotExecutorConfig,
            "NanobotTaskExecutor": NanobotTaskExecutor,
        }
        return mapping[name]
    if name in {"HTTPClusterWorker", "HTTPWorkerConfig"}:
        from nanobot.cluster.http_worker import HTTPClusterWorker, HTTPWorkerConfig

        mapping = {
            "HTTPClusterWorker": HTTPClusterWorker,
            "HTTPWorkerConfig": HTTPWorkerConfig,
        }
        return mapping[name]
    raise AttributeError(name)


__all__ = [
    "ClusterRequest",
    "ClusterServer",
    "ClusterTask",
    "ClusterTaskExecutor",
    "ClusterTaskResult",
    "ClusterHTTPClient",
    "ClusterHTTPClientConfig",
    "ClusterHTTPService",
    "ClusterWorker",
    "ClusterWorkerConfig",
    "FileSessionStateStore",
    "HTTPClusterWorker",
    "HTTPWorkerConfig",
    "NanobotExecutorConfig",
    "NanobotTaskExecutor",
    "SessionStateMeta",
    "SessionStateStore",
    "WorkerRegistration",
]
