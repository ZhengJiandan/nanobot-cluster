"""HTTP-based remote worker daemon."""

from __future__ import annotations

import asyncio
import os
import socket
import tempfile
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from nanobot.cluster.http_client import ClusterHTTPClient, ClusterHTTPClientConfig
from nanobot.cluster.interfaces import ClusterTaskExecutor
from nanobot.cluster.models import ClusterTask


@dataclass(slots=True)
class HTTPWorkerConfig:
    """Runtime settings for a remote worker daemon."""

    server_url: str
    worker_id: str
    token: str | None = None
    max_concurrency: int = 1
    poll_interval_s: float = 0.2
    heartbeat_interval_s: float = 5.0
    acquire_wait_ms: int = 1500
    temp_root: Path | None = None
    request_timeout_s: float = 60.0
    labels: dict[str, str] | None = None

    @classmethod
    def with_default_worker_id(
        cls,
        *,
        server_url: str,
        token: str | None = None,
        max_concurrency: int = 1,
    ) -> "HTTPWorkerConfig":
        host = socket.gethostname()
        worker_id = f"{host}-{os.getpid()}"
        return cls(
            server_url=server_url,
            token=token,
            worker_id=worker_id,
            max_concurrency=max_concurrency,
        )


class HTTPClusterWorker:
    """Worker daemon that communicates with cluster-server over HTTP."""

    def __init__(
        self,
        *,
        config: HTTPWorkerConfig,
        executor: ClusterTaskExecutor,
    ):
        self.config = config
        self.executor = executor
        self._running = False
        self._inflight: set[asyncio.Task] = set()
        self._client = ClusterHTTPClient(
            ClusterHTTPClientConfig(
                server_url=config.server_url,
                token=config.token,
                timeout_s=config.request_timeout_s,
            )
        )

    async def run(self) -> None:
        """Run daemon loop until stop() is called."""
        if self._running:
            return
        self._running = True
        await self._client.register_worker(
            self.config.worker_id,
            max_concurrency=self.config.max_concurrency,
            labels=self.config.labels,
        )
        logger.info("HTTP worker '{}' connected to {}", self.config.worker_id, self.config.server_url)

        last_heartbeat = 0.0
        try:
            while self._running or self._inflight:
                now = asyncio.get_running_loop().time()
                if now - last_heartbeat >= self.config.heartbeat_interval_s:
                    await self._client.heartbeat(self.config.worker_id)
                    last_heartbeat = now

                while self._running and len(self._inflight) < self.config.max_concurrency:
                    task = await self._client.acquire_task(
                        self.config.worker_id,
                        wait_ms=self.config.acquire_wait_ms,
                    )
                    if task is None:
                        break
                    runner = asyncio.create_task(self._run_task(task))
                    self._inflight.add(runner)
                    runner.add_done_callback(self._inflight.discard)

                if self._inflight:
                    done, _ = await asyncio.wait(
                        self._inflight,
                        timeout=self.config.poll_interval_s,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for finished in done:
                        finished.result()
                else:
                    await asyncio.sleep(self.config.poll_interval_s)
        finally:
            for task in list(self._inflight):
                task.cancel()
            if self._inflight:
                await asyncio.gather(*self._inflight, return_exceptions=True)
            try:
                await self._client.unregister_worker(self.config.worker_id)
            except Exception:
                logger.warning("HTTP worker '{}' unregister failed", self.config.worker_id)
            await self._client.aclose()
            logger.info("HTTP worker '{}' stopped", self.config.worker_id)

    def stop(self) -> None:
        """Request graceful shutdown."""
        self._running = False

    async def _run_task(self, task: ClusterTask) -> None:
        logger.info("HTTP worker '{}' running task {}", self.config.worker_id, task.task_id)
        try:
            with tempfile.TemporaryDirectory(prefix=f"nb-remote-{self.config.worker_id}-", dir=self._temp_dir()) as td:
                workspace = Path(td) / "workspace"
                workspace.mkdir(parents=True, exist_ok=True)
                await self._client.hydrate_workspace(task.session_key, workspace)
                response = await self.executor.execute(workspace, task)
                await self._client.persist_workspace(task.session_key, workspace)
            await self._client.complete_task(self.config.worker_id, task.task_id, response)
        except Exception as e:
            logger.exception("HTTP worker '{}' task {} failed", self.config.worker_id, task.task_id)
            await self._client.fail_task(self.config.worker_id, task.task_id, str(e))

    def _temp_dir(self) -> str | None:
        if self.config.temp_root is None:
            return None
        self.config.temp_root.mkdir(parents=True, exist_ok=True)
        return str(self.config.temp_root)
