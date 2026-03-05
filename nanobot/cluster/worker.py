"""Cluster worker runtime."""

from __future__ import annotations

import asyncio
import tempfile
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from nanobot.cluster.interfaces import ClusterTaskExecutor
from nanobot.cluster.models import ClusterTask
from nanobot.cluster.server import ClusterServer


@dataclass(slots=True)
class ClusterWorkerConfig:
    """Polling and runtime controls for a worker process."""

    max_concurrency: int = 1
    poll_interval_s: float = 0.2
    heartbeat_interval_s: float = 5.0
    temp_root: Path | None = None


class ClusterWorker:
    """Consumes tasks from ClusterServer and executes them in temp workspaces."""

    def __init__(
        self,
        *,
        worker_id: str,
        server: ClusterServer,
        executor: ClusterTaskExecutor,
        config: ClusterWorkerConfig | None = None,
        labels: dict[str, str] | None = None,
    ):
        self.worker_id = worker_id
        self.server = server
        self.executor = executor
        self.config = config or ClusterWorkerConfig()
        self.labels = labels or {}
        self._running = False
        self._inflight: set[asyncio.Task] = set()

    async def run(self) -> None:
        """Main worker loop."""
        if self._running:
            return
        self._running = True
        await self.server.register_worker(
            self.worker_id,
            max_concurrency=self.config.max_concurrency,
            labels=self.labels,
        )
        logger.info("Cluster worker '{}' started", self.worker_id)

        last_heartbeat = 0.0
        try:
            while self._running or self._inflight:
                now = asyncio.get_running_loop().time()
                if now - last_heartbeat >= self.config.heartbeat_interval_s:
                    await self.server.heartbeat(self.worker_id)
                    last_heartbeat = now

                while self._running and len(self._inflight) < self.config.max_concurrency:
                    task = await self.server.acquire_task(self.worker_id)
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
                        # Surface exceptions early; _run_task already reports task failure.
                        finished.result()
                else:
                    await asyncio.sleep(self.config.poll_interval_s)
        finally:
            for task in list(self._inflight):
                task.cancel()
            if self._inflight:
                await asyncio.gather(*self._inflight, return_exceptions=True)
            await self.server.unregister_worker(self.worker_id)
            logger.info("Cluster worker '{}' stopped", self.worker_id)

    def stop(self) -> None:
        """Signal graceful shutdown."""
        self._running = False

    async def _run_task(self, task: ClusterTask) -> None:
        logger.info("Worker '{}' executing task {} ({})", self.worker_id, task.task_id, task.session_key)
        try:
            with tempfile.TemporaryDirectory(prefix=f"nanobot-{self.worker_id}-", dir=self._temp_dir()) as td:
                workspace = Path(td) / "workspace"
                workspace.mkdir(parents=True, exist_ok=True)

                await self.server.hydrate_workspace(task.session_key, workspace)
                response = await self.executor.execute(workspace, task)
                await self.server.persist_workspace(task.session_key, workspace)
            await self.server.complete_task(self.worker_id, task.task_id, response)
        except Exception as e:
            logger.exception("Worker '{}' failed task {}", self.worker_id, task.task_id)
            await self.server.fail_task(self.worker_id, task.task_id, str(e))

    def _temp_dir(self) -> str | None:
        if self.config.temp_root is None:
            return None
        self.config.temp_root.mkdir(parents=True, exist_ok=True)
        return str(self.config.temp_root)
