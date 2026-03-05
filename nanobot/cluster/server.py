"""Cluster scheduler/server: worker registry, queueing, and task dispatch."""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path

from nanobot.cluster.models import (
    ClusterRequest,
    ClusterTask,
    ClusterTaskResult,
    WorkerRegistration,
)
from nanobot.cluster.state_store import SessionStateMeta, SessionStateStore


class ClusterServer:
    """In-memory scheduler with centralized session storage hooks."""

    def __init__(
        self,
        state_store: SessionStateStore,
        *,
        worker_timeout_s: int = 60,
    ):
        self.state_store = state_store
        self.worker_timeout = timedelta(seconds=worker_timeout_s)
        self._workers: dict[str, WorkerRegistration] = {}
        self._tasks: dict[str, ClusterTask] = {}
        self._queue: deque[str] = deque()
        self._running_sessions: set[str] = set()
        self._waiters: dict[str, asyncio.Future[ClusterTaskResult]] = {}
        self._lock = asyncio.Lock()

    async def register_worker(
        self,
        worker_id: str,
        *,
        max_concurrency: int = 1,
        labels: dict[str, str] | None = None,
    ) -> WorkerRegistration:
        async with self._lock:
            info = WorkerRegistration(
                worker_id=worker_id,
                max_concurrency=max(1, max_concurrency),
                labels=labels or {},
                last_seen=datetime.now(),
            )
            self._workers[worker_id] = info
            return info

    async def unregister_worker(self, worker_id: str) -> None:
        async with self._lock:
            worker = self._workers.pop(worker_id, None)
            if worker is None:
                return
            running = [
                t for t in self._tasks.values()
                if t.status == "running" and t.assigned_worker == worker_id
            ]
            for task in running:
                task.status = "queued"
                task.assigned_worker = None
                task.started_at = None
                self._running_sessions.discard(task.session_key)
                self._queue.appendleft(task.task_id)

    async def heartbeat(self, worker_id: str) -> bool:
        async with self._lock:
            worker = self._workers.get(worker_id)
            if worker is None:
                return False
            worker.last_seen = datetime.now()
            return True

    async def submit_request(
        self,
        *,
        channel: str,
        chat_id: str,
        content: str,
        metadata: dict | None = None,
    ) -> str:
        request = ClusterRequest(channel=channel, chat_id=chat_id, content=content, metadata=metadata or {})
        task = ClusterTask.from_request(request)
        async with self._lock:
            self._tasks[task.task_id] = task
            self._queue.append(task.task_id)
        return task.task_id

    async def submit_and_wait(
        self,
        *,
        channel: str,
        chat_id: str,
        content: str,
        metadata: dict | None = None,
        timeout_s: float | None = None,
    ) -> ClusterTaskResult:
        request = ClusterRequest(channel=channel, chat_id=chat_id, content=content, metadata=metadata or {})
        task = ClusterTask.from_request(request)
        future: asyncio.Future[ClusterTaskResult] = asyncio.get_running_loop().create_future()
        async with self._lock:
            self._tasks[task.task_id] = task
            self._queue.append(task.task_id)
            self._waiters[task.task_id] = future
        try:
            if timeout_s is None:
                return await future
            return await asyncio.wait_for(future, timeout=timeout_s)
        except asyncio.TimeoutError:
            async with self._lock:
                self._waiters.pop(task.task_id, None)
            raise

    async def get_task(self, task_id: str) -> ClusterTask | None:
        async with self._lock:
            return self._tasks.get(task_id)

    async def acquire_task(self, worker_id: str) -> ClusterTask | None:
        async with self._lock:
            self._reap_stale_workers_locked()

            worker = self._workers.get(worker_id)
            if worker is None:
                return None
            if worker.in_flight >= worker.max_concurrency:
                return None
            if not self._queue:
                return None

            queue_len = len(self._queue)
            for _ in range(queue_len):
                task_id = self._queue.popleft()
                task = self._tasks.get(task_id)
                if task is None or task.status != "queued":
                    continue
                if task.session_key in self._running_sessions:
                    self._queue.append(task_id)
                    continue

                task.status = "running"
                task.assigned_worker = worker_id
                task.started_at = datetime.now()
                task.error = None
                worker.in_flight += 1
                self._running_sessions.add(task.session_key)
                return task
            return None

    async def complete_task(self, worker_id: str, task_id: str, response: str) -> ClusterTaskResult:
        async with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                raise KeyError(f"unknown task {task_id}")
            self._release_task_slot_locked(task, worker_id)
            task.status = "succeeded"
            task.response = response
            task.error = None
            task.finished_at = datetime.now()
            result = ClusterTaskResult(
                task_id=task.task_id,
                session_key=task.session_key,
                status=task.status,
                worker_id=worker_id,
                response=response,
                started_at=task.started_at,
                finished_at=task.finished_at,
            )
            self._resolve_waiter_locked(task.task_id, result)
            return result

    async def fail_task(self, worker_id: str, task_id: str, error: str) -> ClusterTaskResult:
        async with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                raise KeyError(f"unknown task {task_id}")
            self._release_task_slot_locked(task, worker_id)
            task.status = "failed"
            task.error = error
            task.response = None
            task.finished_at = datetime.now()
            result = ClusterTaskResult(
                task_id=task.task_id,
                session_key=task.session_key,
                status=task.status,
                worker_id=worker_id,
                error=error,
                started_at=task.started_at,
                finished_at=task.finished_at,
            )
            self._resolve_waiter_locked(task.task_id, result)
            return result

    async def hydrate_workspace(self, session_key: str, workspace: Path) -> SessionStateMeta:
        return await self.state_store.hydrate_session(session_key, workspace)

    async def persist_workspace(self, session_key: str, workspace: Path) -> SessionStateMeta:
        return await self.state_store.persist_session(session_key, workspace)

    async def pending_tasks(self) -> int:
        async with self._lock:
            return sum(1 for t in self._tasks.values() if t.status in {"queued", "running"})

    def _resolve_waiter_locked(self, task_id: str, result: ClusterTaskResult) -> None:
        waiter = self._waiters.pop(task_id, None)
        if waiter and not waiter.done():
            waiter.set_result(result)

    def _release_task_slot_locked(self, task: ClusterTask, worker_id: str) -> None:
        if task.assigned_worker != worker_id:
            raise ValueError(f"task {task.task_id} is not assigned to worker {worker_id}")
        worker = self._workers.get(worker_id)
        if worker is not None and worker.in_flight > 0:
            worker.in_flight -= 1
        self._running_sessions.discard(task.session_key)

    def _reap_stale_workers_locked(self) -> None:
        if not self._workers:
            return
        now = datetime.now()
        stale = [
            worker_id for worker_id, info in self._workers.items()
            if now - info.last_seen > self.worker_timeout
        ]
        for worker_id in stale:
            self._workers.pop(worker_id, None)
            for task in self._tasks.values():
                if task.status == "running" and task.assigned_worker == worker_id:
                    task.status = "queued"
                    task.assigned_worker = None
                    task.started_at = None
                    self._running_sessions.discard(task.session_key)
                    self._queue.appendleft(task.task_id)
