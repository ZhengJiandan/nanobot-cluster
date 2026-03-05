"""Shared data models for cluster server/worker orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import uuid4


TaskStatus = Literal["queued", "running", "succeeded", "failed"]


@dataclass(slots=True)
class ClusterRequest:
    """A user request submitted to the cluster server."""

    channel: str
    chat_id: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)
    request_id: str = field(default_factory=lambda: uuid4().hex)
    created_at: datetime = field(default_factory=datetime.now)

    @property
    def session_key(self) -> str:
        return f"{self.channel}:{self.chat_id}"


@dataclass(slots=True)
class ClusterTask:
    """A schedulable task derived from a user request."""

    task_id: str
    session_key: str
    channel: str
    chat_id: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)
    status: TaskStatus = "queued"
    assigned_worker: str | None = None
    error: str | None = None
    response: str | None = None
    created_at: datetime = field(default_factory=datetime.now)
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @classmethod
    def from_request(cls, request: ClusterRequest) -> "ClusterTask":
        return cls(
            task_id=request.request_id,
            session_key=request.session_key,
            channel=request.channel,
            chat_id=request.chat_id,
            content=request.content,
            metadata=dict(request.metadata),
        )


@dataclass(slots=True)
class ClusterTaskResult:
    """Final execution result for a task."""

    task_id: str
    session_key: str
    status: TaskStatus
    worker_id: str | None = None
    response: str | None = None
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass(slots=True)
class WorkerRegistration:
    """Metadata tracked by the scheduler for each worker."""

    worker_id: str
    max_concurrency: int
    labels: dict[str, str] = field(default_factory=dict)
    in_flight: int = 0
    last_seen: datetime = field(default_factory=datetime.now)

