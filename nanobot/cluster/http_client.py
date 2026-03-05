"""HTTP client for cluster server APIs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from nanobot.cluster.bundle import bundle_to_workspace, workspace_to_bundle
from nanobot.cluster.models import ClusterTask, ClusterTaskResult
from nanobot.cluster.state_store import SessionStateMeta


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _task_from_payload(data: dict[str, Any]) -> ClusterTask:
    return ClusterTask(
        task_id=str(data.get("task_id", "")),
        session_key=str(data.get("session_key", "")),
        channel=str(data.get("channel", "")),
        chat_id=str(data.get("chat_id", "")),
        content=str(data.get("content", "")),
        metadata=data.get("metadata") or {},
        status=str(data.get("status", "queued")),  # type: ignore[arg-type]
        assigned_worker=data.get("assigned_worker"),
        error=data.get("error"),
        response=data.get("response"),
        created_at=_parse_datetime(data.get("created_at")) or datetime.now(),
        started_at=_parse_datetime(data.get("started_at")),
        finished_at=_parse_datetime(data.get("finished_at")),
    )


def _result_from_payload(data: dict[str, Any]) -> ClusterTaskResult:
    return ClusterTaskResult(
        task_id=str(data.get("task_id", "")),
        session_key=str(data.get("session_key", "")),
        status=str(data.get("status", "failed")),  # type: ignore[arg-type]
        worker_id=data.get("worker_id"),
        response=data.get("response"),
        error=data.get("error"),
        started_at=_parse_datetime(data.get("started_at")),
        finished_at=_parse_datetime(data.get("finished_at")),
    )


@dataclass(slots=True)
class ClusterHTTPClientConfig:
    """HTTP client configuration."""

    server_url: str
    token: str | None = None
    timeout_s: float = 30.0


class ClusterHTTPClient:
    """Async client wrapper for cluster HTTP APIs."""

    def __init__(self, config: ClusterHTTPClientConfig):
        self.config = config
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if config.token:
            headers["X-Cluster-Token"] = config.token
        self._client = httpx.AsyncClient(
            base_url=config.server_url.rstrip("/"),
            timeout=config.timeout_s,
            headers=headers,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def health(self) -> dict[str, Any]:
        return await self._request("GET", "/v1/health")

    async def submit(
        self,
        *,
        channel: str,
        chat_id: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        data = await self._request(
            "POST",
            "/v1/tasks/submit",
            {
                "channel": channel,
                "chat_id": chat_id,
                "content": content,
                "metadata": metadata or {},
            },
        )
        return str(data["task_id"])

    async def submit_and_wait(
        self,
        *,
        channel: str,
        chat_id: str,
        content: str,
        metadata: dict[str, Any] | None = None,
        timeout_s: float = 120.0,
    ) -> ClusterTaskResult:
        data = await self._request(
            "POST",
            "/v1/tasks/submit_and_wait",
            {
                "channel": channel,
                "chat_id": chat_id,
                "content": content,
                "metadata": metadata or {},
                "timeout_s": timeout_s,
            },
        )
        return _result_from_payload(data["result"])

    async def get_task(self, task_id: str) -> ClusterTask | None:
        try:
            data = await self._request("GET", f"/v1/tasks/{task_id}")
        except RuntimeError as e:
            if "404" in str(e):
                return None
            raise
        return _task_from_payload(data["task"])

    async def register_worker(self, worker_id: str, max_concurrency: int, labels: dict[str, str] | None = None) -> None:
        await self._request(
            "POST",
            "/v1/workers/register",
            {
                "worker_id": worker_id,
                "max_concurrency": max_concurrency,
                "labels": labels or {},
            },
        )

    async def unregister_worker(self, worker_id: str) -> None:
        await self._request("POST", f"/v1/workers/{worker_id}/unregister", {})

    async def heartbeat(self, worker_id: str) -> bool:
        data = await self._request("POST", f"/v1/workers/{worker_id}/heartbeat", {})
        return bool(data.get("ok", False))

    async def acquire_task(self, worker_id: str, wait_ms: int = 0) -> ClusterTask | None:
        data = await self._request(
            "POST",
            f"/v1/workers/{worker_id}/acquire",
            {"wait_ms": wait_ms},
        )
        payload = data.get("task")
        if not payload:
            return None
        return _task_from_payload(payload)

    async def complete_task(self, worker_id: str, task_id: str, response: str) -> ClusterTaskResult:
        data = await self._request(
            "POST",
            f"/v1/tasks/{task_id}/complete",
            {"worker_id": worker_id, "response": response},
        )
        return _result_from_payload(data["result"])

    async def fail_task(self, worker_id: str, task_id: str, error: str) -> ClusterTaskResult:
        data = await self._request(
            "POST",
            f"/v1/tasks/{task_id}/fail",
            {"worker_id": worker_id, "error": error},
        )
        return _result_from_payload(data["result"])

    async def hydrate_workspace(self, session_key: str, workspace: Path) -> SessionStateMeta:
        data = await self._request(
            "POST",
            "/v1/sessions/hydrate",
            {"session_key": session_key},
        )
        bundle_to_workspace(str(data.get("bundle", "")), workspace)
        meta = data.get("meta") or {}
        return SessionStateMeta(
            session_key=str(meta.get("session_key", session_key)),
            version=int(meta.get("version", 0)),
            updated_at=meta.get("updated_at"),
        )

    async def persist_workspace(self, session_key: str, workspace: Path) -> SessionStateMeta:
        data = await self._request(
            "POST",
            "/v1/sessions/persist",
            {"session_key": session_key, "bundle": workspace_to_bundle(workspace)},
        )
        meta = data.get("meta") or {}
        return SessionStateMeta(
            session_key=str(meta.get("session_key", session_key)),
            version=int(meta.get("version", 0)),
            updated_at=meta.get("updated_at"),
        )

    async def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        response = await self._client.request(method, path, json=payload)
        text = response.text
        try:
            data = response.json() if text else {}
        except Exception:
            data = {"raw": text}
        if response.status_code >= 400:
            message = data.get("error") if isinstance(data, dict) else text
            raise RuntimeError(f"cluster http {response.status_code}: {message}")
        if not isinstance(data, dict):
            raise RuntimeError("cluster http invalid response payload")
        return data

