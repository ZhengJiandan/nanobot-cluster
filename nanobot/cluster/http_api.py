"""HTTP protocol server for cluster scheduling and worker coordination."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from dataclasses import asdict
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import urlparse

from loguru import logger

from nanobot.cluster.bundle import bundle_to_workspace, workspace_to_bundle
from nanobot.cluster.models import ClusterTask, ClusterTaskResult
from nanobot.cluster.server import ClusterServer


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def task_to_payload(task: ClusterTask) -> dict[str, Any]:
    """Serialize a ClusterTask for HTTP responses."""
    return {
        "task_id": task.task_id,
        "session_key": task.session_key,
        "channel": task.channel,
        "chat_id": task.chat_id,
        "content": task.content,
        "metadata": task.metadata,
        "status": task.status,
        "assigned_worker": task.assigned_worker,
        "error": task.error,
        "response": task.response,
        "created_at": _iso(task.created_at),
        "started_at": _iso(task.started_at),
        "finished_at": _iso(task.finished_at),
    }


def result_to_payload(result: ClusterTaskResult) -> dict[str, Any]:
    """Serialize a ClusterTaskResult for HTTP responses."""
    return {
        "task_id": result.task_id,
        "session_key": result.session_key,
        "status": result.status,
        "worker_id": result.worker_id,
        "response": result.response,
        "error": result.error,
        "started_at": _iso(result.started_at),
        "finished_at": _iso(result.finished_at),
    }


class _LoopBridge:
    """Run cluster coroutines on one dedicated event loop thread."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True, name="cluster-loop")
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._thread.start()
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return
        try:
            self.loop.call_soon_threadsafe(self.loop.stop)
        except RuntimeError:
            pass
        self._thread.join(timeout=5)
        self._started = False

    def run(self, coro: Any, timeout_s: float = 60.0):
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result(timeout=timeout_s)

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()
        self.loop.close()


class ClusterHTTPService:
    """Threaded HTTP service exposing cluster server APIs."""

    def __init__(
        self,
        server: ClusterServer,
        *,
        host: str = "0.0.0.0",
        port: int = 18880,
        auth_token: str | None = None,
        request_timeout_s: float = 60.0,
    ):
        self.server = server
        self.host = host
        self.port = port
        self.auth_token = auth_token
        self.request_timeout_s = request_timeout_s
        self._bridge = _LoopBridge()
        self._httpd: ThreadingHTTPServer | None = None

    def serve_forever(self) -> None:
        """Start the HTTP service and block."""
        self._bridge.start()
        handler_cls = self._make_handler()
        self._httpd = ThreadingHTTPServer((self.host, self.port), handler_cls)
        logger.info("Cluster HTTP server listening on {}:{}", self.host, self.port)
        try:
            self._httpd.serve_forever(poll_interval=0.5)
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Stop HTTP server and async loop bridge."""
        if self._httpd is not None:
            try:
                self._httpd.shutdown()
            except Exception:
                pass
            try:
                self._httpd.server_close()
            except Exception:
                pass
            self._httpd = None
        self._bridge.stop()

    def _make_handler(self):
        app = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
                logger.debug("cluster-http {} - {}", self.address_string(), fmt % args)

            def do_GET(self) -> None:  # noqa: N802
                app._handle(self)

            def do_POST(self) -> None:  # noqa: N802
                app._handle(self)

        return Handler

    def _handle(self, req: BaseHTTPRequestHandler) -> None:
        try:
            status, payload = self._route(req)
        except ValueError as e:
            status, payload = HTTPStatus.BAD_REQUEST, {"error": str(e)}
        except Exception as e:
            logger.exception("cluster-http request failed")
            status, payload = HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)}
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req.send_response(int(status))
        req.send_header("Content-Type", "application/json; charset=utf-8")
        req.send_header("Content-Length", str(len(body)))
        req.end_headers()
        req.wfile.write(body)

    def _route(self, req: BaseHTTPRequestHandler) -> tuple[HTTPStatus, dict[str, Any]]:
        method = req.command.upper()
        parsed = urlparse(req.path)
        path = parsed.path

        if path == "/v1/health" and method == "GET":
            pending = self._bridge.run(self.server.pending_tasks(), timeout_s=self.request_timeout_s)
            return HTTPStatus.OK, {"ok": True, "pending_tasks": pending}

        if not self._is_authorized(req):
            return HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"}

        body = self._read_json_body(req) if method == "POST" else {}

        if path == "/v1/tasks/submit" and method == "POST":
            task_id = self._bridge.run(
                self.server.submit_request(
                    channel=str(body.get("channel", "")),
                    chat_id=str(body.get("chat_id", "")),
                    content=str(body.get("content", "")),
                    metadata=body.get("metadata") or {},
                ),
                timeout_s=self.request_timeout_s,
            )
            return HTTPStatus.OK, {"task_id": task_id}

        if path == "/v1/tasks/submit_and_wait" and method == "POST":
            timeout_s = float(body.get("timeout_s", 120))
            result = self._bridge.run(
                self.server.submit_and_wait(
                    channel=str(body.get("channel", "")),
                    chat_id=str(body.get("chat_id", "")),
                    content=str(body.get("content", "")),
                    metadata=body.get("metadata") or {},
                    timeout_s=timeout_s,
                ),
                timeout_s=timeout_s + 5,
            )
            return HTTPStatus.OK, {"result": result_to_payload(result)}

        if path.startswith("/v1/tasks/") and method == "GET":
            task_id = path.removeprefix("/v1/tasks/")
            task = self._bridge.run(self.server.get_task(task_id), timeout_s=self.request_timeout_s)
            if task is None:
                return HTTPStatus.NOT_FOUND, {"error": "task not found"}
            return HTTPStatus.OK, {"task": task_to_payload(task)}

        if path == "/v1/workers/register" and method == "POST":
            worker = self._bridge.run(
                self.server.register_worker(
                    worker_id=str(body.get("worker_id", "")),
                    max_concurrency=int(body.get("max_concurrency", 1)),
                    labels=body.get("labels") or {},
                ),
                timeout_s=self.request_timeout_s,
            )
            return HTTPStatus.OK, {"worker": asdict(worker)}

        if path.startswith("/v1/workers/") and path.endswith("/heartbeat") and method == "POST":
            worker_id = path.split("/")[3]
            ok = self._bridge.run(self.server.heartbeat(worker_id), timeout_s=self.request_timeout_s)
            return HTTPStatus.OK if ok else HTTPStatus.NOT_FOUND, {"ok": ok}

        if path.startswith("/v1/workers/") and path.endswith("/unregister") and method == "POST":
            worker_id = path.split("/")[3]
            self._bridge.run(self.server.unregister_worker(worker_id), timeout_s=self.request_timeout_s)
            return HTTPStatus.OK, {"ok": True}

        if path.startswith("/v1/workers/") and path.endswith("/acquire") and method == "POST":
            worker_id = path.split("/")[3]
            wait_ms = int(body.get("wait_ms", 0))
            task = self._bridge.run(
                self._acquire_with_wait(worker_id, wait_ms),
                timeout_s=max(self.request_timeout_s, wait_ms / 1000 + 2),
            )
            return HTTPStatus.OK, {"task": task_to_payload(task) if task else None}

        if path.startswith("/v1/tasks/") and path.endswith("/complete") and method == "POST":
            task_id = path.split("/")[3]
            result = self._bridge.run(
                self.server.complete_task(
                    worker_id=str(body.get("worker_id", "")),
                    task_id=task_id,
                    response=str(body.get("response", "")),
                ),
                timeout_s=self.request_timeout_s,
            )
            return HTTPStatus.OK, {"result": result_to_payload(result)}

        if path.startswith("/v1/tasks/") and path.endswith("/fail") and method == "POST":
            task_id = path.split("/")[3]
            result = self._bridge.run(
                self.server.fail_task(
                    worker_id=str(body.get("worker_id", "")),
                    task_id=task_id,
                    error=str(body.get("error", "")),
                ),
                timeout_s=self.request_timeout_s,
            )
            return HTTPStatus.OK, {"result": result_to_payload(result)}

        if path == "/v1/sessions/hydrate" and method == "POST":
            session_key = str(body.get("session_key", ""))
            if not session_key:
                return HTTPStatus.BAD_REQUEST, {"error": "missing session_key"}
            with TemporaryDirectory(prefix="cluster-hydrate-") as td:
                workspace = Path(td) / "workspace"
                workspace.mkdir(parents=True, exist_ok=True)
                meta = self._bridge.run(
                    self.server.hydrate_workspace(session_key, workspace),
                    timeout_s=self.request_timeout_s,
                )
                bundle = workspace_to_bundle(workspace)
            return HTTPStatus.OK, {"meta": asdict(meta), "bundle": bundle}

        if path == "/v1/sessions/persist" and method == "POST":
            session_key = str(body.get("session_key", ""))
            bundle = str(body.get("bundle", ""))
            if not session_key:
                return HTTPStatus.BAD_REQUEST, {"error": "missing session_key"}
            with TemporaryDirectory(prefix="cluster-persist-") as td:
                workspace = Path(td) / "workspace"
                workspace.mkdir(parents=True, exist_ok=True)
                bundle_to_workspace(bundle, workspace)
                meta = self._bridge.run(
                    self.server.persist_workspace(session_key, workspace),
                    timeout_s=self.request_timeout_s,
                )
            return HTTPStatus.OK, {"meta": asdict(meta)}

        return HTTPStatus.NOT_FOUND, {"error": "not found"}

    async def _acquire_with_wait(self, worker_id: str, wait_ms: int) -> ClusterTask | None:
        if wait_ms <= 0:
            return await self.server.acquire_task(worker_id)
        deadline = time.monotonic() + wait_ms / 1000
        while True:
            task = await self.server.acquire_task(worker_id)
            if task is not None:
                return task
            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(0.1)

    def _read_json_body(self, req: BaseHTTPRequestHandler) -> dict[str, Any]:
        length = int(req.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = req.rfile.read(length)
        if not raw:
            return {}
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("request body must be a JSON object")
        return data

    def _is_authorized(self, req: BaseHTTPRequestHandler) -> bool:
        if not self.auth_token:
            return True
        header_token = req.headers.get("X-Cluster-Token", "")
        auth = req.headers.get("Authorization", "")
        if header_token == self.auth_token:
            return True
        if auth == f"Bearer {self.auth_token}":
            return True
        return False
