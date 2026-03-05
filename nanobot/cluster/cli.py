"""CLI entrypoint for cluster server/worker deployment."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
from pathlib import Path

from loguru import logger

from nanobot.cluster.executor import NanobotTaskExecutor
from nanobot.cluster.http_api import ClusterHTTPService
from nanobot.cluster.http_client import ClusterHTTPClient, ClusterHTTPClientConfig
from nanobot.cluster.http_worker import HTTPClusterWorker, HTTPWorkerConfig
from nanobot.cluster.server import ClusterServer
from nanobot.cluster.state_store import FileSessionStateStore


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="nanobot cluster server/worker")
    sub = parser.add_subparsers(dest="command", required=True)

    p_server = sub.add_parser("server", help="run cluster HTTP server")
    p_server.add_argument("--host", default="0.0.0.0")
    p_server.add_argument("--port", type=int, default=18880)
    p_server.add_argument("--state-dir", default="~/.nanobot/cluster-state")
    p_server.add_argument("--worker-timeout", type=int, default=60)
    p_server.add_argument("--token", default=os.environ.get("NANOBOT_CLUSTER_TOKEN", ""))

    p_worker = sub.add_parser("worker", help="run cluster remote worker daemon")
    p_worker.add_argument("--server-url", default="http://127.0.0.1:18880")
    p_worker.add_argument("--worker-id", default="")
    p_worker.add_argument("--token", default=os.environ.get("NANOBOT_CLUSTER_TOKEN", ""))
    p_worker.add_argument("--max-concurrency", type=int, default=1)
    p_worker.add_argument("--poll-interval", type=float, default=0.2)
    p_worker.add_argument("--heartbeat-interval", type=float, default=5.0)
    p_worker.add_argument("--acquire-wait-ms", type=int, default=1500)
    p_worker.add_argument("--temp-root", default="")
    p_worker.add_argument("--request-timeout", type=float, default=60.0)

    p_submit = sub.add_parser("submit", help="submit one task to cluster server")
    p_submit.add_argument("--server-url", default="http://127.0.0.1:18880")
    p_submit.add_argument("--token", default=os.environ.get("NANOBOT_CLUSTER_TOKEN", ""))
    p_submit.add_argument("--channel", required=True)
    p_submit.add_argument("--chat-id", required=True)
    p_submit.add_argument("--content", required=True)
    p_submit.add_argument("--metadata", default="{}")
    p_submit.add_argument("--wait", action="store_true")
    p_submit.add_argument("--timeout", type=float, default=120.0)

    p_health = sub.add_parser("health", help="check cluster server health")
    p_health.add_argument("--server-url", default="http://127.0.0.1:18880")
    p_health.add_argument("--token", default=os.environ.get("NANOBOT_CLUSTER_TOKEN", ""))

    return parser


def run_server(args: argparse.Namespace) -> None:
    state_dir = Path(args.state_dir).expanduser()
    store = FileSessionStateStore(state_dir)
    scheduler = ClusterServer(store, worker_timeout_s=args.worker_timeout)
    token = args.token or None
    service = ClusterHTTPService(
        scheduler,
        host=args.host,
        port=args.port,
        auth_token=token,
    )
    try:
        service.serve_forever()
    except KeyboardInterrupt:
        logger.info("cluster server interrupted, shutting down")


async def run_worker(args: argparse.Namespace) -> None:
    worker_id = args.worker_id.strip() or f"{socket.gethostname()}-{os.getpid()}"
    config = HTTPWorkerConfig(
        server_url=args.server_url,
        worker_id=worker_id,
        token=args.token or None,
        max_concurrency=max(1, args.max_concurrency),
        poll_interval_s=max(0.05, args.poll_interval),
        heartbeat_interval_s=max(1.0, args.heartbeat_interval),
        acquire_wait_ms=max(0, args.acquire_wait_ms),
        temp_root=Path(args.temp_root).expanduser() if args.temp_root else None,
        request_timeout_s=max(5.0, args.request_timeout),
    )
    executor = NanobotTaskExecutor.from_default_config()
    worker = HTTPClusterWorker(config=config, executor=executor)
    try:
        await worker.run()
    except KeyboardInterrupt:
        worker.stop()


async def run_submit(args: argparse.Namespace) -> None:
    metadata = json.loads(args.metadata)
    client = ClusterHTTPClient(
        ClusterHTTPClientConfig(
            server_url=args.server_url,
            token=args.token or None,
            timeout_s=max(args.timeout, 5.0),
        )
    )
    try:
        if args.wait:
            result = await client.submit_and_wait(
                channel=args.channel,
                chat_id=args.chat_id,
                content=args.content,
                metadata=metadata,
                timeout_s=args.timeout,
            )
            print(json.dumps({"result": result.__dict__}, ensure_ascii=False, indent=2, default=str))
        else:
            task_id = await client.submit(
                channel=args.channel,
                chat_id=args.chat_id,
                content=args.content,
                metadata=metadata,
            )
            print(json.dumps({"task_id": task_id}, ensure_ascii=False, indent=2))
    finally:
        await client.aclose()


async def run_health(args: argparse.Namespace) -> None:
    client = ClusterHTTPClient(
        ClusterHTTPClientConfig(
            server_url=args.server_url,
            token=args.token or None,
            timeout_s=10.0,
        )
    )
    try:
        data = await client.health()
        print(json.dumps(data, ensure_ascii=False, indent=2))
    finally:
        await client.aclose()


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "server":
        run_server(args)
        return
    if args.command == "worker":
        asyncio.run(run_worker(args))
        return
    if args.command == "submit":
        asyncio.run(run_submit(args))
        return
    if args.command == "health":
        asyncio.run(run_health(args))
        return
    parser.error(f"unknown command {args.command}")
