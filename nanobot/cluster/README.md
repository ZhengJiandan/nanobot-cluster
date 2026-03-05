# nanobot Cluster Runtime

`nanobot.cluster` adds a deployable control plane + worker plane without
changing core nanobot agent code.

## What is included

- Cluster scheduler (`ClusterServer`): queue, worker registry, per-session
  serialization.
- Centralized state store (`FileSessionStateStore`).
- HTTP protocol service (`ClusterHTTPService`) for cross-node communication.
- Remote worker daemon (`HTTPClusterWorker`) that:
  1. pulls tasks from cluster-server
  2. hydrates session snapshot to a temp workspace
  3. executes task via `AgentLoop`
  4. persists updated snapshot back to cluster-server

## CLI

Run with:

```bash
python -m nanobot.cluster --help
```

### Start cluster-server

```bash
python -m nanobot.cluster server \
  --host 0.0.0.0 \
  --port 18880 \
  --state-dir ~/.nanobot/cluster-state
```

Optional auth token:

```bash
export NANOBOT_CLUSTER_TOKEN="replace-me"
python -m nanobot.cluster server --port 18880
```

### Start worker node(s)

Run this on each worker machine (can be same machine for local scaling):

```bash
python -m nanobot.cluster worker \
  --server-url http://<server-ip>:18880 \
  --worker-id worker-a \
  --max-concurrency 1
```

If token is enabled:

```bash
export NANOBOT_CLUSTER_TOKEN="replace-me"
python -m nanobot.cluster worker --server-url http://<server-ip>:18880 --worker-id worker-a
```

### Submit request (smoke test)

```bash
python -m nanobot.cluster submit \
  --server-url http://127.0.0.1:18880 \
  --channel telegram \
  --chat-id 12345 \
  --content "hello" \
  --wait
```

### Health check

```bash
python -m nanobot.cluster health --server-url http://127.0.0.1:18880
```

## Single-node -> cluster migration checklist

1. Keep current nanobot config for provider/model/tool settings on each worker.
2. Deploy one `cluster server` service with persistent `--state-dir`.
3. Deploy multiple `cluster worker` services on one or many machines.
4. Route inbound messages to `POST /v1/tasks/submit` or use
   `python -m nanobot.cluster submit` equivalent in your gateway adapter.
5. Use `submit_and_wait` API where synchronous response is needed.

## HTTP API (summary)

- `GET /v1/health`
- `POST /v1/tasks/submit`
- `POST /v1/tasks/submit_and_wait`
- `GET /v1/tasks/{task_id}`
- `POST /v1/workers/register`
- `POST /v1/workers/{worker_id}/heartbeat`
- `POST /v1/workers/{worker_id}/acquire`
- `POST /v1/workers/{worker_id}/unregister`
- `POST /v1/tasks/{task_id}/complete`
- `POST /v1/tasks/{task_id}/fail`
- `POST /v1/sessions/hydrate`
- `POST /v1/sessions/persist`

