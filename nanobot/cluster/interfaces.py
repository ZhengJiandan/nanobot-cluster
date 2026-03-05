"""Lightweight interfaces shared across cluster modules."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from nanobot.cluster.models import ClusterTask


class ClusterTaskExecutor(Protocol):
    """Interface implemented by worker executors."""

    async def execute(self, workspace: Path, task: ClusterTask) -> str:
        """Execute one cluster task and return assistant response."""

