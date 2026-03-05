"""Centralized session state store used by cluster server/workers."""

from __future__ import annotations

import asyncio
import json
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from nanobot.utils.helpers import ensure_dir, safe_filename


@dataclass(slots=True)
class SessionStateMeta:
    """Session snapshot metadata."""

    session_key: str
    version: int = 0
    updated_at: str | None = None


class SessionStateStore(Protocol):
    """Abstract storage operations for centralized session state."""

    async def hydrate_session(self, session_key: str, workspace: Path) -> SessionStateMeta:
        """Restore one session snapshot into a worker workspace."""

    async def persist_session(self, session_key: str, workspace: Path) -> SessionStateMeta:
        """Persist one worker workspace back into centralized storage."""


class FileSessionStateStore:
    """
    File-backed session store.

    Layout:
      root/
        <safe_session_key>/
          .meta.json
          *.md
          memory/
          sessions/
    """

    def __init__(
        self,
        root: Path,
        *,
        sync_dirs: tuple[str, ...] = ("memory", "sessions"),
    ):
        self.root = ensure_dir(root)
        self.sync_dirs = sync_dirs
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def _session_dir(self, session_key: str) -> Path:
        safe_key = safe_filename(session_key.replace(":", "_"))
        return self.root / safe_key

    @staticmethod
    def _meta_path(session_dir: Path) -> Path:
        return session_dir / ".meta.json"

    @staticmethod
    def _load_meta(session_dir: Path, session_key: str) -> SessionStateMeta:
        path = FileSessionStateStore._meta_path(session_dir)
        if not path.exists():
            return SessionStateMeta(session_key=session_key)
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return SessionStateMeta(
                session_key=session_key,
                version=int(data.get("version", 0)),
                updated_at=data.get("updated_at"),
            )
        except Exception:
            return SessionStateMeta(session_key=session_key)

    @staticmethod
    def _save_meta(session_dir: Path, meta: SessionStateMeta) -> None:
        payload = {
            "session_key": meta.session_key,
            "version": meta.version,
            "updated_at": meta.updated_at,
        }
        FileSessionStateStore._meta_path(session_dir).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _copy_file(src: Path, dst: Path) -> None:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    @staticmethod
    def _replace_tree(src: Path, dst: Path) -> None:
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)

    async def hydrate_session(self, session_key: str, workspace: Path) -> SessionStateMeta:
        """Restore session snapshot into workspace before a task runs."""
        lock = self._locks[session_key]
        async with lock:
            workspace.mkdir(parents=True, exist_ok=True)
            session_dir = self._session_dir(session_key)
            meta = self._load_meta(session_dir, session_key)
            if not session_dir.exists():
                return meta

            for src in session_dir.glob("*.md"):
                self._copy_file(src, workspace / src.name)

            for rel in self.sync_dirs:
                src = session_dir / rel
                if src.is_dir():
                    self._replace_tree(src, workspace / rel)
            return meta

    async def persist_session(self, session_key: str, workspace: Path) -> SessionStateMeta:
        """Persist workspace snapshot into centralized session storage."""
        lock = self._locks[session_key]
        async with lock:
            session_dir = ensure_dir(self._session_dir(session_key))
            meta = self._load_meta(session_dir, session_key)

            source_md = {p.name for p in workspace.glob("*.md") if p.is_file()}
            for name in source_md:
                self._copy_file(workspace / name, session_dir / name)
            for stale in session_dir.glob("*.md"):
                if stale.name not in source_md:
                    stale.unlink()

            for rel in self.sync_dirs:
                src = workspace / rel
                dst = session_dir / rel
                if src.is_dir():
                    self._replace_tree(src, dst)
                elif dst.exists():
                    if dst.is_dir():
                        shutil.rmtree(dst)
                    else:
                        dst.unlink()

            meta.version += 1
            meta.updated_at = datetime.now().isoformat()
            self._save_meta(session_dir, meta)
            return meta
