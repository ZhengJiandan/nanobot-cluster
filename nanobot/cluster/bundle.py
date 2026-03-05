"""Workspace snapshot bundle helpers for HTTP transport."""

from __future__ import annotations

import base64
import io
import zipfile
from pathlib import Path


def workspace_to_bundle(workspace: Path) -> str:
    """Pack workspace files into a base64-encoded zip bundle."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in workspace.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(workspace).as_posix()
            zf.write(path, rel)
    data = buf.getvalue()
    if not data:
        return ""
    return base64.b64encode(data).decode("ascii")


def bundle_to_workspace(bundle_b64: str, workspace: Path) -> None:
    """Extract a base64-encoded zip bundle into workspace."""
    workspace.mkdir(parents=True, exist_ok=True)
    if not bundle_b64:
        return
    raw = base64.b64decode(bundle_b64.encode("ascii"))
    with zipfile.ZipFile(io.BytesIO(raw), "r") as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            name = member.filename
            target = workspace / name
            resolved = target.resolve()
            workspace_root = workspace.resolve()
            if workspace_root not in resolved.parents and resolved != workspace_root:
                raise ValueError(f"invalid bundle path: {name}")
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(member, "r") as src, open(target, "wb") as dst:
                dst.write(src.read())

