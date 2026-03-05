"""Task executors used by cluster workers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from nanobot.agent.loop import AgentLoop
from nanobot.bus.queue import MessageBus
from nanobot.config.loader import load_config
from nanobot.config.schema import Config
from nanobot.providers.base import LLMProvider
from nanobot.session.manager import SessionManager
from nanobot.utils.helpers import sync_workspace_templates

from nanobot.cluster.interfaces import ClusterTaskExecutor
from nanobot.cluster.models import ClusterTask
from nanobot.cluster.provider_factory import make_provider_from_config


@dataclass(slots=True)
class NanobotExecutorConfig:
    """Runtime knobs for AgentLoop execution in worker mode."""

    model: str
    temperature: float = 0.1
    max_tokens: int = 4096
    max_iterations: int = 40
    memory_window: int = 100
    reasoning_effort: str | None = None
    brave_api_key: str | None = None
    web_proxy: str | None = None
    restrict_to_workspace: bool = True
    exec_config: object | None = None
    channels_config: object | None = None
    mcp_servers: dict | None = None

    @classmethod
    def from_config(cls, config: Config) -> "NanobotExecutorConfig":
        defaults = config.agents.defaults
        return cls(
            model=defaults.model,
            temperature=defaults.temperature,
            max_tokens=defaults.max_tokens,
            max_iterations=defaults.max_tool_iterations,
            memory_window=defaults.memory_window,
            reasoning_effort=defaults.reasoning_effort,
            brave_api_key=config.tools.web.search.api_key or None,
            web_proxy=config.tools.web.proxy or None,
            restrict_to_workspace=True,
            exec_config=config.tools.exec,
            channels_config=config.channels,
            mcp_servers=config.tools.mcp_servers,
        )


class NanobotTaskExecutor:
    """Default executor that runs a task with AgentLoop in a temp workspace."""

    def __init__(
        self,
        provider_factory: Callable[[], LLMProvider],
        runtime: NanobotExecutorConfig,
    ):
        self.provider_factory = provider_factory
        self.runtime = runtime

    @classmethod
    def from_default_config(cls, config: Config | None = None) -> "NanobotTaskExecutor":
        cfg = config or load_config()
        runtime = NanobotExecutorConfig.from_config(cfg)
        return cls(provider_factory=lambda: make_provider_from_config(cfg), runtime=runtime)

    async def execute(self, workspace: Path, task: ClusterTask) -> str:
        sync_workspace_templates(workspace, silent=True)
        provider = self.provider_factory()
        bus = MessageBus()
        loop = AgentLoop(
            bus=bus,
            provider=provider,
            workspace=workspace,
            model=self.runtime.model,
            temperature=self.runtime.temperature,
            max_tokens=self.runtime.max_tokens,
            max_iterations=self.runtime.max_iterations,
            memory_window=self.runtime.memory_window,
            reasoning_effort=self.runtime.reasoning_effort,
            brave_api_key=self.runtime.brave_api_key,
            web_proxy=self.runtime.web_proxy,
            exec_config=self.runtime.exec_config,
            restrict_to_workspace=self.runtime.restrict_to_workspace,
            session_manager=SessionManager(workspace),
            mcp_servers=self.runtime.mcp_servers,
            channels_config=self.runtime.channels_config,
        )

        async def _silent_progress(*_args, **_kwargs) -> None:
            return None

        try:
            return await loop.process_direct(
                task.content,
                session_key=task.session_key,
                channel=task.channel,
                chat_id=task.chat_id,
                on_progress=_silent_progress,
            )
        finally:
            await loop.close_mcp()
