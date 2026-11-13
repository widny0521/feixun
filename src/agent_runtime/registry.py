"""Logical role to provider adapter registry."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agent_protocol import AgentCapabilities
    from agent_runtime.adapter import AgentAdapter


class ProviderNotFoundError(LookupError):
    """Raised when no adapter is registered for a logical role."""

    error_code = "provider_not_found"

    def __init__(self, role: str) -> None:
        self.role = role
        super().__init__(f"no provider registered for role {role!r}")


class ProviderRegistry:
    """Resolves logical workflow roles to concrete adapters.

    Workflows depend on roles (``default``, ``reviewer``, ...) rather than model
    names, so vendor/model selection stays in configuration and adapters.
    """

    def __init__(self) -> None:
        self._adapters: dict[str, AgentAdapter] = {}

    def register(self, role: str, adapter: AgentAdapter) -> None:
        if not role:
            raise ValueError("role must not be empty")
        if role in self._adapters:
            raise ValueError(f"provider already registered for role {role!r}")
        self._adapters[role] = adapter

    def resolve(self, role: str) -> AgentAdapter:
        try:
            return self._adapters[role]
        except KeyError:
            raise ProviderNotFoundError(role) from None

    def roles(self) -> tuple[str, ...]:
        return tuple(sorted(self._adapters))

    def capabilities(self) -> dict[str, AgentCapabilities]:
        return {role: adapter.capabilities for role, adapter in self._adapters.items()}
