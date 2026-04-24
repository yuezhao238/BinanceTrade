from __future__ import annotations

import json
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .types import MarketType, SubmissionMode


@dataclass(frozen=True, slots=True)
class DaemonSettings:
    reconcile_on_start: bool = True
    reconcile_interval_seconds: int = 300
    heartbeat_interval_seconds: int = 30
    auto_restart: bool = True
    restart_initial_delay_seconds: int = 5
    restart_max_delay_seconds: int = 60
    stop_on_strategy_exit: bool = False
    stale_after_seconds: int = 90

    def __post_init__(self) -> None:
        values = {
            "reconcile_interval_seconds": self.reconcile_interval_seconds,
            "heartbeat_interval_seconds": self.heartbeat_interval_seconds,
            "restart_initial_delay_seconds": self.restart_initial_delay_seconds,
            "restart_max_delay_seconds": self.restart_max_delay_seconds,
            "stale_after_seconds": self.stale_after_seconds,
        }
        for name, value in values.items():
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.restart_max_delay_seconds < self.restart_initial_delay_seconds:
            raise ValueError("restart_max_delay_seconds must be >= restart_initial_delay_seconds")


@dataclass(frozen=True, slots=True)
class RuntimeProfile:
    name: str
    market: MarketType
    strategy_ref: str
    params: dict[str, Any] = field(default_factory=dict)
    submission_mode: SubmissionMode | None = None
    description: str = ""
    notes: tuple[str, ...] = ()
    daemon: DaemonSettings = field(default_factory=DaemonSettings)
    path: Path | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("profile name must not be empty")
        if not self.strategy_ref.strip():
            raise ValueError("strategy_ref must not be empty")

    def resolve_submission_mode(self, override: SubmissionMode | None, *, default_dry_run: bool) -> SubmissionMode:
        if override is not None:
            return override
        if self.submission_mode is not None:
            return self.submission_mode
        return SubmissionMode.DRY_RUN if default_dry_run else SubmissionMode.LIVE

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "market": self.market.value,
            "strategy_ref": self.strategy_ref,
            "params": dict(self.params),
            "submission_mode": None if self.submission_mode is None else self.submission_mode.value,
            "description": self.description,
            "notes": list(self.notes),
            "daemon": {
                "reconcile_on_start": self.daemon.reconcile_on_start,
                "reconcile_interval_seconds": self.daemon.reconcile_interval_seconds,
                "heartbeat_interval_seconds": self.daemon.heartbeat_interval_seconds,
                "auto_restart": self.daemon.auto_restart,
                "restart_initial_delay_seconds": self.daemon.restart_initial_delay_seconds,
                "restart_max_delay_seconds": self.daemon.restart_max_delay_seconds,
                "stop_on_strategy_exit": self.daemon.stop_on_strategy_exit,
                "stale_after_seconds": self.daemon.stale_after_seconds,
            },
            "path": None if self.path is None else str(self.path),
        }


@dataclass(frozen=True, slots=True)
class RuntimeStackSettings:
    heartbeat_interval_seconds: int = 30
    stale_after_seconds: int = 90
    stop_on_member_exit: bool = True
    stop_on_member_failure: bool = True

    def __post_init__(self) -> None:
        if self.heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive")
        if self.stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")


@dataclass(frozen=True, slots=True)
class RuntimeStack:
    name: str
    profiles: tuple[RuntimeProfile, ...]
    description: str = ""
    notes: tuple[str, ...] = ()
    settings: RuntimeStackSettings = field(default_factory=RuntimeStackSettings)
    path: Path | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("stack name must not be empty")
        if not self.profiles:
            raise ValueError("stack must include at least one runtime profile")
        names = [profile.name for profile in self.profiles]
        if len(set(names)) != len(names):
            raise ValueError("stack profile names must be unique")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "notes": list(self.notes),
            "settings": {
                "heartbeat_interval_seconds": self.settings.heartbeat_interval_seconds,
                "stale_after_seconds": self.settings.stale_after_seconds,
                "stop_on_member_exit": self.settings.stop_on_member_exit,
                "stop_on_member_failure": self.settings.stop_on_member_failure,
            },
            "profiles": [profile.to_dict() for profile in self.profiles],
            "path": None if self.path is None else str(self.path),
        }


def load_runtime_profile(path: str | Path) -> RuntimeProfile:
    profile_path = Path(path).expanduser()
    if not profile_path.exists():
        raise ValueError(f"runtime profile {profile_path} does not exist")

    payload = _load_mapping_file(profile_path)
    if not isinstance(payload, dict):
        raise ValueError("runtime profile must decode to a mapping")

    daemon_payload = payload.get("daemon", {})
    if daemon_payload is None:
        daemon_payload = {}
    if not isinstance(daemon_payload, dict):
        raise ValueError("[daemon] must decode to a mapping")

    params = payload.get("params", {})
    if params is None:
        params = {}
    if not isinstance(params, dict):
        raise ValueError("[params] must decode to a mapping")

    submission_mode = payload.get("submission_mode")
    profile = RuntimeProfile(
        name=str(payload.get("name", profile_path.stem)).strip(),
        market=MarketType(str(payload.get("market", "spot")).lower()),
        strategy_ref=str(payload.get("strategy_ref", "")).strip(),
        params=dict(params),
        submission_mode=None if submission_mode in (None, "", "inherit") else SubmissionMode(str(submission_mode).upper()),
        description=str(payload.get("description", "")),
        notes=tuple(str(item) for item in payload.get("notes", []) or []),
        daemon=DaemonSettings(
            reconcile_on_start=bool(daemon_payload.get("reconcile_on_start", True)),
            reconcile_interval_seconds=int(daemon_payload.get("reconcile_interval_seconds", 300)),
            heartbeat_interval_seconds=int(daemon_payload.get("heartbeat_interval_seconds", 30)),
            auto_restart=bool(daemon_payload.get("auto_restart", True)),
            restart_initial_delay_seconds=int(daemon_payload.get("restart_initial_delay_seconds", 5)),
            restart_max_delay_seconds=int(daemon_payload.get("restart_max_delay_seconds", 60)),
            stop_on_strategy_exit=bool(daemon_payload.get("stop_on_strategy_exit", False)),
            stale_after_seconds=int(daemon_payload.get("stale_after_seconds", 90)),
        ),
        path=profile_path.resolve(),
    )
    return profile


def load_runtime_stack(path: str | Path) -> RuntimeStack:
    stack_path = Path(path).expanduser()
    if not stack_path.exists():
        raise ValueError(f"runtime stack {stack_path} does not exist")

    payload = _load_mapping_file(stack_path)
    if not isinstance(payload, dict):
        raise ValueError("runtime stack must decode to a mapping")

    profile_refs = payload.get("profiles", [])
    if not isinstance(profile_refs, list):
        raise ValueError("profiles must decode to a list")
    profiles = tuple(_load_profile_from_stack_item(stack_path, item) for item in profile_refs)

    settings_payload = payload.get("settings", {})
    if settings_payload is None:
        settings_payload = {}
    if not isinstance(settings_payload, dict):
        raise ValueError("[settings] must decode to a mapping")

    return RuntimeStack(
        name=str(payload.get("name", stack_path.stem)).strip(),
        profiles=profiles,
        description=str(payload.get("description", "")),
        notes=tuple(str(item) for item in payload.get("notes", []) or []),
        settings=RuntimeStackSettings(
            heartbeat_interval_seconds=int(settings_payload.get("heartbeat_interval_seconds", 30)),
            stale_after_seconds=int(settings_payload.get("stale_after_seconds", 90)),
            stop_on_member_exit=bool(settings_payload.get("stop_on_member_exit", True)),
            stop_on_member_failure=bool(settings_payload.get("stop_on_member_failure", True)),
        ),
        path=stack_path.resolve(),
    )


def _load_mapping_file(path: Path) -> dict[str, Any]:
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
    else:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must decode to a mapping")
    return payload


def _load_profile_from_stack_item(stack_path: Path, item: Any) -> RuntimeProfile:
    if not isinstance(item, str) or not item.strip():
        raise ValueError("stack profiles must be non-empty file path strings")
    profile_path = (stack_path.parent / item).resolve() if not Path(item).expanduser().is_absolute() else Path(item).expanduser()
    return load_runtime_profile(profile_path)
