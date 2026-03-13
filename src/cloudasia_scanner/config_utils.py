from __future__ import annotations

from pathlib import Path
import os
import tomllib
from typing import Any


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_config_path() -> Path:
    env_path = os.getenv("CLOUDASIA_CONFIG")
    if env_path:
        return Path(env_path).expanduser().resolve()
    return project_root() / "config.toml"


def load_toml_config(path: Path | None = None) -> tuple[dict[str, Any], Path]:
    config_path = path.resolve() if path is not None else default_config_path()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("rb") as fp:
        data = tomllib.load(fp)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid config format in {config_path}")
    return data, config_path


def as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("1", "true", "yes", "y", "on"):
            return True
        if lowered in ("0", "false", "no", "n", "off"):
            return False
    return default


def resolve_path(base_dir: Path, value: Any, default: str | None = None) -> Path | None:
    raw = value
    if raw is None:
        raw = default
    if raw is None:
        return None
    if not isinstance(raw, str):
        raw = str(raw)
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()
