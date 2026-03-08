from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass
class EmailConfig:
    enabled: bool
    smtp_host: str
    smtp_port: int
    use_ssl: bool
    username: str
    password: str
    sender: str
    recipient: str


@dataclass
class NotificationConfig:
    macos_enabled: bool
    title_prefix: str


@dataclass
class TrackerConfig:
    config_path: Path
    db_path: Path
    snapshot_dir: Path
    default_drop_threshold_pct: float
    daily_run_time: str
    price_provider: str
    email: EmailConfig
    notifications: NotificationConfig


def load_tracker_config(config_path: str | Path | None = None) -> TrackerConfig:
    project_root = Path(__file__).resolve().parents[2]
    config_file = Path(config_path).expanduser().resolve() if config_path else (
        project_root / "config" / "fund_tracker.yaml"
    )

    raw = _load_yaml(config_file) if config_file.exists() else {}
    storage = raw.get("storage", {})
    pricing = raw.get("pricing", {})
    monitor = raw.get("monitor", {})
    schedule = raw.get("schedule", {})
    email_raw = raw.get("email", {})
    notifications_raw = raw.get("notifications", {})

    password_env = email_raw.get("password_env", "FUND_TRACKER_SMTP_PASSWORD")
    email_password = os.getenv(password_env, "")

    db_path = _resolve_path(storage.get("db_path", "data/fund_tracker.db"), project_root)
    snapshot_dir = _resolve_path(
        storage.get("snapshot_dir", "data/fund_tracker_snapshots"),
        project_root,
    )

    config = TrackerConfig(
        config_path=config_file,
        db_path=db_path,
        snapshot_dir=snapshot_dir,
        default_drop_threshold_pct=float(monitor.get("default_drop_threshold_pct", 1.5)),
        daily_run_time=str(schedule.get("daily_run_time", "21:30")),
        price_provider=str(pricing.get("provider", "eastmoney_pingzhongdata")),
        email=EmailConfig(
            enabled=bool(email_raw.get("enabled", False)),
            smtp_host=str(email_raw.get("smtp_host", "")),
            smtp_port=int(email_raw.get("smtp_port", 465)),
            use_ssl=bool(email_raw.get("use_ssl", True)),
            username=str(email_raw.get("username", "")),
            password=email_password,
            sender=str(email_raw.get("sender", "")),
            recipient=str(email_raw.get("recipient", "")),
        ),
        notifications=NotificationConfig(
            macos_enabled=bool(notifications_raw.get("macos_enabled", False)),
            title_prefix=str(notifications_raw.get("title_prefix", "Fund Tracker")),
        ),
    )

    config.db_path.parent.mkdir(parents=True, exist_ok=True)
    config.snapshot_dir.mkdir(parents=True, exist_ok=True)
    return config


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"配置文件格式错误：{path}")
    return data


def _resolve_path(value: str, repo_root: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (repo_root / path).resolve()
    return path
