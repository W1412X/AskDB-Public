"""Unified entry point for all AskDB tunables loaded from JSON (see CONFIGURATION_REFERENCE.md)."""

from __future__ import annotations

from config.app_config import AppConfig, get_app_config


class AppSettingsManager:
    """Thin facade over `AppConfig` so application code depends on one accessor."""

    __slots__ = ("_cfg",)

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg

    @property
    def config(self) -> AppConfig:
        return self._cfg


def get_settings_manager() -> AppSettingsManager:
    return AppSettingsManager(get_app_config())
