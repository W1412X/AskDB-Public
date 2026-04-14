"""
全局配置管理模块。
"""

from .app_config import AppConfig, get_app_config, get_config_dir, reload_app_config
from .settings_manager import AppSettingsManager, get_settings_manager


def get_llm(model_code: str | None = None):
    from .llm_config import get_llm as _get_llm

    return _get_llm(model_code)


def get_llm_model_spec(model_code: str | None = None):
    from .llm_config import get_llm_model_spec as _get_llm_model_spec

    return _get_llm_model_spec(model_code)


__all__ = [
    "AppConfig",
    "AppSettingsManager",
    "get_app_config",
    "get_settings_manager",
    "get_config_dir",
    "reload_app_config",
    "get_llm",
    "get_llm_model_spec",
]
