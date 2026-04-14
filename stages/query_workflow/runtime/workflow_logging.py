"""
Query workflow 侧结构化控制台日志：统一消息键与 LogCategory，便于着色与检索。
"""

from __future__ import annotations

from typing import Any

from utils.log_console import LogCategory
from utils.logger import StructuredLogger


class IntentExecutionLogger:
    """封装 intent_executor 常用日志形态，避免散落重复字符串。"""

    def __init__(self, logger: StructuredLogger) -> None:
        self._l = logger

    def execute_start(self, intent_id: str, phase: str) -> None:
        self._l.info("intent execute start", intent_id=intent_id, phase=phase, category=LogCategory.INTENT)

    def phase(self, intent_id: str, phase: str, **kwargs: Any) -> None:
        self._l.info("intent phase", intent_id=intent_id, phase=phase, category=LogCategory.INTENT, **kwargs)

    def ra_plan_ready(self, intent_id: str, **kwargs: Any) -> None:
        self._l.info("intent ra result", intent_id=intent_id, category=LogCategory.INTENT, **kwargs)

    def sql_render_result(self, intent_id: str, **kwargs: Any) -> None:
        self._l.info("intent sql render result", intent_id=intent_id, category=LogCategory.SQL, **kwargs)

    def sql_validation(self, intent_id: str, **kwargs: Any) -> None:
        self._l.info("intent sql validation result", intent_id=intent_id, category=LogCategory.SQL, **kwargs)

    def phase_error(self, intent_id: str, phase: str, error: str) -> None:
        self._l.warning("intent phase error", intent_id=intent_id, phase=phase, error=error, category=LogCategory.INTENT)

    def error_routed(self, intent_id: str, **kwargs: Any) -> None:
        self._l.info("intent error routing result", intent_id=intent_id, category=LogCategory.ROUTING, **kwargs)


class SchemaLinkRuntimeLogger:
    """SchemaLink 引擎日志：默认分类 SCHEMA，成功类事件可显式传入 SUCCESS。"""

    def __init__(self, logger: StructuredLogger) -> None:
        self._l = logger

    def info(self, message: str, **kwargs: Any) -> None:
        kwargs.setdefault("category", LogCategory.SCHEMA)
        self._l.info(message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        kwargs.setdefault("category", LogCategory.SCHEMA)
        self._l.warning(message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        kwargs.setdefault("category", LogCategory.SCHEMA)
        self._l.error(message, **kwargs)
