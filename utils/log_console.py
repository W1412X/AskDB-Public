"""
控制台日志样式：ANSI 颜色 + 分类标签（TTY 下启用；文件日志不使用本模块）。
"""

from __future__ import annotations

import logging
import sys
from enum import Enum


def supports_ansi_console(stream=None) -> bool:
    stream = stream or sys.stdout
    if not hasattr(stream, "isatty") or not stream.isatty():
        return False
    if sys.platform == "win32":
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            mode = ctypes.c_uint()
            if kernel32.GetConsoleMode(kernel32.GetStdHandle(-11), ctypes.byref(mode)):
                return True
        except Exception:
            return False
    return True


class LogCategory(str, Enum):
    """语义分类，用于控制台标签着色（与 logger 名称独立）。"""

    DEFAULT = "default"
    AGENT = "agent"
    SCHEMA = "schema"
    INTENT = "intent"
    SQL = "sql"
    TOOL = "tool"
    ROUTING = "routing"
    SUCCESS = "success"
    WORKFLOW = "workflow"


class LoggerNameDefaultCategory:
    """未显式指定 log_category 时，按 logging logger name 推断。"""

    _MAP: dict[str, LogCategory] = {
        "query_workflow": LogCategory.AGENT,
        "schemalink": LogCategory.SCHEMA,
        "intent_executor": LogCategory.INTENT,
        "database_tool": LogCategory.TOOL,
    }

    @classmethod
    def for_name(cls, logger_name: str) -> LogCategory:
        return cls._MAP.get(logger_name, LogCategory.DEFAULT)


class LogPalette:
    """ANSI 调色板；无 TTY 时所有方法返回空串或纯文本。"""

    RESET = "\033[0m"
    DIM = "\033[2m"
    BOLD = "\033[1m"

    _LEVEL = {
        "DEBUG": "\033[36m",
        "INFO": "\033[37m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
    }

    _TAG_BG = {
        LogCategory.DEFAULT.value: "\033[90m",
        LogCategory.AGENT.value: "\033[94m",
        LogCategory.SCHEMA.value: "\033[96m",
        LogCategory.INTENT.value: "\033[92m",
        LogCategory.SQL.value: "\033[35m",
        LogCategory.TOOL.value: "\033[33m",
        LogCategory.ROUTING.value: "\033[93m",
        LogCategory.SUCCESS.value: "\033[32m",
        LogCategory.WORKFLOW.value: "\033[95m",
    }

    _TAG_LABEL = {
        LogCategory.DEFAULT.value: "---",
        LogCategory.AGENT.value: "AGT",
        LogCategory.SCHEMA.value: "SCH",
        LogCategory.INTENT.value: "INT",
        LogCategory.SQL.value: "SQL",
        LogCategory.TOOL.value: "TBL",
        LogCategory.ROUTING.value: "RTR",
        LogCategory.SUCCESS.value: "OK ",
        LogCategory.WORKFLOW.value: "WF ",
    }

    def __init__(self, enabled: bool) -> None:
        self._on = enabled

    @property
    def enabled(self) -> bool:
        return self._on

    def level(self, levelname: str) -> str:
        if not self._on:
            return ""
        return self._LEVEL.get(levelname, "")

    def tag(self, category_value: str) -> str:
        label = self._TAG_LABEL.get(category_value, category_value[:3].upper().ljust(3))
        if not self._on:
            return f"[{label}]"
        color = self._TAG_BG.get(category_value, self._TAG_BG[LogCategory.DEFAULT.value])
        return f"{self.DIM}{color}{self.BOLD}[{label}]{self.RESET}"

    def reset(self) -> str:
        return self.RESET if self._on else ""


class ColoredConsoleFormatter(logging.Formatter):
    """
    单行格式：时间 | 级别(着色) | 分类标签(着色) | 消息
    依赖 LogRecord.log_category（由 StructuredLogger 写入）。
    """

    def __init__(self, *, use_color: bool | None = None, datefmt: str = "%H:%M:%S") -> None:
        super().__init__(datefmt=datefmt)
        if use_color is None:
            use_color = supports_ansi_console()
        self._palette = LogPalette(use_color)

    def format(self, record: logging.LogRecord) -> str:
        from datetime import datetime

        ts = datetime.fromtimestamp(record.created).strftime(self.datefmt or "%H:%M:%S")
        level = record.levelname
        cat_raw = getattr(record, "log_category", None)
        if not cat_raw or not isinstance(cat_raw, str):
            cat = LoggerNameDefaultCategory.for_name(record.name).value
        else:
            cat = cat_raw
        try:
            msg = record.getMessage()
        except Exception:
            msg = record.msg if isinstance(record.msg, str) else str(record.msg)

        lv_color = self._palette.level(level)
        reset = self._palette.reset()
        tag = self._palette.tag(cat)
        level_plain = f"{level:7}"
        if self._palette.enabled:
            level_part = f"{lv_color}{self._palette.BOLD}{level_plain}{reset}"
        else:
            level_part = level_plain
        return f"{ts} | {level_part} | {tag} {msg}"
