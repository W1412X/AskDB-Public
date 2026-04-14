"""
Init job state and log capture for Web UI.
Single in-process state: status, phase, message, logs[]. Thread-safe.
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List

_lock = threading.Lock()
_status: str = "idle"  # idle | running | success | failed
_phase: str = ""
_message: str = ""
_logs: List[Dict[str, Any]] = []
_error: str = ""


def get_state() -> Dict[str, Any]:
    with _lock:
        return {
            "status": _status,
            "phase": _phase,
            "message": _message,
            "logs": list(_logs),
            "error": _error,
        }


def set_phase(phase: str, message: str = "") -> None:
    global _phase, _message
    with _lock:
        _phase = phase
        _message = message or phase


def append_log(level: str, message: str) -> None:
    with _lock:
        _logs.append({"level": level, "message": message})


def set_status(status: str, error: str = "") -> None:
    global _status, _error
    with _lock:
        _status = status
        _error = error


def clear_and_start() -> None:
    global _status, _phase, _message, _logs, _error
    with _lock:
        _status = "running"
        _phase = "preparing"
        _message = "准备初始化..."
        _logs = []
        _error = ""


class InitLogHandler(logging.Handler):
    """Capture log records into init_state._logs when init is running."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            level = (record.levelname or "INFO").lower()
            append_log(level, msg)
        except Exception:
            self.handleError(record)
