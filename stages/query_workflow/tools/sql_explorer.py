from __future__ import annotations

from typing import Any
from datetime import date, datetime
from decimal import Decimal

from utils.database_tool import _db_tool


def _sql_explore_defaults() -> tuple[int, int]:
    from config import get_settings_manager

    d = get_settings_manager().config.stages.sql_explorer
    return int(d.default_limit), int(d.default_timeout_ms)


class SqlExploreTool:
    name = "sql_explorer"

    def invoke(self, payload: dict[str, Any]) -> dict[str, Any]:
        sql = str(payload.get("sql") or "").strip()
        database = str(payload.get("database") or "").strip() or None
        def_limit, def_timeout = _sql_explore_defaults()
        limit = int(payload.get("limit") or def_limit)
        timeout_ms = int(payload.get("timeout_ms") or def_timeout)
        if not sql.lower().startswith(("select", "with")):
            raise ValueError("SqlExploreTool only allows SELECT/WITH")
        if _db_tool is None:
            raise RuntimeError("global database tool is not initialized")
        rows = _db_tool.execute_query(sql=sql, database=database, readonly=True, timeout_ms=timeout_ms)
        raw_rows = rows[:limit]
        columns = list(raw_rows[0].keys()) if raw_rows else []
        data_rows = [[self._serialize_value(row.get(col)) for col in columns] for row in raw_rows]
        return {"columns": columns, "rows": data_rows, "truncated": len(rows) > limit}

    def _serialize_value(self, value: Any) -> Any:
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        return value
