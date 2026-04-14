from __future__ import annotations

from utils.database_tool import _db_tool

from ..contracts import SQLExecutionResult


class SQLExecutor:
    def execute(self, sql: str, database_scope: list[str], timeout_ms: int, max_rows: int) -> SQLExecutionResult:
        if _db_tool is None:
            return SQLExecutionResult(status="FAILED", execution_message="global database tool is not initialized")
        database = database_scope[0] if len(database_scope) == 1 else None
        try:
            rows = _db_tool.execute_query(sql=sql, database=database, readonly=True, timeout_ms=timeout_ms)
            trimmed = rows[:max_rows]
            columns = list(trimmed[0].keys()) if trimmed else []
            data_rows = [[row.get(col) for col in columns] for row in trimmed]
            return SQLExecutionResult(
                status="SUCCESS",
                columns=columns,
                rows=data_rows,
                row_count=len(rows),
                truncated=len(rows) > max_rows,
                execution_message="",
            )
        except Exception as exc:
            return SQLExecutionResult(status="FAILED", execution_message=str(exc))

