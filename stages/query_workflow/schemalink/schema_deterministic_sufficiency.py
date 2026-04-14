from __future__ import annotations

from ..agents.schema_sufficiency_validator_agent import SchemaSufficiencyResult
from ..contracts import Schema


def deterministic_sufficiency(intent: str, schema: Schema) -> SchemaSufficiencyResult | None:
    """
    Deterministic generic sufficiency checks (no LLM, no domain special-case):
    schema must contain at least one table and one column.
    Returns a SchemaSufficiencyResult when insufficient, else None.
    """
    _ = intent
    table_count = 0
    column_count = 0
    for _db_name, db_obj in (schema.databases or {}).items():
        for _table_name, table_obj in (db_obj.tables or {}).items():
            table_count += 1
            column_count += len(table_obj.columns or {})

    if table_count == 0:
        return SchemaSufficiencyResult(
            sufficient=False,
            gap_category="missing_fields",
            reason="缺少关键数据源覆盖",
        )
    if column_count == 0:
        return SchemaSufficiencyResult(
            sufficient=False,
            gap_category="missing_fields",
            reason="缺少关键字段覆盖",
        )
    return None
