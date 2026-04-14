from __future__ import annotations

from typing import Any

from .sql_explorer import SqlExploreTool


class RelationValidationTool:
    name = "relation_validator"

    def __init__(self) -> None:
        self.sql_explorer = SqlExploreTool()

    def invoke(self, payload: dict[str, Any]) -> dict[str, Any]:
        left = str(payload.get("left_column") or "").strip()
        right = str(payload.get("right_column") or "").strip()
        left_parts = left.split(".")
        right_parts = right.split(".")
        if len(left_parts) != 3 or len(right_parts) != 3:
            raise ValueError("relation validator expects db.table.column format")
        left_db, left_table, left_column = left_parts
        right_db, right_table, right_column = right_parts
        sql = (
            f"select count(*) as total_rows, "
            f"sum(case when l.`{left_column}` is null then 1 else 0 end) as left_nulls, "
            f"sum(case when r.`{right_column}` is null then 1 else 0 end) as right_nulls, "
            f"sum(case when r.`{right_column}` is not null then 1 else 0 end) as matched_rows "
            f"from `{left_db}`.`{left_table}` l left join `{right_db}`.`{right_table}` r "
            f"on l.`{left_column}` = r.`{right_column}`"
        )
        out = self.sql_explorer.invoke({"sql": sql, "limit": 1})
        row = (out.get("rows") or [[0, 0, 0, 0]])[0]
        total = float(row[0] or 0.0) or 1.0
        left_nulls = float(row[1] or 0.0)
        right_nulls = float(row[2] or 0.0)
        matched = float(row[3] or 0.0)
        return {
            "is_joinable": matched > 0,
            "join_type_hint": "many_to_one",
            "left_null_rate": round(left_nulls / total, 4),
            "right_null_rate": round(right_nulls / total, 4),
            "match_rate": round(matched / total, 4),
            "sample_mismatches": [],
        }
