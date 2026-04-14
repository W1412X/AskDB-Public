from __future__ import annotations

import json
from typing import Any

from utils.data_paths import DataPaths


class ListTableColumnsTool:
    name = "list_table_columns"

    def invoke(self, payload: dict[str, Any]) -> dict[str, Any]:
        database = str(payload.get("database") or "").strip()
        table = str(payload.get("table") or "").strip()
        if not database or not table:
            raise ValueError("database and table are required")
        table_dir = DataPaths.default().table_description_path(database, table)
        columns: list[dict[str, Any]] = []
        if table_dir.exists():
            for path in table_dir.iterdir():
                if not path.is_file():
                    continue
                if path.name.startswith("TABLE_") or ".bak" in path.name:
                    continue
                if path.suffix.lower() != ".json":
                    continue
                try:
                    with path.open("r", encoding="utf-8") as fh:
                        data = json.load(fh)
                except Exception:
                    continue
                columns.append(
                    {
                        "column": data.get("column_name") or path.stem,
                        "description": str(data.get("semantic_summary") or data.get("comment") or "").strip(),
                        "type": str(data.get("data_type") or "").strip(),
                    }
                )
        return {"database": database, "table": table, "columns": columns}
