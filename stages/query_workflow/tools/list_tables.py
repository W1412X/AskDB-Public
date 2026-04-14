from __future__ import annotations

import json
from typing import Any

from utils.data_paths import DataPaths


class ListTablesTool:
    name = "list_tables"

    def invoke(self, payload: dict[str, Any]) -> dict[str, Any]:
        database = str(payload.get("database") or "").strip()
        if not database:
            raise ValueError("database is required")
        db_dir = DataPaths.default().initialize_agent_database_dir(database)
        tables: list[dict[str, Any]] = []
        if db_dir.exists():
            for table_dir in db_dir.iterdir():
                if not table_dir.is_dir():
                    continue
                table_name = table_dir.name
                table_json = table_dir / f"TABLE_{table_name}.json"
                if not table_json.exists():
                    continue
                try:
                    with table_json.open("r", encoding="utf-8") as fh:
                        data = json.load(fh)
                except Exception:
                    continue
                tables.append(
                    {
                        "table": data.get("table_name") or table_name,
                        "description": str(data.get("description") or "").strip(),
                    }
                )
        return {"database": database, "tables": tables}
