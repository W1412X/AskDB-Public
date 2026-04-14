from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from utils.data_paths import DataPaths


class SchemaCatalogTool:
    name = "schema_catalog"

    def invoke(self, payload: dict[str, Any]) -> dict[str, Any]:
        database = str(payload.get("database") or "").strip()
        table = str(payload.get("table") or "").strip()
        column = str(payload.get("column") or "").strip()
        fields = list(payload.get("fields") or [])

        if column:
            data = self._load_json(DataPaths.default().find_column_description_path(database, table, column))
        elif table:
            data = self._load_json(DataPaths.default().table_description_path(database, table) / f"TABLE_{table}.json")
        else:
            data = self._load_json(DataPaths.default().initialize_agent_database_dir(database) / f"DATABASE_{database}.json")

        if not fields:
            return {"database": database, "table": table, "column": column, "data": data}
        if isinstance(data, dict):
            filtered = {key: data.get(key) for key in fields}
        else:
            filtered = data
        return {"database": database, "table": table, "column": column, "data": filtered}

    def _load_json(self, path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
