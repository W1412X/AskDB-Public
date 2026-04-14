from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class SchemaInitResolver:
    def __init__(self, base_dir: str | None = None) -> None:
        root = Path(base_dir) if base_dir else Path(__file__).resolve().parents[3] / "data" / "initialize" / "agent"
        self.base_dir = root

    def database_exists(self, database: str) -> bool:
        return self._database_dir(database).is_dir()

    def table_exists(self, database: str, table: str) -> bool:
        return self._table_meta_path(database, table).is_file()

    def column_exists(self, database: str, table: str, column: str) -> bool:
        return self._column_meta_path(database, table, column).is_file()

    def load_table_meta(self, database: str, table: str) -> dict[str, Any]:
        return self._load_json(self._table_meta_path(database, table))

    def load_column_meta(self, database: str, table: str, column: str) -> dict[str, Any]:
        return self._load_json(self._column_meta_path(database, table, column))

    def _database_dir(self, database: str) -> Path:
        return self.base_dir / str(database or "").strip()

    def _table_meta_path(self, database: str, table: str) -> Path:
        return self._database_dir(database) / str(table or "").strip() / f"TABLE_{str(table or '').strip()}.json"

    def _column_meta_path(self, database: str, table: str, column: str) -> Path:
        return self._database_dir(database) / str(table or "").strip() / f"{str(column or '').strip()}.json"

    def _load_json(self, path: Path) -> dict[str, Any]:
        if not path.is_file():
            raise FileNotFoundError(str(path))
        return json.loads(path.read_text(encoding="utf-8"))
