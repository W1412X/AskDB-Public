from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
import unittest
from unittest.mock import patch


def _ensure_package(name: str, path: Path) -> None:
    if name in sys.modules:
        return
    module = types.ModuleType(name)
    module.__path__ = [str(path)]  # type: ignore[attr-defined]
    sys.modules[name] = module


ROOT = Path(__file__).resolve().parents[2]
_ensure_package("stages", ROOT / "stages")
_ensure_package("stages.query_workflow", ROOT / "stages" / "query_workflow")
_ensure_package("stages.query_workflow.tools", ROOT / "stages" / "query_workflow" / "tools")

table_module = importlib.import_module("stages.query_workflow.tools.semantic_table_search")
column_module = importlib.import_module("stages.query_workflow.tools.semantic_column_search")
service_module = importlib.import_module("stages.initialize.embedding.search")

SemanticTableSearchTool = table_module.SemanticTableSearchTool
SemanticColumnSearchTool = column_module.SemanticColumnSearchTool


class SemanticVectorSearchToolsTest(unittest.TestCase):
    def test_table_search_delegates_to_shared_vector_helper(self) -> None:
        tool = SemanticTableSearchTool()
        with patch.object(
            service_module.get_semantic_embedding_search_service(),
            "search_tables_by_text",
            return_value=[
                {"database_name": "db", "table_name": "orders", "similarity": 0.91, "description": "orders"},
            ],
        ) as mocked:
            out = tool.invoke({"text": "orders", "database_scope": ["db"], "top_k": 5})

        mocked.assert_called_once_with("orders", ["db"], top_k=5)
        self.assertEqual(out["items"][0]["table"], "orders")
        self.assertEqual(out["items"][0]["score"], 0.91)

    def test_column_search_delegates_to_shared_vector_helper(self) -> None:
        tool = SemanticColumnSearchTool()
        with patch.object(
            service_module.get_semantic_embedding_search_service(),
            "search_columns_by_text",
            return_value=[
                {
                    "database_name": "db",
                    "table_name": "orders",
                    "column_name": "order_id",
                    "similarity": 0.87,
                    "description": "orders id",
                    "type": "bigint",
                }
            ],
        ) as mocked:
            out = tool.invoke({"text": "order id", "database_scope": ["db"], "top_k": 5})

        mocked.assert_called_once_with("order id", ["db"], top_k=5)
        self.assertEqual(out["items"][0]["column"], "order_id")
        self.assertEqual(out["items"][0]["score"], 0.87)


if __name__ == "__main__":
    unittest.main()
