from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
import unittest
from unittest.mock import patch

import numpy as np


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

module = importlib.import_module("stages.query_workflow.tools.semantic_join_path_search")
service_module = importlib.import_module("stages.initialize.embedding.search")
ColumnRecord = module.ColumnRecord
SemanticJoinPathSearchTool = module.SemanticJoinPathSearchTool


class SemanticJoinPathSearchToolTest(unittest.TestCase):
    def test_discovers_cross_db_multi_hop_path(self) -> None:
        tool = SemanticJoinPathSearchTool()
        synthetic_records = [
            ColumnRecord(
                database="order_data",
                table="order_line_item",
                column="customer_id",
                data_type="bigint",
                table_description="订单行项目",
                comment="customer id",
                semantic_summary="用于关联客户",
                semantic_keywords=("客户", "关联", "订单"),
                is_foreign_key=True,
                foreign_key_ref="crm_data.customers(id)",
                has_index=True,
                indexes=("idx_customer_id",),
            ),
            ColumnRecord(
                database="order_data",
                table="order_line_item",
                column="id",
                data_type="bigint",
                table_description="订单行项目",
                comment="主键",
                semantic_summary="订单行主键",
                semantic_keywords=("主键", "订单行", "标识"),
                is_primary_key=True,
                has_index=True,
                indexes=("PRIMARY",),
            ),
            ColumnRecord(
                database="crm_data",
                table="customers",
                column="id",
                data_type="bigint",
                table_description="客户主数据",
                comment="主键",
                semantic_summary="客户主键",
                semantic_keywords=("客户", "主键", "标识"),
                is_primary_key=True,
                has_index=True,
                indexes=("PRIMARY",),
            ),
            ColumnRecord(
                database="crm_data",
                table="customers",
                column="region_id",
                data_type="bigint",
                table_description="客户主数据",
                comment="region id",
                semantic_summary="用于关联地区",
                semantic_keywords=("地区", "关联", "客户"),
                is_foreign_key=True,
                foreign_key_ref="geo_data.regions(id)",
                has_index=True,
                indexes=("idx_region_id",),
            ),
            ColumnRecord(
                database="geo_data",
                table="regions",
                column="id",
                data_type="bigint",
                table_description="地区维表",
                comment="主键",
                semantic_summary="地区主键",
                semantic_keywords=("地区", "主键", "标识"),
                is_primary_key=True,
                has_index=True,
                indexes=("PRIMARY",),
            ),
        ]

        tool._load_records = lambda _scope: synthetic_records  # type: ignore[method-assign]

        out = tool.invoke(
            {
                "seed_columns": ["order_data.order_line_item.customer_id"],
                "target_tables": ["geo_data.regions"],
                "database_scope": ["order_data", "crm_data", "geo_data"],
                "top_k": 10,
                "max_columns_per_database": 20,
                "min_score": 0.1,
                "allow_cross_database": True,
            }
        )

        paths = out["paths"]
        self.assertTrue(paths)
        self.assertGreaterEqual(out["depth_used"], 1)
        self.assertIn(out["stop_reason"], {"no_new_paths", "no_improvement", "hard_cap"})
        self.assertTrue(any(path["hops"] == 2 and len(path["tables"]) == 3 for path in paths))
        self.assertTrue(
            any(
                path["hops"] == 2
                and "crm_data.customers" in path["tables"]
                and "geo_data.regions" in path["tables"]
                for path in paths
            )
        )
        self.assertTrue(any(edge["source_table"] != edge["target_table"] for path in paths for edge in path["edges"]))
        self.assertIn("geo_data.regions", {item["table"] for item in out["reachable_tables"]})

    def test_text_similarity_uses_vector_embeddings(self) -> None:
        tool = SemanticJoinPathSearchTool()
        with patch.object(
            service_module.get_semantic_embedding_search_service(),
            "text_similarity_by_texts",
            side_effect=lambda left, right: float(
                np.dot(
                    np.asarray([1.0, 0.0], dtype=np.float32),
                    np.asarray([1.0, 0.0], dtype=np.float32),
                )
            )
            if {str(left), str(right)} == {"alpha entity", "beta record"}
            else 0.0,
        ) as mocked:
            score = tool._text_similarity("alpha entity", "beta record")

        mocked.assert_called_once_with("alpha entity", "beta record")
        self.assertAlmostEqual(score, 1.0, places=6)


if __name__ == "__main__":
    unittest.main()
