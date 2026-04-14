from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
import unittest


def _ensure_package(name: str, path: Path) -> None:
    if name in sys.modules:
        return
    module = types.ModuleType(name)
    module.__path__ = [str(path)]  # type: ignore[attr-defined]
    sys.modules[name] = module


if "sentence_transformers" not in sys.modules:
    fake = types.ModuleType("sentence_transformers")

    class _DummySentenceTransformer:  # pragma: no cover - test shim only
        def __init__(self, *args, **kwargs) -> None:
            raise RuntimeError("sentence_transformers shim should not be instantiated in this test")

    fake.SentenceTransformer = _DummySentenceTransformer
    sys.modules["sentence_transformers"] = fake


ROOT = Path(__file__).resolve().parents[2]
_ensure_package("stages", ROOT / "stages")
_ensure_package("stages.query_workflow", ROOT / "stages" / "query_workflow")
_ensure_package("stages.query_workflow.schemalink", ROOT / "stages" / "query_workflow" / "schemalink")
_ensure_package("stages.query_workflow.tools", ROOT / "stages" / "query_workflow" / "tools")

SchemaLinkEngine = importlib.import_module("stages.query_workflow.schemalink.engine").SchemaLinkEngine
JoinSemanticGuard = importlib.import_module("stages.query_workflow.schemalink.join_semantic_guard").JoinSemanticGuard
SchemaInitResolver = importlib.import_module("stages.query_workflow.schemalink.schema_init_resolver").SchemaInitResolver
RelationValidationTool = importlib.import_module("stages.query_workflow.tools.relation_validator").RelationValidationTool


class JoinSemanticGuardTest(unittest.TestCase):
    def setUp(self) -> None:
        self.guard = JoinSemanticGuard(SchemaInitResolver())

    def test_accepts_fk_pk_like_join(self) -> None:
        decision = self.guard.validate(
            "test_db_industrial_monitoring.factories.factory_id",
            "test_db_industrial_monitoring.production_lines.factory_id",
            {"is_joinable": True, "match_rate": 1.0},
        )
        self.assertTrue(decision.accepted)

    def test_rejects_id_family_mismatch_join(self) -> None:
        decision = self.guard.validate(
            "test_db_industrial_monitoring.equipment.type_id",
            "test_db_industrial_monitoring.sensors.sensor_id",
            {"is_joinable": True, "match_rate": 0.6},
        )
        self.assertFalse(decision.accepted)
        self.assertIn(decision.reason, {"id_family_mismatch", "low_semantic_score"})

    def test_accepts_generic_id_bridge_without_fk(self) -> None:
        decision = self.guard.validate(
            "test_db_industrial_monitoring.customers.id",
            "test_db_industrial_monitoring.orders.customer_id",
            {"is_joinable": True, "match_rate": 0.92},
        )
        self.assertTrue(decision.accepted)

    def test_accepts_cross_database_join_when_semantics_match(self) -> None:
        decision = self.guard.validate(
            "order_data.order_line_item.customer_id",
            "crm_data.customers.id",
            {"is_joinable": True, "match_rate": 0.94},
        )
        self.assertTrue(decision.accepted)

    def test_engine_write_plan_rejects_semantic_mismatch(self) -> None:
        engine = SchemaLinkEngine(model_name="missing-model", max_rounds=1)
        tool_output = {
            "confirm": {
                "tables": [],
                "columns": [],
                "more_info": [],
                "join_paths": [
                    {
                        "left": "test_db_industrial_monitoring.equipment.type_id",
                        "right": "test_db_industrial_monitoring.sensors.sensor_id",
                    }
                ],
            }
        }
        tool_trace = [
            {
                "tool": "relation_validator",
                "arguments": {
                    "left_column": "test_db_industrial_monitoring.equipment.type_id",
                    "right_column": "test_db_industrial_monitoring.sensors.sensor_id",
                },
                "result": {"is_joinable": True, "join_type_hint": "many_to_one", "match_rate": 0.7},
            }
        ]
        write_plan, invalid = engine._write_plan_from_confirm(tool_output, tool_trace)
        self.assertEqual(write_plan, [])
        self.assertTrue(any(item.startswith("semantic_rejected:") for item in invalid))

    def test_relation_validator_allows_cross_database_validation(self) -> None:
        tool = RelationValidationTool()
        tool.sql_explorer.invoke = lambda _payload: {  # type: ignore[method-assign]
            "columns": ["total_rows", "left_nulls", "right_nulls", "matched_rows"],
            "rows": [[10, 0, 0, 8]],
            "truncated": False,
        }
        out = tool.invoke(
            {
                "left_column": "order_data.order_line_item.customer_id",
                "right_column": "crm_data.customers.id",
            }
        )
        self.assertTrue(out["is_joinable"])
        self.assertGreater(out["match_rate"], 0.0)


if __name__ == "__main__":
    unittest.main()
