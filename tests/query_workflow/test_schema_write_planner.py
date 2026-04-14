from __future__ import annotations

import unittest

from stages.query_workflow.contracts import Schema
from stages.query_workflow.schemalink.schema_init_resolver import SchemaInitResolver
from stages.query_workflow.schemalink.schema_write_planner import SchemaWritePlanner


class SchemaWritePlannerTest(unittest.TestCase):
    def test_plans_table_column_and_join_from_initialize_metadata(self) -> None:
        planner = SchemaWritePlanner(SchemaInitResolver())
        delta = planner.plan(
            intent="每个工厂下面各有几条生产线？按工厂汇总一下数量。",
            write_goal={"goal": "写入工厂和产线的最小完备 schema", "scope": "join_path"},
            current_schema=Schema(),
            tool_output={
                "summary": "已确认 factories 与 production_lines 通过 factory_id 关联。",
                "structured_findings": {
                    "candidate_tables": [
                        {"database": "test_db_industrial_monitoring", "table": "factories"},
                        {"database": "test_db_industrial_monitoring", "table": "production_lines"},
                    ],
                    "candidate_columns": [
                        {"database": "test_db_industrial_monitoring", "table": "factories", "column": "factory_id"},
                        {"database": "test_db_industrial_monitoring", "table": "production_lines", "column": "factory_id"},
                    ],
                    "candidate_relations": [
                        {
                            "left": "test_db_industrial_monitoring.factories.factory_id",
                            "right": "test_db_industrial_monitoring.production_lines.factory_id",
                            "cardinality": "one_to_many",
                            "left_null_rate": 0.0,
                            "right_null_rate": 0.0,
                            "reason": "工厂与生产线通过 factory_id 关联",
                        }
                    ],
                },
            },
            database_scope=["test_db_industrial_monitoring"],
        )
        ops = [item.op for item in delta.writes]
        self.assertIn("upsert_database", ops)
        self.assertIn("upsert_table", ops)
        self.assertIn("upsert_column", ops)
        self.assertIn("upsert_join_path", ops)
        column_write = next(item for item in delta.writes if item.op == "upsert_column" and item.table == "factories")
        self.assertEqual(column_write.spec.type, "int")
        self.assertEqual(column_write.spec.indexes, ["PRIMARY"])
        self.assertEqual(len(column_write.spec.sample_values), 3)


if __name__ == "__main__":
    unittest.main()
