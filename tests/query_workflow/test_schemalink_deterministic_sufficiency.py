from __future__ import annotations

import unittest

from stages.query_workflow.contracts import ColumnSpec, DatabaseSchema, Schema, TableSchema
from stages.query_workflow.schemalink.schema_deterministic_sufficiency import deterministic_sufficiency


class DeterministicSufficiencyTest(unittest.TestCase):
    def test_empty_schema_is_insufficient(self) -> None:
        result = deterministic_sufficiency("任意问题", Schema())
        self.assertIsNotNone(result)
        self.assertFalse(result.sufficient)
        self.assertEqual(result.gap_category, "missing_fields")

    def test_schema_without_columns_is_insufficient(self) -> None:
        schema = Schema(
            databases={
                "test_db_industrial_monitoring": DatabaseSchema(
                    tables={"factories": TableSchema(columns={})}
                )
            }
        )
        result = deterministic_sufficiency("任意问题", schema)
        self.assertIsNotNone(result)
        self.assertFalse(result.sufficient)
        self.assertEqual(result.gap_category, "missing_fields")

    def test_schema_with_table_and_column_passes(self) -> None:
        schema = Schema(
            databases={
                "test_db_industrial_monitoring": DatabaseSchema(
                    tables={
                        "factories": TableSchema(
                            columns={"factory_id": ColumnSpec(type="int", description="工厂主键")}
                        )
                    }
                )
            }
        )
        result = deterministic_sufficiency(
            "过去 30 天里，温度传感器有没有超过 80 度的读数？",
            schema,
        )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
