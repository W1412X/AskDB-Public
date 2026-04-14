from __future__ import annotations

import unittest

from stages.query_workflow.contracts import ColumnSpec, DatabaseSchema, JoinPath, Schema, TableSchema
from stages.query_workflow.schemalink.schema_init_resolver import SchemaInitResolver
from stages.query_workflow.schemalink.schema_validator import SchemaValidator


class SchemaValidatorTest(unittest.TestCase):
    def test_rejects_database_out_of_scope_and_default(self) -> None:
        validator = SchemaValidator(SchemaInitResolver())
        schema = Schema(databases={"default": DatabaseSchema()})
        result = validator.validate_schema(schema, ["test_db_industrial_monitoring"])
        self.assertFalse(result.valid)
        self.assertTrue(any("default" in item for item in result.errors))

    def test_rejects_join_path_without_existing_columns(self) -> None:
        validator = SchemaValidator(SchemaInitResolver())
        schema = Schema(
            databases={
                "test_db_industrial_monitoring": DatabaseSchema(
                    tables={
                        "factories": TableSchema(
                            columns={"factory_id": ColumnSpec(type="int", description="", indexes=["PRIMARY"], sample_values=["1"])}
                        )
                    }
                )
            },
            join_paths=[
                JoinPath(
                    left="test_db_industrial_monitoring.factories.factory_id",
                    right="test_db_industrial_monitoring.production_lines.factory_id",
                    cardinality="one_to_many",
                    null_rate=0.0,
                )
            ],
        )
        result = validator.validate_schema(schema, ["test_db_industrial_monitoring"])
        self.assertFalse(result.valid)
        self.assertTrue(any("join path right reference not found" in item for item in result.errors))


if __name__ == "__main__":
    unittest.main()
