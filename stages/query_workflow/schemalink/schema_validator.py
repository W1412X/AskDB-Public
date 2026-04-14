from __future__ import annotations

from pydantic import BaseModel, Field

from ..contracts import Schema
from .schema_init_resolver import SchemaInitResolver


class SchemaValidationResult(BaseModel):
    valid: bool
    errors: list[str] = Field(default_factory=list)


class SchemaValidator:
    def __init__(self, init_resolver: SchemaInitResolver) -> None:
        self.init_resolver = init_resolver

    def validate_schema(self, schema: Schema, database_scope: list[str]) -> SchemaValidationResult:
        errors: list[str] = []
        scope = set(database_scope or [])
        for database, db_schema in (schema.databases or {}).items():
            if not database:
                errors.append("database name must not be empty")
                continue
            if scope and database not in scope:
                errors.append(f"database out of scope: {database}")
            if database == "default":
                errors.append("database 'default' is not allowed")
            if not self.init_resolver.database_exists(database):
                errors.append(f"database initialize directory not found: {database}")
            for table, table_schema in (db_schema.tables or {}).items():
                if not table:
                    errors.append(f"table name must not be empty under database {database}")
                    continue
                if not self.init_resolver.table_exists(database, table):
                    errors.append(f"table metadata not found: {database}.{table}")
                for column in (table_schema.columns or {}).keys():
                    if not column:
                        errors.append(f"column name must not be empty under {database}.{table}")
                        continue
                    if not self.init_resolver.column_exists(database, table, column):
                        errors.append(f"column metadata not found: {database}.{table}.{column}")
        existing = self._existing_columns(schema)
        for path in schema.join_paths or []:
            if path.left not in existing:
                errors.append(f"join path left reference not found: {path.left}")
            if path.right not in existing:
                errors.append(f"join path right reference not found: {path.right}")
        return SchemaValidationResult(valid=not errors, errors=errors)

    def _existing_columns(self, schema: Schema) -> set[str]:
        refs: set[str] = set()
        for database, db_schema in (schema.databases or {}).items():
            for table, table_schema in (db_schema.tables or {}).items():
                for column in (table_schema.columns or {}).keys():
                    refs.add(f"{database}.{table}.{column}")
        return refs
