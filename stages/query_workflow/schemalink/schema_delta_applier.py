from __future__ import annotations

from ..contracts import ColumnSpec, Schema, SchemaDelta
from ..execution.schema_merge import merge_description


class SchemaDeltaApplier:
    def apply(self, schema: Schema, delta: SchemaDelta) -> Schema:
        updated = schema.model_copy(deep=True)
        from ..contracts import DatabaseSchema, JoinPath, TableSchema

        for write in delta.writes:
            if write.type == "db_create":
                db = updated.databases.setdefault(write.database, DatabaseSchema())
            elif write.type == "table_create":
                db = updated.databases.setdefault(write.database, DatabaseSchema())
                table = db.tables.setdefault(write.table, TableSchema())
            elif write.type == "column_create":
                db = updated.databases.setdefault(write.database, DatabaseSchema())
                table = db.tables.setdefault(write.table, TableSchema())
                table.columns[write.column] = (write.spec or ColumnSpec()).model_copy(deep=True)
            elif write.type == "join_path_create":
                updated.join_paths.append(
                    JoinPath(
                        left=write.left,
                        right=write.right,
                        cardinality=write.cardinality or "",
                        null_rate=write.null_rate,
                    )
                )
        return self._normalize(updated, delta)

    def _normalize(self, updated: Schema, delta: SchemaDelta) -> Schema:
        from ..contracts import DatabaseSchema, TableSchema

        for write in delta.writes:
            if write.type != "column_description_merge":
                continue
            if write.scope == "database":
                db = updated.databases.setdefault(write.database, DatabaseSchema())
                if write.text and write.text not in db.description:
                    db.description = merge_description(db.description, write.text)
            if write.scope == "table":
                db = updated.databases.setdefault(write.database, DatabaseSchema())
                table = db.tables.setdefault(write.table, TableSchema())
                if write.text and write.text not in table.description:
                    table.description = merge_description(table.description, write.text)
            if write.scope == "column":
                db = updated.databases.setdefault(write.database, DatabaseSchema())
                table = db.tables.setdefault(write.table, TableSchema())
                column = table.columns.setdefault(write.column, ColumnSpec())
                if write.text and write.text not in column.description:
                    column.description = merge_description(column.description, write.text)
        unique = {}
        for path in updated.join_paths:
            key = tuple(sorted([path.left, path.right]))
            existing = unique.get(key)
            if existing is None or float(path.null_rate or 1.0) <= float(existing.null_rate or 1.0):
                unique[key] = path
        updated.join_paths = list(unique.values())
        return updated
