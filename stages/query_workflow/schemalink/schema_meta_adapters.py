from __future__ import annotations

from typing import Any

from ..contracts import ColumnSpec, SchemaWrite
from .schema_init_resolver import SchemaInitResolver


class SchemaMetaAdapters:
    """
    Maps initialize/*.json metadata into contracts (ColumnSpec, SchemaWrite) so planners
    do not duplicate JSON key knowledge.
    """

    def __init__(self, resolver: SchemaInitResolver) -> None:
        self._resolver = resolver

    @staticmethod
    def column_spec_from_meta(column_meta: dict[str, Any]) -> ColumnSpec:
        semantic_summary = str(column_meta.get("semantic_summary") or "").strip()
        additions: list[str] = []
        comment = str(column_meta.get("comment") or "").strip()
        data_type = str(column_meta.get("data_type") or "").strip()
        if comment:
            additions.append(f"comment={comment}")
        if data_type:
            additions.append(f"data_type={data_type}")
        index_names: list[str] = []
        for entry in column_meta.get("indexes") or []:
            name = str((entry or {}).get("index_name") or "").strip()
            if name and name not in index_names:
                index_names.append(name)
        if index_names:
            additions.append(f"indexes={','.join(index_names)}")
        distinct_samples = ((column_meta.get("samples") or {}).get("distinct_samples") or [])[:3]
        sample_values = [
            str((sample or {}).get("sample_value") or "").strip()
            for sample in distinct_samples
            if str((sample or {}).get("sample_value") or "").strip()
        ]
        if sample_values:
            additions.append(f"samples={','.join(sample_values)}")
        description = "；".join([part for part in [semantic_summary, "；".join(additions)] if part])
        indexes: list[str] = []
        for entry in column_meta.get("indexes") or []:
            name = str((entry or {}).get("index_name") or "").strip()
            if name and name not in indexes:
                indexes.append(name)
        return ColumnSpec(
            type=str(column_meta.get("data_type") or "").strip(),
            description=description,
            indexes=indexes,
            sample_values=sample_values,
        )

    def table_bootstrap_writes(
        self,
        database: str,
        table: str,
        *,
        known_databases: set[str],
        known_tables: set[tuple[str, str]],
    ) -> list[SchemaWrite]:
        writes: list[SchemaWrite] = []
        if database not in known_databases and self._resolver.database_exists(database):
            writes.append(SchemaWrite(type="db_create", database=database))
            known_databases.add(database)
        if (database, table) not in known_tables and self._resolver.table_exists(database, table):
            table_meta = self._resolver.load_table_meta(database, table)
            writes.append(
                SchemaWrite(
                    type="table_create",
                    database=database,
                    table=table,
                )
            )
            table_desc = str(table_meta.get("description") or "").strip()
            if table_desc:
                writes.append(
                    SchemaWrite(
                        type="column_description_merge",
                        database=database,
                        table=table,
                        scope="table",
                        text=table_desc,
                    )
                )
            known_tables.add((database, table))
        return writes

    def column_create_writes(
        self,
        database: str,
        table: str,
        column: str,
        *,
        known_columns: set[tuple[str, str, str]],
    ) -> list[SchemaWrite]:
        if (database, table, column) in known_columns or not self._resolver.column_exists(database, table, column):
            return []
        column_meta = self._resolver.load_column_meta(database, table, column)
        known_columns.add((database, table, column))
        return [
            SchemaWrite(
                type="column_create",
                database=database,
                table=table,
                column=column,
                spec=self.column_spec_from_meta(column_meta),
            )
        ]
