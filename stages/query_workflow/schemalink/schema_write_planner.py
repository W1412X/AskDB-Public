from __future__ import annotations

from ..contracts import Schema, SchemaDelta, SchemaWrite
from .ref_parse import parse_column_ref, parse_table_ref
from .schema_init_resolver import SchemaInitResolver
from .schema_meta_adapters import SchemaMetaAdapters


class SchemaWritePlanner:
    def __init__(self, init_resolver: SchemaInitResolver) -> None:
        self.init_resolver = init_resolver
        self._meta = SchemaMetaAdapters(init_resolver)

    def plan(
        self,
        *,
        intent: str,
        write_plan: list[dict],
        current_schema: Schema,
        tool_output: dict,
        database_scope: list[str],
    ) -> SchemaDelta:
        writes: list[SchemaWrite] = []
        invalid_targets: list[str] = []
        known_databases = set((current_schema.databases or {}).keys())
        known_tables = self._table_refs(current_schema)
        known_columns = self._column_refs(current_schema)
        scope_set = set(database_scope or [])

        ordered = self._order_plan(write_plan or [])

        for item in ordered:
            item_type = str(item.get("type") or "").strip()
            if item_type == "db_create":
                target = str(item.get("target") or "").strip()
                if not target or "." in target:
                    invalid_targets.append(f"db_create:{target}")
                    continue
                if scope_set and target not in scope_set:
                    invalid_targets.append(f"db_create:{target}")
                    continue
                if not self.init_resolver.database_exists(target):
                    invalid_targets.append(f"db_create:{target}")
                    continue
                if target not in known_databases:
                    writes.append(SchemaWrite(type="db_create", database=target))
                    known_databases.add(target)
                continue

            if item_type == "table_create":
                target = str(item.get("target") or "").strip()
                parsed_table = parse_table_ref(target)
                if not parsed_table:
                    invalid_targets.append(f"table_create:{target}")
                    continue
                db, table = parsed_table
                if scope_set and db not in scope_set:
                    invalid_targets.append(f"table_create:{target}")
                    continue
                if not self.init_resolver.table_exists(db, table):
                    invalid_targets.append(f"table_create:{target}")
                    continue
                writes.extend(
                    self._meta.table_bootstrap_writes(db, table, known_databases=known_databases, known_tables=known_tables)
                )
                continue

            if item_type == "column_create":
                target = str(item.get("target") or "").strip()
                ref = parse_column_ref(target)
                if not ref:
                    invalid_targets.append(f"column_create:{target}")
                    continue
                db, table, column = ref
                if scope_set and db not in scope_set:
                    invalid_targets.append(f"column_create:{target}")
                    continue
                if not self.init_resolver.column_exists(db, table, column):
                    invalid_targets.append(f"column_create:{target}")
                    continue
                writes.extend(
                    self._meta.table_bootstrap_writes(db, table, known_databases=known_databases, known_tables=known_tables)
                )
                writes.extend(self._meta.column_create_writes(db, table, column, known_columns=known_columns))
                continue

            if item_type == "column_description_merge":
                target = str(item.get("target") or "").strip()
                text = str(item.get("description") or "").strip()
                ref = parse_column_ref(target)
                if not ref or not text:
                    invalid_targets.append(f"column_description_merge:{target}")
                    continue
                db, table, column = ref
                if scope_set and db not in scope_set:
                    invalid_targets.append(f"column_description_merge:{target}")
                    continue
                if not self.init_resolver.column_exists(db, table, column):
                    invalid_targets.append(f"column_description_merge:{target}")
                    continue
                writes.extend(
                    self._meta.table_bootstrap_writes(db, table, known_databases=known_databases, known_tables=known_tables)
                )
                writes.extend(self._meta.column_create_writes(db, table, column, known_columns=known_columns))
                writes.append(
                    SchemaWrite(
                        type="column_description_merge",
                        database=db,
                        table=table,
                        column=column,
                        scope="column",
                        text=text,
                    )
                )
                continue

            if item_type == "join_path_create":
                left = str(item.get("left") or "").strip()
                right = str(item.get("right") or "").strip()
                cardinality = str(item.get("cardinality") or "").strip()
                null_rate = item.get("null_rate")
                left_ref = parse_column_ref(left)
                right_ref = parse_column_ref(right)
                if not left_ref or not right_ref:
                    invalid_targets.append(f"join_path_create:{left}={right}")
                    continue
                left_db, left_table, left_column = left_ref
                right_db, right_table, right_column = right_ref
                if scope_set and (left_db not in scope_set or right_db not in scope_set):
                    invalid_targets.append(f"join_path_create:{left}={right}")
                    continue
                if not self.init_resolver.column_exists(left_db, left_table, left_column):
                    invalid_targets.append(f"join_path_create:{left}")
                    continue
                if not self.init_resolver.column_exists(right_db, right_table, right_column):
                    invalid_targets.append(f"join_path_create:{right}")
                    continue
                writes.extend(
                    self._meta.table_bootstrap_writes(
                        left_db, left_table, known_databases=known_databases, known_tables=known_tables
                    )
                )
                writes.extend(
                    self._meta.table_bootstrap_writes(
                        right_db, right_table, known_databases=known_databases, known_tables=known_tables
                    )
                )
                writes.extend(self._meta.column_create_writes(left_db, left_table, left_column, known_columns=known_columns))
                writes.extend(self._meta.column_create_writes(right_db, right_table, right_column, known_columns=known_columns))
                writes.append(
                    SchemaWrite(
                        type="join_path_create",
                        left=f"{left_db}.{left_table}.{left_column}",
                        right=f"{right_db}.{right_table}.{right_column}",
                        cardinality=cardinality,
                        null_rate=float(null_rate) if null_rate is not None else None,
                    )
                )
                continue

        summary = str((tool_output or {}).get("summary") or "").strip()
        return SchemaDelta(
            writes=writes,
            summary=summary or f"schema write planned for intent: {intent}",
            invalid_targets=invalid_targets,
        )

    def _order_plan(self, items: list[dict]) -> list[dict]:
        order = {
            "db_create": 0,
            "table_create": 1,
            "column_create": 2,
            "column_description_merge": 3,
            "join_path_create": 4,
        }
        return sorted(items, key=lambda item: order.get(str(item.get("type") or ""), 99))

    def _table_refs(self, schema: Schema) -> set[tuple[str, str]]:
        refs: set[tuple[str, str]] = set()
        for database, db_schema in (schema.databases or {}).items():
            for table in (db_schema.tables or {}).keys():
                refs.add((database, table))
        return refs

    def _column_refs(self, schema: Schema) -> set[tuple[str, str, str]]:
        refs: set[tuple[str, str, str]] = set()
        for database, db_schema in (schema.databases or {}).items():
            for table, table_schema in (db_schema.tables or {}).items():
                for column in (table_schema.columns or {}).keys():
                    refs.add((database, table, column))
        return refs
