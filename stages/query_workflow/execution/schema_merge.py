from __future__ import annotations

from collections.abc import Callable

from ..contracts import Schema


def merge_description(existing: str, incoming: str) -> str:
    """Merge two description strings: dedupe substring containment, join with full-width semicolon."""
    existing_text = str(existing or "").strip()
    incoming_text = str(incoming or "").strip()
    if not existing_text:
        return incoming_text
    if not incoming_text:
        return existing_text
    if incoming_text in existing_text:
        return existing_text
    return "；".join([existing_text, incoming_text])


def merge_schema(
    base: Schema,
    incoming: Schema,
    *,
    description_merge: Callable[[str, str], str] | None = None,
) -> Schema:
    merged = base.model_copy(deep=True)
    merge_desc = description_merge or merge_description
    for db_name, db_schema in incoming.databases.items():
        target_db = merged.databases.setdefault(db_name, db_schema.model_copy(deep=True))
        if target_db is db_schema:
            continue
        if db_schema.description and db_schema.description not in target_db.description:
            target_db.description = merge_desc(target_db.description, db_schema.description)
        for table_name, table_schema in db_schema.tables.items():
            target_table = target_db.tables.setdefault(table_name, table_schema.model_copy(deep=True))
            if target_table is table_schema:
                continue
            if table_schema.description and table_schema.description not in target_table.description:
                target_table.description = merge_desc(target_table.description, table_schema.description)
            for column_name, column_spec in table_schema.columns.items():
                existing = target_table.columns.setdefault(column_name, column_spec.model_copy(deep=True))
                if existing is column_spec:
                    continue
                if not existing.type and column_spec.type:
                    existing.type = column_spec.type
                if column_spec.description and column_spec.description not in existing.description:
                    existing.description = merge_desc(existing.description, column_spec.description)
                seen_indexes = set(existing.indexes)
                for item in column_spec.indexes:
                    if item not in seen_indexes:
                        existing.indexes.append(item)
                        seen_indexes.add(item)
                seen = set(existing.sample_values)
                for value in column_spec.sample_values:
                    if value not in seen:
                        existing.sample_values.append(value)
                        seen.add(value)
    keys = set()
    merged_paths = []
    for item in list(merged.join_paths) + list(incoming.join_paths):
        key = tuple(sorted([item.left, item.right]))
        if key in keys:
            continue
        keys.add(key)
        merged_paths.append(item)
    merged.join_paths = merged_paths
    return merged
