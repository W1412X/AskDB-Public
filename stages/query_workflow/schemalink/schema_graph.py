from __future__ import annotations

from collections import deque

from ..contracts import Schema
from .ref_parse import parse_column_ref, parse_table_ref


class SchemaGraph:
    """
    Deterministic schema connectivity graph.

    Node: "db.table"
    Edge: derived from schema.join_paths (undirected).
    """

    def __init__(self, schema: Schema) -> None:
        self._adj: dict[str, set[str]] = {}
        for path in schema.join_paths or []:
            left = self._table_ref(path.left)
            right = self._table_ref(path.right)
            if not left or not right:
                continue
            self._adj.setdefault(left, set()).add(right)
            self._adj.setdefault(right, set()).add(left)

    def reachable_any(self, sources: list[str], targets: list[str]) -> bool:
        if not sources or not targets:
            return False
        target_set = set(targets)
        q: deque[str] = deque()
        seen: set[str] = set()
        for s in sources:
            if s in seen:
                continue
            seen.add(s)
            q.append(s)
        while q:
            cur = q.popleft()
            if cur in target_set:
                return True
            for nxt in self._adj.get(cur, set()):
                if nxt in seen:
                    continue
                seen.add(nxt)
                q.append(nxt)
        return False

    def _table_ref(self, column_ref: str) -> str:
        col = parse_column_ref(column_ref)
        if col:
            return f"{col[0]}.{col[1]}"
        table = parse_table_ref(column_ref)
        if table:
            return f"{table[0]}.{table[1]}"
        return ""

