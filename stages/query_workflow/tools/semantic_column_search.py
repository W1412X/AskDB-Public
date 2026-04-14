from __future__ import annotations

from typing import Any

from stages.initialize.embedding.search import get_semantic_embedding_search_service


class SemanticColumnSearchTool:
    name = "semantic_column_search"

    def invoke(self, payload: dict[str, Any]) -> dict[str, Any]:
        text = str(payload.get("text") or "").strip()
        database_scope = list(payload.get("database_scope") or [])
        top_k = int(payload.get("top_k") or 10)
        if not text or not database_scope:
            return {"items": []}
        service = get_semantic_embedding_search_service()
        items = []
        for item in service.search_columns_by_text(text, database_scope, top_k=top_k):
            score = float(item.get("similarity") or 0.0)
            if score <= 0:
                continue
            items.append(
                {
                    "database": str(item.get("database_name") or "").strip(),
                    "table": str(item.get("table_name") or "").strip(),
                    "column": str(item.get("column_name") or "").strip(),
                    "score": round(score, 4),
                    "description": str(item.get("description") or "").strip(),
                    "type": str(item.get("type") or "").strip(),
                }
            )
        return {"items": items}
