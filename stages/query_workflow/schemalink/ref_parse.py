from __future__ import annotations


def parse_column_ref(text: str) -> tuple[str, str, str] | None:
    """Parse ``db.table.column``; requires exactly three dot-separated segments."""
    parts = [part.strip() for part in str(text or "").split(".") if part.strip()]
    if len(parts) != 3:
        return None
    return parts[0], parts[1], parts[2]


def parse_table_ref(text: str) -> tuple[str, str] | None:
    """Parse ``db.table``; requires exactly two dot-separated segments."""
    parts = [part.strip() for part in str(text or "").split(".") if part.strip()]
    if len(parts) != 2:
        return None
    return parts[0], parts[1]
