from __future__ import annotations

import sqlparse
from sqlparse import tokens as T
from sqlparse.sql import Function, Identifier, IdentifierList, Parenthesis, Statement, TokenList
from utils.logger import get_logger
from utils.database_tool import _db_tool

from ..contracts import SQLRenderResult, SQLValidationResult, ValidationErrorReport


class SQLValidator:
    def __init__(self) -> None:
        self.logger = get_logger("intent_executor")

    def validate(
        self,
        render_result: SQLRenderResult,
        resolved_schema: dict,
        database_scope: list[str],
        timeout_ms: int | None = None,
    ) -> SQLValidationResult:
        if timeout_ms is None:
            from config import get_settings_manager

            timeout_ms = int(get_settings_manager().config.stages.query_workflow.sql_timeout_ms)
        reports: list[ValidationErrorReport] = []
        best_index = -1
        explain_database = database_scope[0] if len(database_scope) == 1 else None

        for idx, candidate in enumerate(render_result.candidates):
            errors = []
            sql = str(candidate.sql or "").strip()
            parsed_stmt: Statement | None = None
            try:
                parsed = sqlparse.parse(sql)
                if not parsed:
                    errors.append("empty sql")
                elif len(parsed) != 1:
                    errors.append("multi statement sql is not allowed")
                else:
                    parsed_stmt = parsed[0]
                    first = parsed_stmt.token_first(skip_cm=True, skip_ws=True)
                    if first is None or str(first).upper() not in {"SELECT", "WITH"}:
                        errors.append("only SELECT/WITH is allowed")
                if parsed_stmt is not None and _contains_forbidden_dml_ddl(parsed_stmt):
                    errors.append("ddl/dml is not allowed")
            except Exception as exc:
                errors.append(str(exc))
            passed = not errors
            if passed:
                if _db_tool is None:
                    errors.append("global database tool is not initialized")
                    passed = False
                else:
                    try:
                        _db_tool.execute_query(
                            sql=f"EXPLAIN {sql}",
                            database=explain_database,
                            readonly=True,
                            timeout_ms=timeout_ms,
                        )
                    except Exception as exc:
                        errors.append(f"explain failed: {exc}")
                        passed = False
            if passed and candidate.expected_columns and parsed_stmt is not None:
                try:
                    alias_map = _extract_table_aliases(parsed_stmt)
                    lineage, has_wildcard = _select_lineage(parsed_stmt, alias_map)
                    for column in candidate.expected_columns:
                        if not _expected_satisfied(str(column or ""), lineage, has_wildcard):
                            errors.append(f"expected output column missing from sql: {column}")
                            passed = False
                            break
                except Exception as exc:
                    # Fallback: if lineage extraction fails, keep behavior permissive by using a coarse check.
                    lowered = sql.lower()
                    for column in candidate.expected_columns:
                        if str(column).lower() not in lowered:
                            errors.append(f"expected output column missing from sql: {column}")
                            passed = False
                            break
            if passed and best_index < 0:
                best_index = idx
            report = ValidationErrorReport(candidate_index=idx, passed=passed, errors=errors)
            reports.append(report)
            self.logger.info(
                "sql validator candidate report",
                candidate_index=idx,
                passed=passed,
                errors=errors,
                sql_preview=sql[:1200],
                expected_columns=list(candidate.expected_columns or []),
            )
        status = "SUCCESS" if best_index >= 0 else "FAILED"
        self.logger.info(
            "sql validator result",
            status=status,
            best_candidate_index=best_index,
            report_count=len(reports),
        )
        return SQLValidationResult(status=status, best_candidate_index=best_index, reports=reports)


_FORBIDDEN_KEYWORDS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "CREATE",
    "REPLACE",
}


def _contains_forbidden_dml_ddl(stmt: Statement) -> bool:
    for tok in stmt.flatten():
        if tok.ttype in (T.Keyword, T.Keyword.DDL, T.Keyword.DML) or tok.is_keyword:
            val = str(tok.value or "").strip().upper()
            if val in _FORBIDDEN_KEYWORDS:
                return True
    return False


def _extract_table_aliases(stmt: Statement) -> dict[str, tuple[str, str]]:
    """
    Return alias -> (db, table). db may be "" when not specified.
    Includes a self-alias for the real table name as well.
    """
    aliases: dict[str, tuple[str, str]] = {}
    tokens = [t for t in stmt.tokens if not t.is_whitespace and t.ttype != T.Comment]
    expect_table = False
    for tok in tokens:
        if tok.ttype in (T.Keyword, T.Keyword.DML) and str(tok.value or "").upper() in {"FROM", "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN"}:
            expect_table = True
            continue
        if not expect_table:
            continue
        ident = _as_identifier(tok)
        if ident is None:
            continue
        db = str(ident.get_parent_name() or "").strip()
        table = str(ident.get_real_name() or "").strip()
        alias = str(ident.get_alias() or ident.get_name() or "").strip()
        if table:
            # Map alias -> real table
            if alias:
                aliases[alias] = (db, table)
            # Also allow using the table name directly as a "pseudo alias"
            aliases[table] = (db, table)
        expect_table = False
    return aliases


def _as_identifier(tok) -> Identifier | None:
    if isinstance(tok, Identifier):
        return tok
    if isinstance(tok, IdentifierList):
        # FROM t1, t2 is possible; use first as a best-effort.
        for item in tok.get_identifiers():
            if isinstance(item, Identifier):
                return item
    return None


def _select_lineage(stmt: Statement, alias_map: dict[str, tuple[str, str]]) -> tuple[set[str], bool]:
    """
    Extract base column lineage from SELECT list:
    - Returns a set of normalized refs: ["table.col", "db.table.col"] when db known.
    - Also returns has_wildcard if SELECT includes * (or table.*).
    """
    lineage: set[str] = set()
    has_wildcard = False

    # The wildcard_flag uses a closure to set has_wildcard without returning it through recursion.
    def _set_true():
        nonlocal has_wildcard
        has_wildcard = True

    # Find SELECT ... FROM boundary at top level.
    select_seen = False
    for tok in stmt.tokens:
        if tok.is_whitespace or tok.ttype == T.Comment:
            continue
        if not select_seen:
            if tok.ttype == T.DML and str(tok.value or "").upper() == "SELECT":
                select_seen = True
            continue
        if tok.ttype == T.Keyword and str(tok.value or "").upper() == "FROM":
            break
        _collect_expr_lineage(tok, alias_map, lineage, wildcard_flag=_set_true)

    return lineage, has_wildcard


def _collect_expr_lineage(tok, alias_map: dict[str, tuple[str, str]], out: set[str], wildcard_flag) -> None:
    if tok is None:
        return
    if getattr(tok, "ttype", None) == T.Wildcard or str(getattr(tok, "value", "")).strip() == "*":
        wildcard_flag()
        return
    if isinstance(tok, IdentifierList):
        for item in tok.get_identifiers():
            _collect_expr_lineage(item, alias_map, out, wildcard_flag)
        return
    if isinstance(tok, Identifier):
        # Identifier may wrap complex expression; try to extract column refs.
        parent = str(tok.get_parent_name() or "").strip()
        real = str(tok.get_real_name() or "").strip()
        alias = str(tok.get_alias() or "").strip()
        if alias:
            # Keep select-item alias to satisfy expected output columns like "line_name".
            out.add(alias.lower())
        # table.* case
        if real == "*" and parent:
            wildcard_flag()
            return
        if parent and real:
            # Keep parent-qualified reference as written in SQL (e.g. alias.col),
            # so expected_columns using aliases can be matched deterministically.
            out.add(f"{parent}.{real}".lower())
            db, table = alias_map.get(parent, ("", parent))
            _add_ref(out, db, table, real)
            return
        if real == "*":
            wildcard_flag()
            return
        if real:
            # Unqualified column; keep as-is (can satisfy expected by suffix match).
            out.add(real.lower())
        # Recurse into children for expressions like "a + b" or "func(x)".
        if isinstance(tok, TokenList):
            for child in tok.tokens:
                _collect_expr_lineage(child, alias_map, out, wildcard_flag)
        return
    if isinstance(tok, Function):
        for child in tok.tokens:
            _collect_expr_lineage(child, alias_map, out, wildcard_flag)
        return
    if isinstance(tok, Parenthesis):
        for child in tok.tokens:
            _collect_expr_lineage(child, alias_map, out, wildcard_flag)
        return
    if isinstance(tok, TokenList):
        for child in tok.tokens:
            _collect_expr_lineage(child, alias_map, out, wildcard_flag)


def _add_ref(out: set[str], db: str, table: str, col: str) -> None:
    t = str(table or "").strip()
    c = str(col or "").strip()
    d = str(db or "").strip()
    if not t or not c:
        return
    out.add(f"{t}.{c}".lower())
    if d:
        out.add(f"{d}.{t}.{c}".lower())


def _expected_satisfied(expected: str, lineage: set[str], has_wildcard: bool) -> bool:
    exp = str(expected or "").strip().lower()
    if not exp:
        return True
    if has_wildcard:
        return True
    if exp in lineage:
        return True
    parts = [p for p in exp.split(".") if p]
    if len(parts) >= 2:
        # Support expected in forms:
        # - table.col
        # - db.table.col
        table = parts[-2]
        col = parts[-1]
        suffix = f"{table}.{col}"
        for item in lineage:
            if item.endswith(suffix):
                return True
        return False
    # expected is unqualified column name
    for item in lineage:
        if item == exp or item.endswith(f".{exp}"):
            return True
    return False
