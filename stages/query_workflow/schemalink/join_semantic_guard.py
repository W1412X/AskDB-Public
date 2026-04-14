from __future__ import annotations

from dataclasses import dataclass

from .ref_parse import parse_column_ref
from .schema_init_resolver import SchemaInitResolver


@dataclass
class JoinSemanticDecision:
    accepted: bool
    reason: str = ""
    score: float = 0.0


class JoinSemanticGuard:
    """
    Generic deterministic semantic guard for join paths.

    Goal:
    - keep clearly valid joins (PK/FK, same-id semantics)
    - reject statistically joinable but semantically suspicious joins
    """

    def __init__(self, init_resolver: SchemaInitResolver) -> None:
        self.init_resolver = init_resolver

    def validate(self, left: str, right: str, relation_result: dict | None = None) -> JoinSemanticDecision:
        left_ref = parse_column_ref(left)
        right_ref = parse_column_ref(right)
        if left_ref is None or right_ref is None:
            return JoinSemanticDecision(False, reason="invalid_ref")
        left_db, left_table, left_column = left_ref
        right_db, right_table, right_column = right_ref

        if left == right:
            return JoinSemanticDecision(False, reason="self_join_same_column")

        result = relation_result or {}
        if not bool(result.get("is_joinable")):
            return JoinSemanticDecision(False, reason="not_joinable")

        left_meta = self._load_column_meta(left_db, left_table, left_column)
        right_meta = self._load_column_meta(right_db, right_table, right_column)

        left_fk_target = self._parse_foreign_key_ref(str(left_meta.get("foreign_key_ref") or ""))
        right_fk_target = self._parse_foreign_key_ref(str(right_meta.get("foreign_key_ref") or ""))

        left_is_pk = bool(left_meta.get("is_primary_key"))
        right_is_pk = bool(right_meta.get("is_primary_key"))
        left_is_fk = bool(left_meta.get("is_foreign_key"))
        right_is_fk = bool(right_meta.get("is_foreign_key"))

        same_column_name = left_column == right_column
        left_base = self._id_base(left_column)
        right_base = self._id_base(right_column)
        same_id_family = bool(left_base and right_base and left_base == right_base)
        both_id_family = bool(left_base and right_base)
        generic_bridge_ok = self._generic_id_bridge_ok(
            left_table=left_table,
            right_table=right_table,
            left_base=left_base,
            right_base=right_base,
        )
        fk_pair_exact = self._fk_points_to(left_fk_target, right_table, right_column) or self._fk_points_to(
            right_fk_target,
            left_table,
            left_column,
        )

        if both_id_family and not same_id_family and not fk_pair_exact and not generic_bridge_ok:
            return JoinSemanticDecision(False, reason="id_family_mismatch")
        if (
            self._is_generic_id(left_column, left_base) != self._is_generic_id(right_column, right_base)
            and not fk_pair_exact
            and not generic_bridge_ok
        ):
            return JoinSemanticDecision(False, reason="generic_id_without_fk")

        score = 0.0
        if same_column_name:
            score += 2.0
        if same_id_family:
            score += 2.0
        if generic_bridge_ok:
            score += 2.0
        if fk_pair_exact:
            score += 4.0
        if left_is_fk and right_is_pk and same_column_name:
            score += 2.0
        if right_is_fk and left_is_pk and same_column_name:
            score += 2.0

        try:
            match_rate = float(result.get("match_rate") or 0.0)
        except Exception:
            match_rate = 0.0
        if match_rate >= 0.8:
            score += 1.0
        elif match_rate < 0.05:
            score -= 1.0

        if score >= 3.0:
            return JoinSemanticDecision(True, reason="accepted", score=score)
        return JoinSemanticDecision(False, reason="low_semantic_score", score=score)

    def _load_column_meta(self, database: str, table: str, column: str) -> dict:
        try:
            return self.init_resolver.load_column_meta(database, table, column)
        except Exception:
            return {}

    def _parse_foreign_key_ref(self, value: str) -> tuple[str, str] | None:
        text = str(value or "").strip()
        if not text:
            return None
        if "(" not in text or ")" not in text:
            return None
        left, _, rest = text.partition("(")
        col, _, _ = rest.partition(")")
        table = str(left or "").strip().split(".")[-1]
        column = str(col or "").strip().split(".")[-1]
        if not table or not column:
            return None
        return table, column

    def _fk_points_to(self, fk_ref: tuple[str, str] | None, table: str, column: str) -> bool:
        if fk_ref is None:
            return False
        return fk_ref[0] == table and fk_ref[1] == column

    def _id_base(self, column: str) -> str:
        col = str(column or "").strip().lower()
        if not col:
            return ""
        if col.endswith("_id"):
            return col[: -len("_id")]
        if col.endswith("_key"):
            return col[: -len("_key")]
        if col == "id":
            return "__generic_id__"
        return ""

    def _is_generic_id(self, column: str, base: str) -> bool:
        col = str(column or "").strip().lower()
        return col == "id" or base == "__generic_id__"

    def _generic_id_bridge_ok(self, *, left_table: str, right_table: str, left_base: str, right_base: str) -> bool:
        left_generic = left_base == "__generic_id__"
        right_generic = right_base == "__generic_id__"
        if left_generic and (not right_generic):
            return self._table_matches_base(right_base, left_table)
        if right_generic and (not left_generic):
            return self._table_matches_base(left_base, right_table)
        return False

    def _table_matches_base(self, base: str, table: str) -> bool:
        b = self._normalize_token(base)
        t = self._normalize_token(table)
        if not b or not t:
            return False
        if b == t:
            return True
        return b in t or t in b

    def _normalize_token(self, value: str) -> str:
        token = str(value or "").strip().lower().replace("_", "")
        if not token:
            return ""
        if token.endswith("ies") and len(token) > 3:
            token = token[:-3] + "y"
        elif token.endswith("s") and len(token) > 1:
            token = token[:-1]
        return token
