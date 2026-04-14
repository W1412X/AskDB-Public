from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from config import get_settings_manager
from stages.initialize.embedding.search import get_semantic_embedding_search_service
from utils.data_paths import DataPaths


_NUMERIC_TYPES = {
    "tinyint",
    "smallint",
    "mediumint",
    "int",
    "integer",
    "bigint",
    "decimal",
    "numeric",
    "float",
    "double",
}
_TEXT_TYPES = {
    "char",
    "varchar",
    "text",
    "tinytext",
    "mediumtext",
    "longtext",
}
_TEMPORAL_TYPES = {
    "date",
    "datetime",
    "timestamp",
    "time",
    "year",
}
_TOKEN_RE = re.compile(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+")


@dataclass(frozen=True)
class ColumnRecord:
    database: str
    table: str
    column: str
    data_type: str = ""
    table_description: str = ""
    comment: str = ""
    semantic_summary: str = ""
    semantic_keywords: tuple[str, ...] = ()
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_ref: str = ""
    has_index: bool = False
    indexes: tuple[str, ...] = ()

    @property
    def table_ref(self) -> str:
        return f"{self.database}.{self.table}"

    @property
    def ref(self) -> str:
        return f"{self.database}.{self.table}.{self.column}"

    @property
    def blob(self) -> str:
        return " ".join(
            [
                self.ref,
                self.table_ref,
                self.table_description,
                self.comment,
                self.semantic_summary,
                " ".join(self.semantic_keywords),
                self.data_type,
                " ".join(self.indexes),
            ]
        ).strip()


@dataclass
class SeedItem:
    record: ColumnRecord
    weight: float
    reason: str = ""


@dataclass
class PathEdge:
    source: ColumnRecord
    target: ColumnRecord
    score: float
    join_type_hint: str = ""
    reasons: list[str] = field(default_factory=list)
    features: dict[str, Any] = field(default_factory=dict)

    def model_dump(self) -> dict[str, Any]:
        return {
            "source_column": self.source.ref,
            "target_column": self.target.ref,
            "source_table": self.source.table_ref,
            "target_table": self.target.table_ref,
            "score": round(self.score, 4),
            "join_type_hint": self.join_type_hint,
            "reasons": list(self.reasons),
            "features": dict(self.features),
        }


@dataclass
class PathCandidate:
    score: float
    columns: list[ColumnRecord] = field(default_factory=list)
    edges: list[PathEdge] = field(default_factory=list)
    seed_reason: str = ""

    def signature(self) -> tuple[str, ...]:
        return tuple(item.ref for item in self.columns)

    def endpoint(self) -> ColumnRecord:
        return self.columns[-1]

    def tables(self) -> list[str]:
        ordered: list[str] = []
        for col in self.columns:
            ref = col.table_ref
            if ref not in ordered:
                ordered.append(ref)
        return ordered

    def model_dump(self) -> dict[str, Any]:
        return {
            "score": round(self.score, 4),
            "hops": max(0, len(self.edges)),
            "tables": self.tables(),
            "columns": [col.ref for col in self.columns],
            "edges": [edge.model_dump() for edge in self.edges],
            "seed_reason": self.seed_reason,
        }


class SemanticJoinPathSearchTool:
    name = "semantic_join_path_search"

    def invoke(self, payload: dict[str, Any]) -> dict[str, Any]:
        text = str(payload.get("text") or "").strip()
        database_scope = [str(item).strip() for item in (payload.get("database_scope") or []) if str(item).strip()]
        seed_columns = [str(item).strip() for item in (payload.get("seed_columns") or []) if str(item).strip()]
        seed_tables = [str(item).strip() for item in (payload.get("seed_tables") or []) if str(item).strip()]
        target_tables = [str(item).strip() for item in (payload.get("target_tables") or []) if str(item).strip()]
        join_cfg = get_settings_manager().config.stages.join_path_search

        def _pick_int(raw_value: Any, default_value: int) -> int:
            return max(1, int(default_value if raw_value is None or raw_value == "" else raw_value))

        def _pick_float(raw_value: Any, default_value: float) -> float:
            return float(default_value if raw_value is None or raw_value == "" else raw_value)

        min_score = _pick_float(payload.get("min_score"), join_cfg.min_path_score)
        top_k = max(1, int(payload.get("top_k") or 20))
        max_columns_per_database = _pick_int(payload.get("max_columns_per_database"), join_cfg.max_columns_per_database)
        min_edge_score = float(join_cfg.min_edge_score)
        max_frontier_size = _pick_int(None, join_cfg.max_frontier_size)
        max_anchors_per_table = _pick_int(None, join_cfg.max_anchors_per_table)
        max_depth_hard_cap = _pick_int(None, join_cfg.max_depth_hard_cap)
        min_improvement = max(0.0, float(join_cfg.min_improvement))
        no_improve_layers = _pick_int(None, join_cfg.no_improve_layers)
        allow_cross_database = bool(payload.get("allow_cross_database", True))

        records = self._load_records(database_scope)
        if not records:
            return {"paths": [], "reachable_tables": [], "candidate_edges": [], "summary": "no columns found"}

        target_table_set = set(target_tables)
        seed_items = self._build_seed_items(
            records=records,
            text=text,
            seed_columns=seed_columns,
            seed_tables=seed_tables,
            target_table_set=target_table_set,
        )
        if not seed_items and text:
            seed_items = self._build_seed_items(
                records=records,
                text=text,
                seed_columns=[],
                seed_tables=[],
                target_table_set=target_table_set,
                force_from_text=True,
            )
        if not seed_items:
            return {
                "paths": [],
                "reachable_tables": [],
                "candidate_edges": [],
                "summary": "no usable seed columns found",
            }

        candidate_pool = self._select_candidate_pool(
            records=records,
            text=text,
            target_table_set=target_table_set,
            max_columns_per_database=max_columns_per_database,
            seed_tables=set(seed_tables),
            seed_columns=set(seed_columns),
        )

        candidate_edges: dict[tuple[str, str], dict[str, Any]] = {}
        frontier = [
            PathCandidate(score=item.weight, columns=[item.record], seed_reason=item.reason or "seed")
            for item in seed_items
        ]
        all_paths: list[PathCandidate] = []
        depth_used = 0
        stop_reason = "no_seed"
        stagnant_layers = 0
        last_layer_best = 0.0

        for depth in range(1, max_depth_hard_cap + 1):
            next_frontier_map: dict[tuple[str, ...], PathCandidate] = {}
            layer_best = 0.0
            for path in frontier:
                source_candidates = self._table_anchor_sources(
                    path.endpoint().table_ref,
                    records=records,
                    text=text,
                    target_table_set=target_table_set,
                    max_anchors=max_anchors_per_table,
                )
                for source in source_candidates:
                    for target in candidate_pool:
                        if target.ref == source.ref:
                            continue
                        if not allow_cross_database and target.database != source.database:
                            continue
                        if target.table_ref == source.table_ref:
                            continue
                        if target.table_ref in path.tables():
                            continue
                        edge_score, edge_features, reasons, join_type_hint = self._score_edge(
                            text=text,
                            source=source,
                            target=target,
                            target_table_set=target_table_set,
                            allow_cross_database=allow_cross_database,
                        )
                        if edge_score < min_edge_score:
                            continue
                        path_score = self._combine_path_score(path.score, edge_score, depth, target.table_ref in target_table_set)
                        if path_score < min_score:
                            continue
                        layer_best = max(layer_best, path_score)
                        edge = PathEdge(
                            source=source,
                            target=target,
                            score=edge_score,
                            join_type_hint=join_type_hint,
                            reasons=reasons,
                            features=edge_features,
                        )
                        new_path = PathCandidate(
                            score=path_score,
                            columns=path.columns + [target],
                            edges=path.edges + [edge],
                            seed_reason=path.seed_reason,
                        )
                        sig = new_path.signature()
                        existing = next_frontier_map.get(sig)
                        if existing is None or new_path.score > existing.score:
                            next_frontier_map[sig] = new_path
                        edge_key = tuple(sorted([source.ref, target.ref]))
                        edge_item = {
                            "source_column": source.ref,
                            "target_column": target.ref,
                            "source_table": source.table_ref,
                            "target_table": target.table_ref,
                            "score": round(edge_score, 4),
                            "join_type_hint": join_type_hint,
                            "reasons": reasons,
                            "features": edge_features,
                            "path_score": round(path_score, 4),
                            "hop": depth,
                        }
                        existing_edge = candidate_edges.get(edge_key)
                        if existing_edge is None or float(edge_item["score"]) > float(existing_edge.get("score") or 0.0):
                            candidate_edges[edge_key] = edge_item
            if not next_frontier_map:
                stop_reason = "no_new_paths"
                break
            frontier = sorted(next_frontier_map.values(), key=lambda item: item.score, reverse=True)[:max_frontier_size]
            all_paths.extend(frontier)
            depth_used = depth
            if depth >= max_depth_hard_cap:
                stop_reason = "hard_cap"
                break
            if layer_best - last_layer_best < min_improvement:
                stagnant_layers += 1
            else:
                stagnant_layers = 0
            last_layer_best = max(last_layer_best, layer_best)
            if stagnant_layers >= no_improve_layers:
                stop_reason = "no_improvement"
                break

        all_paths = self._dedupe_paths(all_paths, top_k=top_k)
        reachable_tables = self._reachable_tables(all_paths)
        candidate_edge_list = sorted(candidate_edges.values(), key=lambda item: float(item.get("path_score") or 0.0), reverse=True)
        summary = self._build_summary(text, seed_items, all_paths, reachable_tables, depth_used, stop_reason)
        return {
            "seed_columns": seed_columns,
            "seed_tables": seed_tables,
            "target_tables": target_tables,
            "paths": [item.model_dump() for item in all_paths[:top_k]],
            "reachable_tables": reachable_tables[:top_k],
            "candidate_edges": candidate_edge_list[:top_k],
            "depth_used": depth_used,
            "stop_reason": stop_reason,
            "summary": summary,
        }

    def _build_seed_items(
        self,
        *,
        records: list[ColumnRecord],
        text: str,
        seed_columns: list[str],
        seed_tables: list[str],
        target_table_set: set[str],
        force_from_text: bool = False,
    ) -> list[SeedItem]:
        by_ref = {record.ref: record for record in records}
        items: list[SeedItem] = []
        seen: set[str] = set()

        for ref in seed_columns:
            record = by_ref.get(ref)
            if record is None or record.ref in seen:
                continue
            items.append(SeedItem(record=record, weight=1.0, reason="explicit seed column"))
            seen.add(record.ref)

        if seed_tables:
            by_table: dict[str, list[ColumnRecord]] = defaultdict(list)
            for record in records:
                by_table[record.table_ref].append(record)
            for table_ref in seed_tables:
                candidates = by_table.get(table_ref, [])
                if not candidates:
                    continue
                ranked = sorted(
                    candidates,
                    key=lambda item: (
                        self._column_relevance(text, item, target_table_set),
                        self._structural_score(item),
                    ),
                    reverse=True,
                )
                for record in ranked[:6]:
                    if record.ref in seen:
                        continue
                    weight = 0.82 + 0.12 * self._structural_score(record)
                    if self._column_relevance(text, record, target_table_set) > 0:
                        weight += 0.05
                    items.append(SeedItem(record=record, weight=min(1.0, weight), reason=f"seed table {table_ref}"))
                    seen.add(record.ref)

        if force_from_text and not items:
            ranked = sorted(
                records,
                key=lambda item: (
                    self._column_relevance(text, item, target_table_set),
                    self._structural_score(item),
                ),
                reverse=True,
            )
            for record in ranked[:8]:
                if record.ref in seen:
                    continue
                weight = 0.55 + 0.3 * self._column_relevance(text, record, target_table_set)
                weight += 0.1 * self._structural_score(record)
                items.append(SeedItem(record=record, weight=min(1.0, weight), reason="text-derived seed"))
                seen.add(record.ref)
        return items

    def _select_candidate_pool(
        self,
        *,
        records: list[ColumnRecord],
        text: str,
        target_table_set: set[str],
        max_columns_per_database: int,
        seed_tables: set[str],
        seed_columns: set[str],
    ) -> list[ColumnRecord]:
        by_db: dict[str, list[ColumnRecord]] = defaultdict(list)
        for record in records:
            by_db[record.database].append(record)

        pool: list[ColumnRecord] = []
        seen: set[str] = set()
        for db_name, db_records in by_db.items():
            ranked = sorted(
                db_records,
                key=lambda item: (
                    self._column_relevance(text, item, target_table_set),
                    self._structural_score(item),
                    1.0 if item.table_ref in seed_tables else 0.0,
                    1.0 if item.ref in seed_columns else 0.0,
                ),
                reverse=True,
            )
            for record in ranked[:max_columns_per_database]:
                if record.ref in seen:
                    continue
                pool.append(record)
                seen.add(record.ref)

        for record in records:
            if record.table_ref in target_table_set or record.ref in seed_columns or record.table_ref in seed_tables:
                if record.ref not in seen:
                    pool.append(record)
                    seen.add(record.ref)

        return pool

    def _table_anchor_sources(
        self,
        table_ref: str,
        *,
        records: list[ColumnRecord],
        text: str,
        target_table_set: set[str],
        max_anchors: int,
    ) -> list[ColumnRecord]:
        table_records = [record for record in records if record.table_ref == table_ref]
        if not table_records:
            return []
        ranked = sorted(
            table_records,
            key=lambda item: (
                self._column_relevance(text, item, target_table_set),
                self._structural_score(item),
                1.0 if self._is_id_like(item.column) else 0.0,
            ),
            reverse=True,
        )
        anchors: list[ColumnRecord] = []
        seen: set[str] = set()
        for record in ranked:
            if record.ref in seen:
                continue
            anchors.append(record)
            seen.add(record.ref)
            if len(anchors) >= max_anchors:
                break
        return anchors

    def _score_edge(
        self,
        *,
        text: str,
        source: ColumnRecord,
        target: ColumnRecord,
        target_table_set: set[str],
        allow_cross_database: bool,
    ) -> tuple[float, dict[str, Any], list[str], str]:
        source_score = self._column_relevance(text, source, target_table_set)
        target_score = self._column_relevance(text, target, target_table_set)
        name_score, name_reason = self._name_score(source, target)
        semantic_score = self._text_similarity(source.blob, target.blob)
        structural_score, structural_reasons, join_type_hint = self._structural_pair_score(source, target)
        type_score, type_label = self._type_score(source.data_type, target.data_type)

        cross_db_bonus = 0.08 if source.database != target.database and allow_cross_database else 0.0
        target_bonus = 0.12 if target.table_ref in target_table_set else 0.0

        score = (
            0.25 * max(source_score, target_score)
            + 0.25 * semantic_score
            + 0.20 * name_score
            + 0.18 * structural_score
            + 0.12 * type_score
            + cross_db_bonus
            + target_bonus
        )
        if source.table_ref == target.table_ref:
            score *= 0.35

        reasons = []
        if text:
            reasons.append(f"seed_query={max(source_score, target_score):.2f}")
        if name_reason:
            reasons.append(name_reason)
        if semantic_score > 0:
            reasons.append(f"semantic={semantic_score:.2f}")
        if type_label:
            reasons.append(type_label)
        reasons.extend(structural_reasons)
        if cross_db_bonus:
            reasons.append("cross_database")
        if target_bonus:
            reasons.append("target_table")

        features = {
            "source_query_score": round(source_score, 4),
            "target_query_score": round(target_score, 4),
            "name_score": round(name_score, 4),
            "semantic_score": round(semantic_score, 4),
            "structural_score": round(structural_score, 4),
            "type_score": round(type_score, 4),
            "cross_db_bonus": round(cross_db_bonus, 4),
            "target_bonus": round(target_bonus, 4),
        }
        return min(score, 1.0), features, reasons[:7], join_type_hint

    def _combine_path_score(self, path_score: float, edge_score: float, hop: int, target_hit: bool) -> float:
        base = path_score * 0.58 + edge_score * 0.42
        if target_hit:
            base += 0.05
        base -= 0.02 * max(0, hop - 1)
        return max(0.0, min(1.0, base))

    def _dedupe_paths(self, paths: list[PathCandidate], *, top_k: int) -> list[PathCandidate]:
        best: dict[tuple[str, ...], PathCandidate] = {}
        for path in paths:
            sig = path.signature()
            current = best.get(sig)
            if current is None or path.score > current.score:
                best[sig] = path
        return sorted(best.values(), key=lambda item: item.score, reverse=True)[:top_k]

    def _reachable_tables(self, paths: list[PathCandidate]) -> list[dict[str, Any]]:
        best: dict[str, dict[str, Any]] = {}
        for idx, path in enumerate(paths):
            endpoint = path.endpoint().table_ref
            entry = best.get(endpoint)
            hops = len(path.edges)
            candidate = {
                "table": endpoint,
                "best_score": round(path.score, 4),
                "best_hops": hops,
                "best_path_index": idx,
            }
            if entry is None or candidate["best_score"] > float(entry.get("best_score") or 0.0):
                best[endpoint] = candidate
        return sorted(best.values(), key=lambda item: float(item.get("best_score") or 0.0), reverse=True)

    def _build_summary(
        self,
        text: str,
        seeds: list[SeedItem],
        paths: list[PathCandidate],
        reachable_tables: list[dict[str, Any]],
        depth_used: int,
        stop_reason: str,
    ) -> str:
        if not seeds:
            return "no seed columns found"
        if not paths:
            seed_refs = ", ".join(item.record.ref for item in seeds[:4])
            return f"no join paths found from seeds: {seed_refs}"
        best = paths[0]
        seed_refs = ", ".join(item.record.ref for item in seeds[:3])
        target = reachable_tables[0]["table"] if reachable_tables else ""
        query_part = f" for {text!r}" if text else ""
        return (
            f"found {len(paths)} paths from {seed_refs}{query_part}; "
            f"best_path_hops={len(best.edges)} endpoint={target or best.endpoint().table_ref} "
            f"depth_used={depth_used} stop_reason={stop_reason}"
        )

    def _load_records(self, database_scope: list[str]) -> list[ColumnRecord]:
        records: list[ColumnRecord] = []
        for db_name in database_scope:
            db_dir = DataPaths.default().initialize_agent_database_dir(db_name)
            if not db_dir.exists():
                continue
            for table_dir in db_dir.iterdir():
                if not table_dir.is_dir():
                    continue
                table_name = table_dir.name
                table_desc = self._load_table_description(table_dir / f"TABLE_{table_name}.json")
                for path in table_dir.glob("*.json"):
                    if path.name.startswith("TABLE_") or path.name.startswith("DATABASE_") or ".bak" in path.name:
                        continue
                    record = self._load_column_record(db_name, table_name, table_desc, path)
                    if record is not None:
                        records.append(record)
        return records

    def _load_table_description(self, path: Path) -> str:
        try:
            with path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            return str(data.get("description") or "").strip()
        except Exception:
            return ""

    def _load_column_record(self, db_name: str, table_name: str, table_desc: str, path: Path) -> ColumnRecord | None:
        try:
            with path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            return None
        column_name = str(data.get("column_name") or path.stem).strip()
        if not column_name:
            return None
        semantic_keywords = tuple(
            str(item).strip()
            for item in (data.get("semantic_keywords") or [])
            if str(item).strip()
        )
        indexes = tuple(
            str((item or {}).get("index_name") or "").strip()
            for item in (data.get("indexes") or [])
            if str((item or {}).get("index_name") or "").strip()
        )
        return ColumnRecord(
            database=db_name,
            table=table_name,
            column=column_name,
            data_type=str(data.get("data_type") or "").strip(),
            table_description=table_desc,
            comment=str(data.get("comment") or "").strip(),
            semantic_summary=str(data.get("semantic_summary") or "").strip(),
            semantic_keywords=semantic_keywords,
            is_primary_key=bool(data.get("is_primary_key")),
            is_foreign_key=bool(data.get("is_foreign_key")),
            foreign_key_ref=str(data.get("foreign_key_ref") or "").strip(),
            has_index=bool(data.get("has_index")),
            indexes=indexes,
        )

    def _column_relevance(self, text: str, record: ColumnRecord, target_table_set: set[str]) -> float:
        query_score = self._text_similarity(text, record.blob) if text else 0.0
        target_boost = 0.18 if record.table_ref in target_table_set else 0.0
        structural = self._structural_score(record)
        return min(1.0, 0.55 * query_score + 0.25 * structural + target_boost)

    def _structural_score(self, record: ColumnRecord) -> float:
        score = 0.0
        if record.is_primary_key:
            score += 0.45
        if record.is_foreign_key:
            score += 0.35
        if record.has_index:
            score += 0.15
        if self._column_base(record.column):
            score += 0.12
        return min(score, 1.0)

    def _name_score(self, left: ColumnRecord, right: ColumnRecord) -> tuple[float, str]:
        if left.column == right.column:
            return 1.0, "same_column_name"
        left_base = self._column_base(left.column)
        right_base = self._column_base(right.column)
        if left_base and right_base and left_base == right_base:
            return 0.95, f"same_base={left_base}"
        if self._is_id_like(left.column) and self._is_id_like(right.column):
            overlap = self._token_overlap(self._tokenize(left.column), self._tokenize(right.column))
            if overlap > 0:
                return 0.78 + 0.12 * overlap, "id_family_overlap"
        overlap = self._token_overlap(self._tokenize(left.table + " " + left.column), self._tokenize(right.table + " " + right.column))
        if overlap > 0:
            return min(0.85, 0.35 + 0.5 * overlap), "name_overlap"
        return 0.0, ""

    def _structural_pair_score(self, left: ColumnRecord, right: ColumnRecord) -> tuple[float, list[str], str]:
        score = 0.0
        reasons: list[str] = []
        join_type_hint = ""

        left_pk = left.is_primary_key
        right_pk = right.is_primary_key
        left_fk = left.is_foreign_key
        right_fk = right.is_foreign_key
        left_base = self._column_base(left.column)
        right_base = self._column_base(right.column)

        if left_fk and right_pk:
            score += 0.6
            reasons.append("left_fk_right_pk")
            join_type_hint = "many_to_one"
        if right_fk and left_pk:
            score += 0.6
            reasons.append("right_fk_left_pk")
            join_type_hint = "one_to_many"
        if left_pk and right_pk:
            score += 0.25
            reasons.append("both_pk")
        if left.has_index and right.has_index:
            score += 0.15
            reasons.append("both_indexed")
        if left_base and right_base and left_base == right_base:
            score += 0.35
            reasons.append(f"same_base={left_base}")
            join_type_hint = join_type_hint or "many_to_one"
        elif self._generic_id_bridge_ok(left, right):
            score += 0.45
            reasons.append("generic_id_bridge")
            join_type_hint = join_type_hint or "many_to_one"
        if self._exact_fk_target(left, right) or self._exact_fk_target(right, left):
            score += 0.75
            reasons.append("exact_fk_ref")
            join_type_hint = join_type_hint or "many_to_one"
        return min(score, 1.0), reasons, join_type_hint

    def _exact_fk_target(self, left: ColumnRecord, right: ColumnRecord) -> bool:
        target = self._foreign_key_target(left.foreign_key_ref)
        return bool(target and target == (right.table, right.column))

    def _type_score(self, left_type: str, right_type: str) -> tuple[float, str]:
        left_family = self._type_family(left_type)
        right_family = self._type_family(right_type)
        if not left_family or not right_family:
            return 0.0, ""
        if left_family == right_family:
            return 1.0, f"type_family={left_family}"
        if {left_family, right_family} <= {"numeric", "decimal"}:
            return 0.9, "type_family=numeric"
        if {left_family, right_family} <= {"text"}:
            return 0.85, "type_family=text"
        if {left_family, right_family} <= {"temporal"}:
            return 0.8, "type_family=temporal"
        if {left_family, right_family} == {"numeric", "text"}:
            return 0.45, "type_family=numeric_text"
        return 0.1, f"type_family={left_family}_{right_family}"

    def _type_family(self, data_type: str) -> str:
        token = str(data_type or "").strip().lower().split("(")[0].strip()
        if token in _NUMERIC_TYPES:
            return "numeric"
        if token in _TEXT_TYPES:
            return "text"
        if token in _TEMPORAL_TYPES:
            return "temporal"
        if token:
            return "other"
        return ""

    def _generic_id_bridge_ok(self, left: ColumnRecord, right: ColumnRecord) -> bool:
        left_base = self._column_base(left.column)
        right_base = self._column_base(right.column)
        if left_base == "__generic_id__":
            return self._table_matches_base(right_base, left.table)
        if right_base == "__generic_id__":
            return self._table_matches_base(left_base, right.table)
        return False

    def _table_matches_base(self, base: str, table: str) -> bool:
        b = self._normalize_token(base)
        t = self._normalize_token(table)
        if not b or not t:
            return False
        return b == t or b in t or t in b

    def _foreign_key_target(self, value: str) -> tuple[str, str] | None:
        text = str(value or "").strip()
        if not text or "(" not in text or ")" not in text:
            return None
        left, _, rest = text.partition("(")
        column_text, _, _ = rest.partition(")")
        table = str(left or "").strip().split(".")[-1]
        column = str(column_text or "").strip().split(".")[-1]
        if not table or not column:
            return None
        return table, column

    def _column_base(self, column: str) -> str:
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

    def _is_id_like(self, column: str) -> bool:
        col = str(column or "").strip().lower()
        return col == "id" or col.endswith("_id") or col.endswith("_key")

    def _normalize_token(self, value: str) -> str:
        token = str(value or "").strip().lower().replace("_", "")
        if not token:
            return ""
        if token.endswith("ies") and len(token) > 3:
            token = token[:-3] + "y"
        elif token.endswith("s") and len(token) > 1:
            token = token[:-1]
        return token

    def _tokenize(self, text: str) -> list[str]:
        tokens: list[str] = []
        for raw in _TOKEN_RE.findall(str(text or "").lower()):
            token = str(raw or "").strip().replace("_", "")
            if not token:
                continue
            if token.isascii():
                tokens.append(token)
                continue
            if len(token) <= 2:
                tokens.append(token)
                continue
            tokens.extend(token[i : i + 2] for i in range(len(token) - 1))
        return tokens

    def _token_overlap(self, left: list[str], right: list[str]) -> float:
        if not left or not right:
            return 0.0
        l = set(left)
        r = set(right)
        union = l | r
        if not union:
            return 0.0
        return len(l & r) / len(union)

    def _text_similarity(self, left: str, right: str) -> float:
        service = get_semantic_embedding_search_service()
        return service.text_similarity_by_texts(left, right)


class SemanticJoinSearchTool(SemanticJoinPathSearchTool):
    name = "semantic_join_search"


__all__ = ["SemanticJoinPathSearchTool", "SemanticJoinSearchTool", "ColumnRecord"]
