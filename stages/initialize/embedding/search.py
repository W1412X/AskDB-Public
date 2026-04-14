from __future__ import annotations

from functools import lru_cache
import os
import pickle
from typing import Any

import numpy as np

from config import get_settings_manager
from utils.data_paths import DataPaths
from utils.embedding import EmbeddingTool
from utils.logger import get_logger


class SemanticEmbeddingSearchService:
    def __init__(self, embedding_tool: EmbeddingTool | None = None) -> None:
        self.logger = get_logger("initialize_embedding_query")
        self._embed_cfg = get_settings_manager().config.stages.initialize.embedding
        self._embedding_tool = embedding_tool
        self._warned_missing_db_dirs: set[str] = set()

    def search_columns_by_text(self, text: str, databases: list[str], top_k: int = 10) -> list[dict[str, Any]]:
        columns = self._list_columns(databases)
        valid_columns: list[dict[str, Any]] = []
        skipped_missing_embedding = 0

        for column in columns:
            try:
                embedding = self.get_column_embedding(column["database_name"], column["table_name"], column["column_name"])
                similarity = self.text_similarity(text, embedding)
                column["similarity"] = similarity
                valid_columns.append(column)
            except FileNotFoundError:
                skipped_missing_embedding += 1
                self.logger.debug(
                    "列 embedding 文件不存在，跳过",
                    database_name=column["database_name"],
                    table_name=column["table_name"],
                    column_name=column["column_name"],
                )
            except Exception as exc:
                self.logger.warning(
                    "读取列 embedding 失败，跳过",
                    exception=str(exc),
                    database_name=column["database_name"],
                    table_name=column["table_name"],
                    column_name=column["column_name"],
                )

        if skipped_missing_embedding > 0:
            self.logger.debug(
                "按文本检索列统计",
                total_candidates=len(columns),
                valid_columns=len(valid_columns),
                skipped_missing_embedding=skipped_missing_embedding,
            )

        valid_columns.sort(key=lambda item: float(item.get("similarity") or 0.0), reverse=True)
        return valid_columns[:top_k]

    def search_tables_by_text(self, text: str, databases: list[str], top_k: int = 10) -> list[dict[str, Any]]:
        tables = self._list_tables(databases)
        valid_tables: list[dict[str, Any]] = []
        skipped_missing_embedding = 0

        for table in tables:
            try:
                embedding = self.get_table_embedding(table["database_name"], table["table_name"])
                similarity = self.text_similarity(text, embedding)
                table["similarity"] = similarity
                valid_tables.append(table)
            except FileNotFoundError:
                skipped_missing_embedding += 1
                self.logger.debug(
                    "表 embedding 文件不存在，跳过",
                    database_name=table["database_name"],
                    table_name=table["table_name"],
                )
            except Exception as exc:
                self.logger.warning(
                    "读取表 embedding 失败，跳过",
                    exception=str(exc),
                    database_name=table["database_name"],
                    table_name=table["table_name"],
                )

        if skipped_missing_embedding > 0:
            self.logger.debug(
                "按文本检索表统计",
                total_candidates=len(tables),
                valid_tables=len(valid_tables),
                skipped_missing_embedding=skipped_missing_embedding,
            )

        valid_tables.sort(key=lambda item: float(item.get("similarity") or 0.0), reverse=True)
        return valid_tables[:top_k]

    def text_similarity(self, text: str, embedding: object) -> float:
        if text is None:
            raise ValueError("text must not be None")

        emb_obj = embedding
        if isinstance(emb_obj, dict) and "embedding" in emb_obj:
            emb_obj = emb_obj.get("embedding")

        emb_vec = np.asarray(emb_obj, dtype=np.float32)
        if emb_vec.ndim > 1:
            emb_vec = emb_vec.reshape(-1)

        text_vec = _embed_text_cached(str(text))
        if text_vec.ndim > 1:
            text_vec = text_vec.reshape(-1)

        if self._embed_cfg.normalize_embeddings:
            return float(np.dot(text_vec, emb_vec))

        denom = (np.linalg.norm(text_vec) * np.linalg.norm(emb_vec)) + 1e-12
        return float(np.dot(text_vec, emb_vec) / denom)

    def text_similarity_by_texts(self, left: str, right: str) -> float:
        left_text = str(left or "").strip()
        right_text = str(right or "").strip()
        if not left_text or not right_text:
            return 0.0

        left_vec = _embed_text_cached(left_text)
        right_vec = _embed_text_cached(right_text)
        if left_vec.size == 0 or right_vec.size == 0 or left_vec.shape != right_vec.shape:
            return 0.0

        if self._embed_cfg.normalize_embeddings:
            return float(np.dot(left_vec, right_vec))

        denom = (np.linalg.norm(left_vec) * np.linalg.norm(right_vec)) + 1e-12
        return float(np.dot(left_vec, right_vec) / denom)

    def get_column_embedding(self, db: str, table: str, column: str) -> np.ndarray:
        embedding_path = DataPaths.default().column_embedding_path(db, table, column)
        with embedding_path.open("rb") as f:
            payload = pickle.load(f)
        if isinstance(payload, dict) and "embedding" in payload:
            return np.asarray(payload["embedding"], dtype=np.float32)
        return np.asarray(payload, dtype=np.float32)

    def get_table_embedding(self, db: str, table: str) -> np.ndarray:
        embedding_path = DataPaths.default().table_embedding_path(db, table)
        with embedding_path.open("rb") as f:
            payload = pickle.load(f)
        if isinstance(payload, dict) and "embedding" in payload:
            return np.asarray(payload["embedding"], dtype=np.float32)
        return np.asarray(payload, dtype=np.float32)

    @property
    def embedding_tool(self) -> EmbeddingTool:
        if self._embedding_tool is None:
            model_path = DataPaths.model_embedding_path(self._embed_cfg.model_path_name)
            hf_endpoint = getattr(self._embed_cfg, "hf_endpoint", None) or ""
            dev = str(self._embed_cfg.device or "").strip()
            self._embedding_tool = EmbeddingTool(
                model_name=self._embed_cfg.model_name,
                model_path=str(model_path),
                hf_endpoint=hf_endpoint.strip() or None,
                normalize_embeddings=bool(self._embed_cfg.normalize_embeddings),
                batch_size=int(self._embed_cfg.batch_size),
                device=dev or None,
                max_length=int(self._embed_cfg.max_length),
                trust_remote_code=bool(self._embed_cfg.trust_remote_code),
            )
        return self._embedding_tool

    def _list_columns(self, databases: list[str]) -> list[dict[str, Any]]:
        columns: list[dict[str, Any]] = []
        skipped_metadata = 0
        for db_name in databases:
            db_path = DataPaths.default().initialize_agent_database_dir(db_name)
            if not os.path.exists(db_path):
                key = f"{db_name}:{db_path}"
                if key not in self._warned_missing_db_dirs:
                    self._warned_missing_db_dirs.add(key)
                    self.logger.warning(
                        "数据库目录不存在，跳过（每库仅提示一次）",
                        database_name=db_name,
                        path=str(db_path),
                    )
                continue

            table_paths = os.listdir(db_path)
            for table_path in table_paths:
                table_dir = os.path.join(db_path, table_path)
                if not os.path.isdir(table_dir):
                    continue
                column_paths = [i for i in os.listdir(table_dir) if i.endswith(".json")]
                for column_path in column_paths:
                    if column_path.startswith("TABLE_") or column_path.startswith("DATABASE_"):
                        skipped_metadata += 1
                        continue
                    column_name = column_path.split(".")[0]
                    columns.append(
                        {
                            "database_name": db_name,
                            "table_name": table_path,
                            "column_name": column_name,
                            "similarity": 0,
                        }
                    )

        if skipped_metadata > 0:
            self.logger.debug(
                "列检索跳过元数据文件",
                skipped_metadata=skipped_metadata,
            )
        return columns

    def _list_tables(self, databases: list[str]) -> list[dict[str, Any]]:
        tables: list[dict[str, Any]] = []
        for db_name in databases:
            db_path = DataPaths.default().initialize_agent_database_dir(db_name)
            if not os.path.exists(db_path):
                key = f"{db_name}:{db_path}"
                if key not in self._warned_missing_db_dirs:
                    self._warned_missing_db_dirs.add(key)
                    self.logger.warning(
                        "数据库目录不存在，跳过（每库仅提示一次）",
                        database_name=db_name,
                        path=str(db_path),
                    )
                continue
            table_dirs = [i for i in os.listdir(db_path) if os.path.isdir(os.path.join(db_path, i))]
            for table_name in table_dirs:
                tables.append({"database_name": db_name, "table_name": table_name, "similarity": 0})
        return tables


_SERVICE: SemanticEmbeddingSearchService | None = None


def get_semantic_embedding_search_service() -> SemanticEmbeddingSearchService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = SemanticEmbeddingSearchService()
    return _SERVICE


def _embed_text_cached(text: str) -> np.ndarray:
    return _embed_text_cached_impl(text)


@lru_cache(maxsize=8192)
def _embed_text_cached_impl(text: str) -> np.ndarray:
    service = get_semantic_embedding_search_service()
    vec = service.embedding_tool.embed(text)
    return np.asarray(vec, dtype=np.float32).reshape(-1)
