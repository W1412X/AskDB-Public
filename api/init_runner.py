"""
Run initialize in background thread and capture logs.
"""
from __future__ import annotations

import logging
import threading

from api.init_state import append_log, set_phase, set_status

_handler: logging.Handler | None = None


def _run_init() -> None:
    global _handler
    root = logging.getLogger()
    _handler = None
    try:
        from api.init_state import InitLogHandler
        from config.app_config import reload_app_config
        from utils.database_tool import reload_db_tool

        reload_app_config()
        reload_db_tool()

        _handler = InitLogHandler()
        _handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S"))
        root.addHandler(_handler)

        from utils.initialize_helper import _apply_hf_endpoint_from_env

        _apply_hf_endpoint_from_env()

        from config import get_settings_manager
        from stages.initialize.agent.run import initialize_databases
        from stages.initialize.embedding.build_embedding import build_embeddings
        from utils.data_paths import DataPaths
        from utils.initialize_helper import (
            _needs_initialize_agent,
            _needs_initialize_embedding,
        )

        cfg = get_settings_manager().config
        if getattr(cfg.stages.initialize, "embedding", None):
            emb_cfg = cfg.stages.initialize.embedding
            if getattr(emb_cfg, "hf_endpoint", None) and str(emb_cfg.hf_endpoint).strip():
                from utils.initialize_helper import _set_hf_endpoint_and_reload
                _set_hf_endpoint_and_reload(str(emb_cfg.hf_endpoint).strip())

        target = sorted(set(cfg.get_initialize_databases()) | set(cfg.get_default_database_scope()))
        if not target:
            set_status("failed", "未配置初始化/查询的数据库")
            return

        set_phase("agent", "正在获取模型并初始化数据库列描述（Agent）...")
        missing_agent = [db for db in target if _needs_initialize_agent(db)]
        if missing_agent:
            append_log("info", f"开始 Agent 初始化: {', '.join(missing_agent)}")
            initialize_databases(
                database_names=missing_agent,
                model_name=cfg.stages.initialize.agent.model_name,
            )
            append_log("info", "Agent 初始化完成")

        set_phase("embedding", "正在生成列向量（Embedding）...")
        missing_embedding = [db for db in target if _needs_initialize_embedding(db)]
        if missing_embedding:
            append_log("info", f"开始 Embedding 生成: {', '.join(missing_embedding)}")
            emb_cfg = cfg.stages.initialize.embedding
            model_path = None
            local_only = False
            if getattr(emb_cfg, "model_path_name", None):
                candidate = DataPaths.model_embedding_path(str(emb_cfg.model_path_name))
                model_path = str(candidate)
                local_only = candidate.exists()
            build_embeddings(
                database_names=missing_embedding,
                model_name=emb_cfg.model_name,
                model_path=model_path,
                hf_endpoint=getattr(emb_cfg, "hf_endpoint", None) or "",
                normalize_embeddings=emb_cfg.normalize_embeddings,
                batch_size=emb_cfg.batch_size,
                device=emb_cfg.device or None,
                local_files_only=local_only,
                overwrite=emb_cfg.overwrite,
            )
            append_log("info", "Embedding 生成完成")

        set_phase("done", "初始化完成")
        set_status("success")
    except Exception as e:
        set_status("failed", str(e))
        append_log("error", f"初始化失败: {e}")
    finally:
        if _handler and root:
            try:
                root.removeHandler(_handler)
            except Exception:
                pass


def start_init_thread() -> None:
    from api.init_state import clear_and_start

    clear_and_start()
    t = threading.Thread(target=_run_init, daemon=True)
    t.start()
