from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, List

from config import get_settings_manager
from stages.query_workflow.facade import resume_query_workflow, run_query_workflow
from utils.data_paths import DataPaths
from utils.initialize_helper import _needs_initialize_agent, _needs_initialize_embedding

EXIT_WORDS = {"exit", "quit"}


def _ensure_initialize_artifacts() -> None:
    from stages.initialize.agent.run import initialize_databases
    from stages.initialize.embedding.build_embedding import build_embeddings

    cfg = get_settings_manager().config
    target_databases = sorted(set(cfg.get_initialize_databases()) | set(cfg.get_default_database_scope()))
    if not target_databases:
        raise RuntimeError("No databases configured for initialize/query flow.")

    missing_agent = [db for db in target_databases if _needs_initialize_agent(db)]
    if missing_agent:
        print(f"[bootstrap] initialize agent outputs missing for: {', '.join(missing_agent)}")
        initialize_databases(
            database_names=missing_agent,
            model_name=cfg.stages.initialize.agent.model_name,
        )

    missing_embedding = [db for db in target_databases if _needs_initialize_embedding(db)]
    if missing_embedding:
        emb_cfg = cfg.stages.initialize.embedding
        model_path = None
        local_only = False
        if getattr(emb_cfg, "model_path_name", None):
            # Always provide a local target dir so EmbeddingTool can snapshot_download via hf_endpoint mirror.
            candidate = DataPaths.model_embedding_path(str(emb_cfg.model_path_name))
            model_path = str(candidate)
            local_only = candidate.exists()
        hf_endpoint = str(getattr(emb_cfg, "hf_endpoint", "") or "").strip() or None
        print(f"[bootstrap] initialize embeddings missing for: {', '.join(missing_embedding)}")
        build_embeddings(
            database_names=missing_embedding,
            model_name=emb_cfg.model_name,
            model_path=model_path,
            hf_endpoint=hf_endpoint,
            normalize_embeddings=emb_cfg.normalize_embeddings,
            batch_size=emb_cfg.batch_size,
            device=emb_cfg.device or None,
            local_files_only=local_only,
            overwrite=emb_cfg.overwrite,
        )


def _print_result(result: Any, *, interactive: bool) -> None:
    if result.status.value == "WAIT_USER" and result.ask_ticket:
        ticket = result.ask_ticket
        print("[需要补充信息]")
        print(f"ticket_id: {ticket.ticket_id}")
        print(ticket.question)
        if ticket.why_needed:
            print(ticket.why_needed)
        if ticket.acceptance_criteria:
            print("请至少补充：")
            for item in ticket.acceptance_criteria:
                print(f"- {item}")
        if not interactive:
            print("[等待恢复] 请使用 ticket_id 调用 resume 接口，或进入交互模式继续。")
            return
        reply = input("补充> ").strip()
        if not reply:
            print("[中断] 空回复，未继续执行。")
            return
        resumed = resume_query_workflow(result.workflow_id, ticket.ticket_id, reply)
        _print_result(resumed, interactive=interactive)
        return
    if result.status.value in {"COMPLETED", "PARTIAL_SUCCESS"}:
        print("[回答]")
        print(result.final_answer or "(空)")
        return
    print("[失败]")
    if result.error:
        print(result.error.message)
    else:
        print("workflow failed")


def _run_query(query: str, *, database_scope: List[str], model_name: str, interactive: bool) -> None:
    result = run_query_workflow(query, database_scope=database_scope, model_name=model_name)
    _print_result(result, interactive=interactive)


def main(argv: List[str] | None = None) -> int:
    cfg = get_settings_manager().config
    # Ensure fresh checkout has required base dirs (data/, log/).
    DataPaths.default().ensure_base_dirs()
    parser = argparse.ArgumentParser(description="Interactive AskDB query workflow terminal.")
    parser.add_argument("--skip-init", action="store_true", help="Skip initialize/embedding bootstrap check.")
    parser.add_argument("--query", default="", help="Run one query once, then exit.")
    args = parser.parse_args(argv)

    if not args.skip_init:
        try:
            _ensure_initialize_artifacts()
        except Exception as exc:
            print(f"[bootstrap-warning] initialize precheck skipped: {type(exc).__name__}: {exc}", file=sys.stderr)

    database_scope = cfg.get_default_database_scope()
    model_name = cfg.stages.query_workflow.model_name

    if args.query.strip():
        _run_query(
            args.query.strip(),
            database_scope=database_scope,
            model_name=model_name,
            interactive=False,
        )
        return 0

    print("SQL generation interactive loop started. 输入 exit/quit 结束。")
    while True:
        try:
            query = input("用户> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not query:
            continue
        if query.lower() in EXIT_WORDS:
            return 0
        try:
            _run_query(
                query,
                database_scope=database_scope,
                model_name=model_name,
                interactive=True,
            )
        except KeyboardInterrupt:
            print()
            return 0
        except Exception as exc:
            print(f"[异常] {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    raise SystemExit(main())
