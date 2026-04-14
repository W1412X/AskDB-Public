from __future__ import annotations

from config import get_settings_manager
from utils.logger import attach_request_log_file, detach_request_log_file

from .contracts import UserReply, WorkflowRequest
from .repositories.workflow_store import WorkflowStore
from .runtime.query_workflow_pipeline import QueryWorkflowPipeline
from .state import state_from_dict, state_to_dict

_STORE = WorkflowStore()


def get_workflow_store() -> WorkflowStore:
    return _STORE


def run_query_workflow(
    query: str,
    *,
    database_scope: list[str] | None = None,
    model_name: str | None = None,
    workflow_id: str | None = None,
) -> object:
    cfg = get_settings_manager().config
    stage_cfg = cfg.stages.query_workflow
    request = WorkflowRequest(
        query=query,
        database_scope=list(database_scope or cfg.get_default_database_scope()),
        model_name=model_name or stage_cfg.model_name,
        workflow_id=workflow_id,
    )
    attach_request_log_file(request.workflow_id or "query_workflow")
    try:
        return QueryWorkflowPipeline(store=_STORE).run(request)
    finally:
        detach_request_log_file()


def resume_query_workflow(workflow_id: str, ticket_id: str, reply: str) -> object:
    attach_request_log_file(workflow_id or "query_workflow_resume")
    try:
        return QueryWorkflowPipeline(store=_STORE).resume(workflow_id, UserReply(ticket_id=ticket_id, reply=reply))
    finally:
        detach_request_log_file()


def build_query_snapshot(workflow_id: str) -> dict | None:
    """API-oriented snapshot: view + optional final_result fields + updated_at."""
    store = _STORE
    state, updated_at = store.load_with_timestamp(workflow_id)
    if state is None:
        return None
    from .runtime.query_workflow_pipeline import build_workflow_view

    view = build_workflow_view(state)
    view["updated_at"] = updated_at
    fr = state.final_result
    terminal_statuses = frozenset({"COMPLETED", "FAILED", "PARTIAL_SUCCESS", "WAIT_USER"})
    snap: dict = {
        "workflow_id": state.workflow_id,
        "updated_at": updated_at,
        "status": state.status.value,
        "terminal": state.status.value in terminal_statuses,
        "view": view,
        "final_answer": fr.final_answer if fr else "",
        "ask_ticket": fr.ask_ticket.model_dump(mode="json") if fr and fr.ask_ticket else None,
        "error": fr.error.model_dump(mode="json") if fr and fr.error else None,
        "intent_results": [item.model_dump(mode="json") for item in fr.intent_results] if fr else [],
    }
    if state.workflow_error and snap["error"] is None:
        snap["error"] = state.workflow_error.model_dump(mode="json")
    return snap


__all__ = [
    "build_query_snapshot",
    "get_workflow_store",
    "run_query_workflow",
    "resume_query_workflow",
    "state_to_dict",
    "state_from_dict",
]
