"""
Query run and resume API; unified view + optional async + SSE stream.
"""
from __future__ import annotations

import asyncio
import json
import threading
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import StreamingResponse

router = APIRouter(prefix="/api/query", tags=["query"])


def _run_query(
    query: str,
    database_scope: Optional[List[str]] = None,
    model_name: Optional[str] = None,
    workflow_id: Optional[str] = None,
) -> Dict[str, Any]:
    from config import get_settings_manager
    from stages.query_workflow.facade import run_query_workflow

    cfg = get_settings_manager().config
    scope = list(database_scope or cfg.get_default_database_scope() or [])
    if not scope:
        raise ValueError("database_scope is required")
    result = run_query_workflow(
        query=query.strip(),
        database_scope=scope,
        model_name=model_name or cfg.stages.query_workflow.model_name,
        workflow_id=workflow_id,
    )
    return {
        "status": result.status.value,
        "workflow_id": result.workflow_id,
        "final_answer": result.final_answer,
        "ask_ticket": result.ask_ticket.model_dump(mode="json") if result.ask_ticket else None,
        "error": result.error.model_dump(mode="json") if result.error else None,
        "intent_results": [item.model_dump(mode="json") for item in result.intent_results],
        "view": result.view,
    }


def _resume_query(workflow_id: str, ticket_id: str, reply: str) -> Dict[str, Any]:
    from stages.query_workflow.facade import resume_query_workflow

    result = resume_query_workflow(
        workflow_id=workflow_id.strip(),
        ticket_id=ticket_id,
        reply=reply.strip(),
    )
    return {
        "status": result.status.value,
        "workflow_id": result.workflow_id,
        "final_answer": result.final_answer,
        "ask_ticket": result.ask_ticket.model_dump(mode="json") if result.ask_ticket else None,
        "error": result.error.model_dump(mode="json") if result.error else None,
        "intent_results": [item.model_dump(mode="json") for item in result.intent_results],
        "view": result.view,
    }


def _run_query_background(workflow_id: str, query: str, database_scope: Optional[List[str]], model_name: Optional[str]) -> None:
    from utils.logger import attach_request_log_file, detach_request_log_file

    attach_request_log_file(workflow_id)
    try:
        _run_query(query, database_scope=database_scope, model_name=model_name, workflow_id=workflow_id)
    except Exception:
        from utils.logger import get_logger

        get_logger("api.query").exception("async query run failed", workflow_id=workflow_id)
    finally:
        detach_request_log_file()


def _resume_query_background(workflow_id: str, ticket_id: str, reply: str) -> None:
    from utils.logger import attach_request_log_file, detach_request_log_file

    attach_request_log_file(workflow_id or "query_workflow_resume")
    try:
        _resume_query(workflow_id, ticket_id, reply)
    except Exception:
        from utils.logger import get_logger

        get_logger("api.query").exception("async query resume failed", workflow_id=workflow_id)
    finally:
        detach_request_log_file()


@router.post("/run")
def query_run(body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Execute natural language query. Returns unified result view."""
    query = body.get("query") or ""
    if not str(query).strip():
        raise HTTPException(status_code=400, detail="query is required")
    try:
        return _run_query(
            query=str(query).strip(),
            database_scope=body.get("database_scope"),
            model_name=body.get("model_name"),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run/async")
def query_run_async(body: Dict[str, Any] = Body(...)) -> Dict[str, str]:
    """Start query in a background thread; use GET /stream/{workflow_id} or GET /status/{workflow_id} to follow progress."""
    from config import get_settings_manager

    query = body.get("query") or ""
    if not str(query).strip():
        raise HTTPException(status_code=400, detail="query is required")
    cfg = get_settings_manager().config
    scope = list(body.get("database_scope") or cfg.get_default_database_scope() or [])
    if not scope:
        raise HTTPException(status_code=400, detail="database_scope is required")
    workflow_id = f"wf_{uuid.uuid4().hex[:12]}"
    model_name = body.get("model_name") or cfg.stages.query_workflow.model_name
    thread = threading.Thread(
        target=_run_query_background,
        args=(workflow_id, str(query).strip(), scope, model_name),
        name=f"askdb-query-{workflow_id}",
        daemon=True,
    )
    thread.start()
    return {"workflow_id": workflow_id}


@router.get("/status/{workflow_id}")
def query_status(workflow_id: str) -> Dict[str, Any]:
    from stages.query_workflow.facade import build_query_snapshot

    snap = build_query_snapshot(workflow_id.strip())
    if snap is None:
        raise HTTPException(status_code=404, detail="workflow not found")
    return snap


@router.get("/stream/{workflow_id}")
async def query_stream(workflow_id: str) -> StreamingResponse:
    """Server-Sent Events: pushes JSON snapshots when store changes or workflow status changes."""

    async def gen():
        from stages.query_workflow.facade import build_query_snapshot

        wid = workflow_id.strip()
        # Wait until background run persists first state (or timeout).
        snap = None
        for _ in range(300):
            snap = build_query_snapshot(wid)
            if snap is not None:
                break
            await asyncio.sleep(0.2)
        if snap is None:
            yield f"data: {json.dumps({'event': 'not_found', 'workflow_id': wid}, ensure_ascii=False)}\n\n"
            return

        last_ts: float | None = None
        last_status: str | None = None
        while True:
            snap = build_query_snapshot(wid)
            if snap is None:
                yield f"data: {json.dumps({'event': 'not_found', 'workflow_id': wid}, ensure_ascii=False)}\n\n"
                return
            ts = snap.get("updated_at")
            st = snap.get("status")
            if ts != last_ts or st != last_status or snap.get("terminal"):
                last_ts = ts
                last_status = st
                payload = {"event": "snapshot", **snap}
                yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                if snap.get("terminal"):
                    return
            await asyncio.sleep(0.2)

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(gen(), media_type="text/event-stream", headers=headers)


@router.post("/resume")
def query_resume(body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Resume after WAIT_USER: pass workflow_id, ticket_id, reply."""
    workflow_id = body.get("workflow_id") or ""
    ticket_id = body.get("ticket_id") or ""
    reply = body.get("reply") or ""
    if not str(workflow_id).strip():
        raise HTTPException(status_code=400, detail="workflow_id is required")
    if not str(ticket_id).strip():
        raise HTTPException(status_code=400, detail="ticket_id is required")
    try:
        return _resume_query(workflow_id=str(workflow_id).strip(), ticket_id=ticket_id, reply=reply)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/resume/async")
def query_resume_async(body: Dict[str, Any] = Body(...)) -> Dict[str, str]:
    """Resume in background; reconnect SSE to the same workflow_id."""
    workflow_id = str(body.get("workflow_id") or "").strip()
    ticket_id = str(body.get("ticket_id") or "").strip()
    reply = str(body.get("reply") or "")
    if not workflow_id:
        raise HTTPException(status_code=400, detail="workflow_id is required")
    if not ticket_id:
        raise HTTPException(status_code=400, detail="ticket_id is required")
    thread = threading.Thread(
        target=_resume_query_background,
        args=(workflow_id, ticket_id, reply),
        name=f"askdb-resume-{workflow_id}",
        daemon=True,
    )
    thread.start()
    return {"workflow_id": workflow_id}
