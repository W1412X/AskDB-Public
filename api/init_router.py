"""
Initialize stage API: status (with logs) and start.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from api.init_state import get_state
from api.init_runner import start_init_thread
from utils.initialize_helper import is_initialized

router = APIRouter(prefix="/api/init", tags=["init"])


@router.get("/status")
def init_status() -> dict:
    """
    Return current init state: status (idle|running|success|failed), phase, message, logs[], error.
    When status is success, is_initialized is true and query page is allowed.
    """
    state = get_state()
    state["is_initialized"] = is_initialized() if state["status"] != "running" else False
    return state


@router.post("/start")
def init_start() -> dict:
    """Start initialization in background. Returns immediately; poll GET /api/init/status for progress and logs."""
    state = get_state()
    if state["status"] == "running":
        raise HTTPException(status_code=409, detail="Initialization already in progress")
    start_init_thread()
    state = get_state()
    state["is_initialized"] = False
    return state
