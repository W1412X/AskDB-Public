from __future__ import annotations

import time
import uuid
from threading import Lock

from ..contracts import WorkflowCheckpoint
from ..state import WorkflowState, state_from_dict, state_to_dict

_WORKFLOWS: dict[str, dict] = {}
_UPDATED_AT: dict[str, float] = {}
_CHECKPOINTS: dict[str, list[dict]] = {}
_LOCK = Lock()


def _max_workflow_items() -> int:
    from config import get_settings_manager

    return max(1, int(get_settings_manager().config.stages.workflow_store.max_items))


def _prune_unlocked() -> None:
    cap = _max_workflow_items()
    if len(_WORKFLOWS) <= cap:
        return
    ordered = sorted(_UPDATED_AT.items(), key=lambda item: item[1])
    overflow = max(0, len(ordered) - cap)
    for workflow_id, _ in ordered[:overflow]:
        _WORKFLOWS.pop(workflow_id, None)
        _UPDATED_AT.pop(workflow_id, None)
        _CHECKPOINTS.pop(workflow_id, None)


class WorkflowStore:
    def save(self, state: WorkflowState) -> str:
        with _LOCK:
            workflow_id = str(state.workflow_id or uuid.uuid4())
            state.workflow_id = workflow_id
            _WORKFLOWS[workflow_id] = state_to_dict(state)
            _UPDATED_AT[workflow_id] = time.time()
            _CHECKPOINTS.setdefault(workflow_id, [])
            _prune_unlocked()
            return workflow_id

    def load(self, workflow_id: str) -> WorkflowState | None:
        with _LOCK:
            payload = _WORKFLOWS.get(str(workflow_id))
            if payload is None:
                return None
            state = state_from_dict(payload)
            state.checkpoints = [WorkflowCheckpoint.model_validate(item) for item in _CHECKPOINTS.get(str(workflow_id), [])]
            return state

    def get_updated_at(self, workflow_id: str) -> float | None:
        with _LOCK:
            ts = _UPDATED_AT.get(str(workflow_id))
            return float(ts) if ts is not None else None

    def load_with_timestamp(self, workflow_id: str) -> tuple[WorkflowState | None, float | None]:
        """Consistent read of state + last mutation time (save / checkpoint), without bumping updated_at."""
        with _LOCK:
            wid = str(workflow_id)
            payload = _WORKFLOWS.get(wid)
            ts = _UPDATED_AT.get(wid)
            if payload is None:
                return None, (float(ts) if ts is not None else None)
            state = state_from_dict(payload)
            state.checkpoints = [WorkflowCheckpoint.model_validate(item) for item in _CHECKPOINTS.get(wid, [])]
            return state, (float(ts) if ts is not None else None)

    def append_checkpoint(self, workflow_id: str, checkpoint: WorkflowCheckpoint, state: WorkflowState) -> None:
        with _LOCK:
            _CHECKPOINTS.setdefault(str(workflow_id), []).append(checkpoint.model_dump(mode="json"))
            _WORKFLOWS[str(workflow_id)] = state_to_dict(state)
            _UPDATED_AT[str(workflow_id)] = time.time()
