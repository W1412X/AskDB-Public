from __future__ import annotations

import time
import uuid

from ..contracts import WorkflowCheckpoint
from ..repositories.workflow_store import WorkflowStore
from ..state import WorkflowState


class CheckpointRecorder:
    def __init__(self, store: WorkflowStore) -> None:
        self.store = store

    def record(self, state: WorkflowState, *, scope: str, owner_id: str = "", label: str = "") -> WorkflowCheckpoint:
        checkpoint = WorkflowCheckpoint(
            checkpoint_id=f"ckpt_{uuid.uuid4().hex[:12]}",
            scope=scope,
            owner_id=owner_id,
            label=label,
            created_at=time.time(),
        )
        state.checkpoints.append(checkpoint)
        self.store.append_checkpoint(state.workflow_id, checkpoint, state)
        return checkpoint
