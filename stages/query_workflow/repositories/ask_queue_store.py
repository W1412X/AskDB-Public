from __future__ import annotations

from ..state import AskQueueState

_ASK_QUEUES: dict[str, dict] = {}


class AskQueueStore:
    def save(self, workflow_id: str, state: AskQueueState) -> None:
        _ASK_QUEUES[str(workflow_id)] = state.model_dump(mode="json")

    def load(self, workflow_id: str) -> AskQueueState | None:
        payload = _ASK_QUEUES.get(str(workflow_id))
        if payload is None:
            return None
        return AskQueueState.model_validate(payload)
