from __future__ import annotations

from config import get_settings_manager


class WorkflowStepLimiter:
    def __init__(self, max_steps: int) -> None:
        self.max_steps = max(1, int(max_steps))

    @classmethod
    def from_settings(cls) -> "WorkflowStepLimiter":
        qw = get_settings_manager().config.stages.query_workflow
        return cls(int(qw.max_steps))

    def can_append(self, current_steps: int, upcoming_steps: int = 1) -> bool:
        return int(current_steps) + max(1, int(upcoming_steps)) <= self.max_steps

    def ensure_can_append(self, current_steps: int, upcoming_steps: int = 1) -> None:
        if not self.can_append(current_steps, upcoming_steps):
            raise RuntimeError(f"workflow exceeded max_steps={self.max_steps}")
