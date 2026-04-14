from __future__ import annotations

from typing import Any


class AskUserTool:
    name = "ask_user"

    def invoke(self, payload: dict[str, Any]) -> dict[str, Any]:
        return dict(payload or {})

