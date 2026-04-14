from __future__ import annotations

from abc import ABC, abstractmethod
import json
from typing import TYPE_CHECKING, Generic, Literal, TypeVar

from pydantic import BaseModel

from ..contracts import AgentStep

if TYPE_CHECKING:
    from ..tools.registry import ToolRegistry

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT", bound=BaseModel)


class BaseAgent(Generic[InputT, OutputT], ABC):
    name: str = ""
    description: str = ""
    model_name: str = ""
    output_model: type[BaseModel]
    available_tools: list[str] = []
    tool_choice_mode: Literal["none", "single", "mixed"] = "none"
    max_tool_calls_per_round: int | None = None
    max_tool_rounds: int | None = None
    max_json_retries: int = 2
    max_semantic_retries: int = 1

    @abstractmethod
    def build_system_prompt(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def build_user_prompt(self, payload: InputT, steps: list[AgentStep] | None) -> str:
        raise NotImplementedError

    def format_steps_block(self, steps: list[AgentStep] | None, *, limit: int = 8) -> str:
        if not steps:
            return "【上下文步骤】\n无\n"
        recent = steps[-limit:]
        lines = ["【上下文步骤（最近几步）】"]
        for item in recent:
            label = f"{item.scope}"
            agent = item.agent or "agent"
            summary = item.summary or "无"
            lines.append(f"- [{label}] {agent}: {summary}")
        return "\n".join(lines)

    def post_validate(self, _payload: InputT, _output: OutputT) -> None:
        return None

    def format_output_schema(self) -> str:
        schema = self.output_model.model_json_schema()
        return json.dumps(schema, ensure_ascii=False, indent=2)

    def supports_tool_calling(self) -> bool:
        return bool(self.available_tools) and self.tool_choice_mode != "none"

    def resolve_tool_specs(self, registry: "ToolRegistry") -> list[dict]:
        return registry.tool_specs(self.available_tools)
