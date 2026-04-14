from __future__ import annotations

from ..agents.agent_runner import AgentRunner
from ..agents.schema_sufficiency_validator_agent import (
    SchemaSufficiencyResult,
    SchemaSufficiencyValidatorAgent,
)
from ..contracts import AgentStep, Schema


class SchemaSufficiencyValidator:
    def __init__(self, model_name: str) -> None:
        self.runner = AgentRunner()
        self.agent = SchemaSufficiencyValidatorAgent()
        self.agent.model_name = model_name

    def validate(
        self,
        intent: str,
        schema: Schema,
        known_information_text: str = "",
        last_tool_output: dict | None = None,
        last_write_result: dict | None = None,
        steps: list[AgentStep] | None = None,
    ) -> SchemaSufficiencyResult:
        payload = {
            "intent": intent,
            "known_information_text": known_information_text,
            "current_schema": schema.model_dump(mode="json"),
            "last_tool_output": last_tool_output or {},
            "last_write_result": last_write_result or {},
        }
        run = self.runner.run(self.agent, payload, steps=steps)
        if not run.ok or run.output is None:
            return SchemaSufficiencyResult(
                sufficient=False,
                gap_category="unknown",
                reason=run.error or "sufficiency validator failed",
            )
        return run.output
