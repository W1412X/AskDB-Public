from __future__ import annotations

from utils.log_console import LogCategory
from utils.logger import get_logger

from ..agents.agent_runner import AgentRunner
from ..agents.error_attribution_agent import ErrorAttributionAgent
from ..contracts import AgentStep, ErrorAttributionOutput, ModuleError
from ..enums import StageName
from .error_attribution_policy import (
    DefaultRepairPolicy,
    ErrorAttributionValidator,
    ErrorRouterSafetyNet,
)


class ErrorRouter:
    """
    Default path: LLM attribution. Tiny safety net before the call; closed-set validation after.
    If the agent run fails, fall back to DefaultRepairPolicy.
    """

    def __init__(self, model_name: str = "") -> None:
        self.runner = AgentRunner()
        self.agent = ErrorAttributionAgent()
        self.agent.model_name = model_name
        self._safety_net = ErrorRouterSafetyNet()
        self._default_policy = DefaultRepairPolicy()
        self._validator = ErrorAttributionValidator(self._default_policy)
        self._log = get_logger("intent_executor")

    def route(
        self,
        current_stage: StageName,
        current_input: dict,
        error_message: str,
        upstream_artifacts: dict,
        steps: list[AgentStep] | None = None,
    ) -> ModuleError:
        early = self._safety_net.try_route(current_stage=current_stage, error_message=error_message)
        if early is not None:
            self._log.info(
                "error route (safety_net)",
                stage=current_stage.value,
                error_code=early.error_code,
                repair=early.repair_action.value,
                category=LogCategory.ROUTING,
            )
            return early

        payload = {
            "current_stage": current_stage.value,
            "current_input": current_input,
            "error_message": error_message,
            "upstream_artifacts": upstream_artifacts,
        }
        run = self.runner.run(self.agent, payload, steps=steps)
        if run.ok and isinstance(run.output, ErrorAttributionOutput):
            out_err = self._validator.to_module_error(
                run.output,
                current_stage=current_stage,
                error_message=error_message,
            )
            self._log.info(
                "error route (llm)",
                stage=current_stage.value,
                error_code=out_err.error_code,
                repair=out_err.repair_action.value,
                router=out_err.evidence.get("router", ""),
                category=LogCategory.ROUTING,
            )
            return out_err
        fb = self._default_policy.resolve(current_stage, error_message)
        self._log.warning(
            "error route (default_policy)",
            stage=current_stage.value,
            agent_error=run.error or "invalid output",
            repair=fb.repair_action.value,
            category=LogCategory.ROUTING,
        )
        return fb
