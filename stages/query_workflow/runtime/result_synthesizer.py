from __future__ import annotations

from ..agents.agent_runner import AgentRunner
from ..agents.final_synthesizer_agent import FinalSynthesizerAgent
import time

from ..contracts import AgentStep, IntentResultSummary, WorkflowResult
from ..enums import IntentStatus, WorkflowStatus
from .step_limiter import WorkflowStepLimiter
from ..state import WorkflowState


class ResultSynthesizer:
    def __init__(self, model_name: str = "") -> None:
        self.runner = AgentRunner()
        self.agent = FinalSynthesizerAgent()
        self.agent.model_name = model_name

    def synthesize(self, workflow_state: WorkflowState, view: dict) -> WorkflowResult:
        intent_results: list[IntentResultSummary] = []
        raw_items = []
        for intent_state in workflow_state.intents.values():
            status = intent_state.status
            item = IntentResultSummary(
                intent_id=intent_state.intent_id,
                intent=intent_state.intent_text,
                status=status,
                answer=intent_state.interpretation_result.answer if intent_state.interpretation_result else "",
                sql=intent_state.selected_sql or "",
                error=intent_state.error_state,
            )
            intent_results.append(item)
            raw_items.append(item.model_dump(mode="json"))
        run = self.runner.run(
            self.agent,
            {"original_query": workflow_state.original_query, "intent_results": raw_items},
            steps=workflow_state.steps,
        )
        if run.ok and run.output is not None:
            final_answer = run.output.final_answer
            limiter = WorkflowStepLimiter.from_settings()
            if limiter.can_append(len(workflow_state.steps)):
                workflow_state.steps.append(
                    AgentStep(
                        step_id=f"workflow_{workflow_state.workflow_id}_{len(workflow_state.steps)+1}",
                        scope="workflow",
                        owner_id=workflow_state.workflow_id,
                        agent=self.agent.name,
                        phase="FINAL_SYNTHESIS",
                        summary=final_answer[:120] or "最终汇总完成",
                        created_at=time.time(),
                    )
                )
        else:
            completed = [item.answer for item in intent_results if item.status == IntentStatus.COMPLETED and item.answer]
            failed = [item.intent for item in intent_results if item.status == IntentStatus.FAILED]
            final_answer = "\n".join(completed)
            if failed:
                final_answer = (final_answer + "\n\n未完成部分：" + "；".join(failed)).strip()
        return WorkflowResult(
            workflow_id=workflow_state.workflow_id,
            status=workflow_state.status,
            final_answer=final_answer,
            intent_results=intent_results,
            ask_ticket=None,
            error=workflow_state.workflow_error,
            view=view,
        )
