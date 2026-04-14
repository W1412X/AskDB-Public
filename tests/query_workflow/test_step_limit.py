from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
import unittest


def _ensure_package(name: str, path: Path) -> None:
    if name in sys.modules:
        return
    module = types.ModuleType(name)
    module.__path__ = [str(path)]  # type: ignore[attr-defined]
    sys.modules[name] = module


ROOT = Path(__file__).resolve().parents[2]
_ensure_package("stages", ROOT / "stages")
_ensure_package("stages.query_workflow", ROOT / "stages" / "query_workflow")
_ensure_package("stages.query_workflow.runtime", ROOT / "stages" / "query_workflow" / "runtime")

pipeline_module = importlib.import_module("stages.query_workflow.runtime.query_workflow_pipeline")
step_limiter_module = importlib.import_module("stages.query_workflow.runtime.step_limiter")
state_module = importlib.import_module("stages.query_workflow.state")

QueryWorkflowPipeline = pipeline_module.QueryWorkflowPipeline
WorkflowStepLimiter = step_limiter_module.WorkflowStepLimiter
WorkflowState = state_module.WorkflowState


class WorkflowStepLimitTest(unittest.TestCase):
    def test_default_max_steps_is_50(self) -> None:
        limiter = WorkflowStepLimiter.from_settings()
        self.assertEqual(limiter.max_steps, 50)

    def test_step_append_raises_after_cap(self) -> None:
        pipeline = QueryWorkflowPipeline()
        state = WorkflowState(
            workflow_id="wf_002",
            original_query="query",
            normalized_query="query",
        )
        pipeline._step_limiter = lambda: WorkflowStepLimiter(1)  # type: ignore[method-assign]
        pipeline._append_step(
            state,
            scope="workflow",
            owner_id=state.workflow_id,
            agent="test_agent",
            phase="TEST",
            summary="first",
        )
        with self.assertRaises(RuntimeError):
            pipeline._append_step(
                state,
                scope="workflow",
                owner_id=state.workflow_id,
                agent="test_agent",
                phase="TEST",
                summary="second",
            )


if __name__ == "__main__":
    unittest.main()
