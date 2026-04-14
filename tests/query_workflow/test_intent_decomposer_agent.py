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
_ensure_package("stages.query_workflow.agents", ROOT / "stages" / "query_workflow" / "agents")

from stages.query_workflow.contracts import IntentPairDecomposeResult

IntentDecomposerAgent = importlib.import_module("stages.query_workflow.agents.intent_decomposer_agent").IntentDecomposerAgent


class IntentDecomposerAgentTest(unittest.TestCase):
    def test_allows_natural_language_with_sqlish_words(self) -> None:
        agent = IntentDecomposerAgent()
        output = IntentPairDecomposeResult.model_validate(
            {
                "intents": [
                    {
                        "query": "请 join 业务上下文并说明 where 条件的业务含义",
                        "schema": "构建可按条件筛选并关联业务上下文的 schema",
                    }
                ]
            }
        )

        agent.post_validate({}, output)

    def test_rejects_physical_refs(self) -> None:
        agent = IntentDecomposerAgent()
        output = IntentPairDecomposeResult.model_validate(
            {
                "intents": [
                    {
                        "query": "查询 order_data.order_line_item 的订单",
                        "schema": "构建可查询 order_data.order_line_item 的 schema",
                    }
                ]
            }
        )

        with self.assertRaises(ValueError):
            agent.post_validate({}, output)


if __name__ == "__main__":
    unittest.main()
