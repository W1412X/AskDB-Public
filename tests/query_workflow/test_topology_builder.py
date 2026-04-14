from __future__ import annotations

import unittest

from stages.query_workflow.contracts import IntentDecomposeResult
from stages.query_workflow.runtime.intent_topology_builder import IntentTopologyBuilder


class TopologyBuilderTest(unittest.TestCase):
    def test_build_layers(self) -> None:
        result = IntentDecomposeResult.model_validate(
            {
                "intents": [
                    {"intent_id": "intent_001", "intent": "A", "dependent_intent_ids": []},
                    {"intent_id": "intent_002", "intent": "B", "dependent_intent_ids": ["intent_001"]},
                    {"intent_id": "intent_003", "intent": "C", "dependent_intent_ids": ["intent_001"]},
                ]
            }
        )
        graph = IntentTopologyBuilder().build(result)
        self.assertEqual(graph.topo_layers, [["intent_001"], ["intent_002", "intent_003"]])


if __name__ == "__main__":
    unittest.main()
