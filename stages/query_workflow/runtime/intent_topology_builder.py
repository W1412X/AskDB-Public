from __future__ import annotations

from ..contracts import IntentDecomposeResult, IntentEdge, IntentGraph, IntentNode


class IntentTopologyBuilder:
    def build(self, result: IntentDecomposeResult) -> IntentGraph:
        nodes = {item.intent_id: IntentNode(intent_id=item.intent_id, intent=item.intent, dependent_intent_ids=item.dependent_intent_ids) for item in result.intents}
        for item in result.intents:
            if item.intent_id in item.dependent_intent_ids:
                raise ValueError("self dependency is not allowed")
            for dep in item.dependent_intent_ids:
                if dep not in nodes:
                    raise ValueError(f"unknown dependency: {dep}")
        indegree = {intent_id: 0 for intent_id in nodes}
        outgoing = {intent_id: [] for intent_id in nodes}
        edges = []
        for item in result.intents:
            for dep in item.dependent_intent_ids:
                indegree[item.intent_id] += 1
                outgoing[dep].append(item.intent_id)
                edges.append(IntentEdge(source=dep, target=item.intent_id))
        layers: list[list[str]] = []
        ready = [intent_id for intent_id, count in indegree.items() if count == 0]
        visited = 0
        while ready:
            current = sorted(ready)
            layers.append(current)
            ready = []
            for node_id in current:
                visited += 1
                for child in outgoing[node_id]:
                    indegree[child] -= 1
                    if indegree[child] == 0:
                        ready.append(child)
        if visited != len(nodes):
            raise ValueError("cycle detected in intent graph")
        return IntentGraph(nodes=nodes, edges=edges, topo_layers=layers)

