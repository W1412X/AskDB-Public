from __future__ import annotations

import unittest
from unittest.mock import Mock

from stages.query_workflow.contracts import (
    DatabaseSchema,
    IntentGraph,
    IntentNode,
    ModuleError,
    ModuleResult,
    ModuleStatus,
    Schema,
    StageName,
    TableSchema,
)
from stages.query_workflow.enums import IntentPhase, RepairAction
from stages.query_workflow.execution.intent_executor import IntentExecutor
from stages.query_workflow.state import IntentState, SchemaLinkState, WorkflowState


def _schema_with_db(name: str) -> Schema:
    return Schema(databases={name: DatabaseSchema(tables={"t": TableSchema()})})


class SchemaCheckpointContinuityTest(unittest.TestCase):
    def test_retry_current_continues_from_checkpoint_across_dag_nodes(self) -> None:
        executor = IntentExecutor(model_name="missing-model")
        intent_state = IntentState(
            intent_id="intent_001",
            intent_text="build schema",
            phase=IntentPhase.SCHEMALINK,
            initial_schema=Schema(),
            schemalink_state=SchemaLinkState(
                intent_id="intent_001",
                intent_text="build schema",
                current_schema=Schema(),
            ),
            schema_sub_intent_graph_cache=IntentGraph(
                nodes={
                    "intent_001": IntentNode(intent_id="intent_001", intent="node 1"),
                    "intent_002": IntentNode(intent_id="intent_002", intent="node 2"),
                },
                topo_layers=[["intent_001"], ["intent_002"]],
            ),
            skip_schema_intent_decompose_once=True,
        )
        workflow_state = WorkflowState(
            workflow_id="wf_001",
            original_query="build schema",
            normalized_query="build schema",
            database_scope=[],
            model_name="missing-model",
        )

        calls: list[Schema] = []

        def first_run(sub_state: SchemaLinkState, database_scope: list[str], steps=None) -> ModuleResult[Schema]:
            calls.append(sub_state.current_schema.model_copy(deep=True))
            if len(calls) == 1:
                self.assertEqual(sub_state.current_schema.databases, {})
                return ModuleResult(status=ModuleStatus.SUCCESS, payload=_schema_with_db("db_one"))
            self.assertIn("db_one", sub_state.current_schema.databases)
            return ModuleResult(
                status=ModuleStatus.FATAL_ERROR,
                error=ModuleError(
                    status="FATAL_ERROR",
                    owner_stage=StageName.SCHEMALINK,
                    current_stage=StageName.SCHEMALINK,
                    error_code="SCHEMALINK_MAX_ROUNDS",
                    message="boom",
                    hint="boom",
                    repair_action=RepairAction.STOP,
                ),
            )

        executor.schemalink.run = Mock(side_effect=first_run)
        out = executor._run_schema_build_dag(intent_state, workflow_state)
        self.assertEqual(out.status, ModuleStatus.FATAL_ERROR)
        self.assertIsNotNone(intent_state.schemalink_accumulated_schema_checkpoint)
        self.assertIn("db_one", intent_state.schemalink_accumulated_schema_checkpoint.databases)

        second_calls: list[Schema] = []
        intent_state.skip_schema_intent_decompose_once = True

        def second_run(sub_state: SchemaLinkState, database_scope: list[str], steps=None) -> ModuleResult[Schema]:
            second_calls.append(sub_state.current_schema.model_copy(deep=True))
            if len(second_calls) == 1:
                self.assertIn("db_one", sub_state.current_schema.databases)
                self.assertNotIn("db_two", sub_state.current_schema.databases)
                return ModuleResult(status=ModuleStatus.SUCCESS, payload=_schema_with_db("db_one"))
            self.assertIn("db_one", sub_state.current_schema.databases)
            return ModuleResult(status=ModuleStatus.SUCCESS, payload=_schema_with_db("db_two"))

        executor.schemalink.run = Mock(side_effect=second_run)
        out2 = executor._run_schema_build_dag(intent_state, workflow_state)
        self.assertEqual(out2.status, ModuleStatus.SUCCESS)
        self.assertIn("db_one", out2.payload.databases)
        self.assertIn("db_two", out2.payload.databases)
        self.assertIsNone(intent_state.schemalink_accumulated_schema_checkpoint)

    def test_rebuild_schema_clears_accumulated_checkpoint(self) -> None:
        executor = IntentExecutor(model_name="missing-model")
        intent_state = IntentState(
            intent_id="intent_001",
            intent_text="build schema",
            phase=IntentPhase.SCHEMALINK,
            initial_schema=Schema(),
            schemalink_accumulated_schema_checkpoint=_schema_with_db("db_one"),
            schemalink_state=SchemaLinkState(
                intent_id="intent_001",
                intent_text="build schema",
                current_schema=_schema_with_db("db_one"),
            ),
        )
        error = ModuleError(
            status="RETRYABLE_ERROR",
            owner_stage=StageName.SCHEMALINK,
            current_stage=StageName.SCHEMALINK,
            error_code="SCHEMALINK_REBUILD",
            message="rebuild",
            hint="rebuild",
            repair_action=RepairAction.REBUILD_SCHEMA,
        )

        applied = executor._apply_repair(intent_state, error)
        self.assertTrue(applied)
        self.assertIsNone(intent_state.schemalink_accumulated_schema_checkpoint)
        self.assertEqual(intent_state.schemalink_state.current_schema.databases, {})


if __name__ == "__main__":
    unittest.main()
