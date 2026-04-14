from __future__ import annotations

import time

from pydantic import BaseModel, ConfigDict, Field

from utils.logger import get_logger

from ..agents.agent_runner import AgentRunner
from ..agents.ra_planner_agent import RAPlannerAgent
from ..agents.result_interpreter_agent import ResultInterpreterAgent
from ..agents.schema_description_merge_agent import SchemaDescriptionMergeAgent
from ..agents.schema_intent_decomposer_agent import SchemaIntentDecomposerAgent
from ..agents.sql_renderer_agent import SQLRendererAgent
from ..agents.sql_validation_agent import SQLValidationAgent
from ..contracts import (
    AgentStep,
    InterpretationResult,
    ModuleError,
    ModuleResult,
    RepairRecord,
    RelationalPlan,
    Schema,
    SQLExecutionResult,
)
from ..execution.schema_merge import merge_description, merge_schema
from ..enums import IntentPhase, IntentStatus, ModuleStatus, RepairAction, StageName
from ..execution.sql_executor import SQLExecutor
from ..execution.sql_validator import SQLValidator
from ..runtime.error_router import ErrorRouter
from ..runtime.workflow_logging import IntentExecutionLogger
from ..runtime.intent_context_builder import IntentContextBuilder
from ..runtime.intent_topology_builder import IntentTopologyBuilder
from ..runtime.step_limiter import WorkflowStepLimiter
from ..schemalink.engine import SchemaLinkEngine
from ..state import IntentState, SchemaLinkState, WorkflowState


class IntentExecutionResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    intent_id: str
    intent: str
    status: IntentStatus
    resolved_schema: Schema | None = Field(default=None, alias="schema")
    ra_plan: RelationalPlan | None = None
    selected_sql: str = ""
    execution_result: SQLExecutionResult | None = None
    interpretation_result: InterpretationResult | None = None
    answer: str = ""
    sql: str = ""
    error: ModuleError | None = None
    ask_ticket: object | None = None


class IntentExecutor:
    def __init__(self, model_name: str, max_schemalink_rounds: int = 8, max_repair_attempts: int = 4, max_rows: int = 100, sql_timeout_ms: int = 30000, checkpoint_cb=None) -> None:
        self.runner = AgentRunner()
        self.logger = get_logger("intent_executor")
        self._ilog = IntentExecutionLogger(self.logger)
        self.context_builder = IntentContextBuilder()
        self.schemalink = SchemaLinkEngine(model_name=model_name, max_rounds=max_schemalink_rounds, checkpoint_cb=checkpoint_cb)
        self.schema_intent_decomposer = SchemaIntentDecomposerAgent()
        self.schema_desc_merger = SchemaDescriptionMergeAgent()
        self.ra_agent = RAPlannerAgent()
        self.renderer_agent = SQLRendererAgent()
        self.sql_validation_agent = SQLValidationAgent()
        self.interpreter_agent = ResultInterpreterAgent()
        for agent in [
            self.schema_intent_decomposer,
            self.schema_desc_merger,
            self.ra_agent,
            self.renderer_agent,
            self.sql_validation_agent,
            self.interpreter_agent,
        ]:
            agent.model_name = model_name
        self.sql_validator = SQLValidator()
        self.sql_executor = SQLExecutor()
        self.error_router = ErrorRouter(model_name=model_name)
        self.max_repair_attempts = max_repair_attempts
        self.max_rows = max_rows
        self.sql_timeout_ms = sql_timeout_ms
        self.checkpoint_cb = checkpoint_cb

    def _step_limiter(self) -> WorkflowStepLimiter:
        return WorkflowStepLimiter.from_settings()

    def execute(self, intent_state: IntentState, workflow_state: WorkflowState) -> IntentExecutionResult:
        intent_state.status = IntentStatus.RUNNING
        self._ilog.execute_start(intent_state.intent_id, intent_state.phase.value)
        repairs = 0
        while repairs <= self.max_repair_attempts:
            try:
                self._step_limiter().ensure_can_append(len(workflow_state.steps))
                if intent_state.phase == IntentPhase.CONTEXT_BUILDING:
                    self._ilog.phase(intent_state.intent_id, IntentPhase.CONTEXT_BUILDING.value)
                    context, known_information_text, initial_schema = self.context_builder.build(intent_state, workflow_state)
                    intent_state.dependency_context = context
                    intent_state.known_information_text = known_information_text
                    intent_state.initial_schema = initial_schema
                    intent_state.schemalink_accumulated_schema_checkpoint = None
                    mode = "ENRICH" if initial_schema.databases else "BUILD"
                    schema_intent_text = intent_state.schema_intent_text or intent_state.intent_text
                    intent_state.schemalink_state = SchemaLinkState(
                        mode=mode,
                        intent_id=intent_state.intent_id,
                        intent_text=schema_intent_text,
                        known_information_text=known_information_text,
                        current_schema=initial_schema.model_copy(deep=True),
                    )
                    intent_state.phase = IntentPhase.SCHEMALINK
                    self._checkpoint(intent_state.intent_id, "context_build_completed")

                if intent_state.phase == IntentPhase.SCHEMALINK:
                    self._ilog.phase(intent_state.intent_id, IntentPhase.SCHEMALINK.value)
                    out = self._run_schema_build_dag(intent_state, workflow_state)
                    if out.status.name == "WAIT_USER":
                        intent_state.status = IntentStatus.WAIT_USER
                        intent_state.phase = IntentPhase.WAIT_USER
                        self._checkpoint(intent_state.intent_id, "schemalink_wait_user")
                        return self._result_from_state(intent_state, status=IntentStatus.WAIT_USER, ask_ticket=out.ask_ticket)
                    if out.status != ModuleStatus.SUCCESS or out.payload is None:
                        # Hard gate: schemalink must succeed before RA/SQL.
                        if out.error:
                            raise RuntimeError(out.error.message)
                        raise RuntimeError("schemalink did not complete successfully")
                    if out.error:
                        raise RuntimeError(out.error.message)
                    intent_state.resolved_schema = out.payload
                    intent_state.phase = IntentPhase.PLAN_RA
                    self._checkpoint(intent_state.intent_id, "schemalink_completed")

                if intent_state.phase == IntentPhase.PLAN_RA:
                    self._ilog.phase(intent_state.intent_id, IntentPhase.PLAN_RA.value)
                    query_intent_text = intent_state.query_intent_text or intent_state.intent_text
                    run = self.runner.run(
                        self.ra_agent,
                        {
                            "intent": query_intent_text,
                            "known_information_text": intent_state.known_information_text,
                            "resolved_schema": intent_state.resolved_schema.model_dump(mode="json") if intent_state.resolved_schema else {},
                            "sql_validation_feedback": intent_state.sql_validation_feedback,
                        },
                        steps=workflow_state.steps,
                    )
                    if not run.ok or run.output is None:
                        raise RuntimeError(run.error or "RAPlannerAgent failed")
                    intent_state.ra_plan = run.output.ra
                    self._append_step(
                        workflow_state,
                        scope="intent",
                        owner_id=intent_state.intent_id,
                        agent=self.ra_agent.name,
                        phase=IntentPhase.PLAN_RA.value,
                        summary=intent_state.ra_plan.summary or "关系代数规划完成",
                    )
                    self._ilog.ra_plan_ready(
                        intent_state.intent_id,
                        ra_plan=intent_state.ra_plan.model_dump(mode="json"),
                    )
                    intent_state.phase = IntentPhase.RENDER_SQL
                    self._checkpoint(intent_state.intent_id, "plan_ra_completed")

                if intent_state.phase == IntentPhase.RENDER_SQL:
                    self._ilog.phase(intent_state.intent_id, IntentPhase.RENDER_SQL.value)
                    run = self.runner.run(
                        self.renderer_agent,
                        {
                            "ra": intent_state.ra_plan.model_dump(mode="json") if intent_state.ra_plan else {},
                            "resolved_schema": intent_state.resolved_schema.model_dump(mode="json") if intent_state.resolved_schema else {},
                            "sql_dialect": workflow_state.sql_dialect,
                            "render_feedback": intent_state.sql_render_feedback,
                        },
                        steps=workflow_state.steps,
                    )
                    if not run.ok or run.output is None:
                        raise RuntimeError(run.error or "SQLRendererAgent failed")
                    intent_state.sql_render_result = run.output
                    summary = "SQL 渲染完成"
                    if intent_state.sql_render_result.candidates:
                        summary = intent_state.sql_render_result.candidates[0].rationale or summary
                    self._append_step(
                        workflow_state,
                        scope="intent",
                        owner_id=intent_state.intent_id,
                        agent=self.renderer_agent.name,
                        phase=IntentPhase.RENDER_SQL.value,
                        summary=summary,
                    )
                    self._ilog.sql_render_result(
                        intent_state.intent_id,
                        candidates=[item.model_dump(mode="json") for item in intent_state.sql_render_result.candidates],
                        mark=intent_state.sql_render_result.mark,
                    )
                    intent_state.phase = IntentPhase.VALIDATE_SQL
                    self._checkpoint(intent_state.intent_id, "render_sql_completed")

                if intent_state.phase == IntentPhase.VALIDATE_SQL:
                    self._ilog.phase(intent_state.intent_id, IntentPhase.VALIDATE_SQL.value)
                    if intent_state.sql_render_result is None or not intent_state.sql_render_result.candidates:
                        intent_state.sql_render_feedback = "sql render result missing"
                        intent_state.phase = IntentPhase.RENDER_SQL
                        continue
                    validation = self.sql_validator.validate(
                        intent_state.sql_render_result,
                        intent_state.resolved_schema.model_dump(mode="json") if intent_state.resolved_schema else {},
                        workflow_state.database_scope,
                        timeout_ms=self.sql_timeout_ms,
                    )
                    intent_state.sql_validation_result = validation
                    self._ilog.sql_validation(
                        intent_state.intent_id,
                        validation_result=validation.model_dump(mode="json"),
                    )
                    if validation.status != "SUCCESS" or validation.best_candidate_index < 0:
                        intent_state.sql_render_feedback = self._build_sql_render_feedback(validation)
                        raise RuntimeError(f"sql validation failed: {intent_state.sql_render_feedback}")
                    intent_state.sql_render_feedback = ""
                    intent_state.selected_sql = intent_state.sql_render_result.candidates[validation.best_candidate_index].sql
                    semantic_run = self.runner.run(
                        self.sql_validation_agent,
                        {
                            "intent": query_intent_text,
                            "known_information_text": intent_state.known_information_text,
                            "resolved_schema": intent_state.resolved_schema.model_dump(mode="json") if intent_state.resolved_schema else {},
                            "ra_plan": intent_state.ra_plan.model_dump(mode="json") if intent_state.ra_plan else {},
                            "sql_render_result": intent_state.sql_render_result.model_dump(mode="json") if intent_state.sql_render_result else {},
                            "selected_sql": intent_state.selected_sql or "",
                            "sql_dialect": workflow_state.sql_dialect,
                            "sql_validation_feedback": intent_state.sql_validation_feedback,
                        },
                        steps=workflow_state.steps,
                    )
                    if not semantic_run.ok or semantic_run.output is None:
                        raise RuntimeError(semantic_run.error or "SQLValidationAgent failed")
                    self._ilog.sql_validation(
                        intent_state.intent_id,
                        validation_kind="semantic",
                        validation_result=semantic_run.output.model_dump(mode="json"),
                    )
                    if semantic_run.output.status != "ok":
                        reason = str(semantic_run.output.reason or "").strip() or "sql semantic validation failed"
                        intent_state.sql_validation_feedback = reason
                        self._ilog.phase_error(intent_state.intent_id, IntentPhase.VALIDATE_SQL.value, reason)
                        self._append_step(
                            workflow_state,
                            scope="intent",
                            owner_id=intent_state.intent_id,
                            agent=self.sql_validation_agent.name,
                            phase=IntentPhase.VALIDATE_SQL.value,
                            summary=reason,
                        )
                        intent_state.error_state = ModuleError(
                            status="RETRYABLE_ERROR",
                            owner_stage=StageName.PLAN_RA,
                            current_stage=StageName.VALIDATE_SQL,
                            error_code="SQL_SEMANTIC_VALIDATION_FAILED",
                            message=reason,
                            hint=reason,
                            repair_action=RepairAction.REPLAN_RA,
                            evidence={"router": "sql_validation_agent"},
                        )
                        intent_state.repair_history.append(
                            RepairRecord(
                                owner_stage=StageName.PLAN_RA,
                                current_stage=StageName.VALIDATE_SQL,
                                repair_action=RepairAction.REPLAN_RA,
                                message=reason,
                            )
                        )
                        if not self._apply_repair(intent_state, intent_state.error_state):
                            intent_state.status = IntentStatus.FAILED
                            intent_state.phase = IntentPhase.FAILED
                            self._checkpoint(intent_state.intent_id, "intent_failed")
                            return self._result_from_state(intent_state, status=IntentStatus.FAILED)
                        repairs += 1
                        continue
                    intent_state.sql_validation_feedback = ""
                    intent_state.phase = IntentPhase.EXECUTE_SQL
                    self._checkpoint(intent_state.intent_id, "validate_sql_completed")

                if intent_state.phase == IntentPhase.EXECUTE_SQL:
                    self._ilog.phase(intent_state.intent_id, IntentPhase.EXECUTE_SQL.value)
                    execution = self.sql_executor.execute(
                        intent_state.selected_sql or "",
                        workflow_state.database_scope,
                        timeout_ms=self.sql_timeout_ms,
                        max_rows=self.max_rows,
                    )
                    intent_state.execution_result = execution
                    if execution.status != "SUCCESS":
                        raise RuntimeError(execution.execution_message or "sql execute failed")
                    intent_state.phase = IntentPhase.INTERPRET_RESULT
                    self._checkpoint(intent_state.intent_id, "execute_sql_completed")

                if intent_state.phase == IntentPhase.INTERPRET_RESULT:
                    self._ilog.phase(intent_state.intent_id, IntentPhase.INTERPRET_RESULT.value)
                    query_intent_text = intent_state.query_intent_text or intent_state.intent_text
                    run = self.runner.run(
                        self.interpreter_agent,
                        {
                            "intent": query_intent_text,
                            "selected_sql": intent_state.selected_sql or "",
                            "execution_result": intent_state.execution_result.model_dump(mode="json") if intent_state.execution_result else {},
                        },
                        steps=workflow_state.steps,
                    )
                    if not run.ok or run.output is None:
                        raise RuntimeError(run.error or "ResultInterpreterAgent failed")
                    intent_state.interpretation_result = run.output
                    self._append_step(
                        workflow_state,
                        scope="intent",
                        owner_id=intent_state.intent_id,
                        agent=self.interpreter_agent.name,
                        phase=IntentPhase.INTERPRET_RESULT.value,
                        summary=intent_state.interpretation_result.answer or "结果解释完成",
                    )
                    intent_state.error_state = None
                    intent_state.phase = IntentPhase.COMPLETED
                    intent_state.status = IntentStatus.COMPLETED
                    self._checkpoint(intent_state.intent_id, "interpret_result_completed")
                    return self._result_from_state(intent_state, status=IntentStatus.COMPLETED)
            except Exception as exc:
                self._ilog.phase_error(intent_state.intent_id, intent_state.phase.value, str(exc))
                current_stage = self._phase_to_stage(intent_state.phase)
                routed = self.error_router.route(
                    current_stage=current_stage,
                    current_input=self._current_input(intent_state),
                    error_message=str(exc),
                    upstream_artifacts=self._upstream(intent_state),
                    steps=workflow_state.steps,
                )
                self._append_step(
                    workflow_state,
                    scope="intent",
                    owner_id=intent_state.intent_id,
                    agent="error_attribution",
                    phase=current_stage.value,
                    summary=routed.message or "错误归因完成",
                )
                self._ilog.error_routed(
                    intent_state.intent_id,
                    owner_stage=routed.owner_stage.value,
                    current_stage=routed.current_stage.value,
                    repair_action=routed.repair_action.value,
                    error_code=routed.error_code,
                    route_message=routed.message,
                    evidence_router=routed.evidence.get("router", ""),
                )
                intent_state.error_state = routed
                intent_state.repair_history.append(
                    RepairRecord(
                        owner_stage=routed.owner_stage,
                        current_stage=routed.current_stage,
                        repair_action=routed.repair_action,
                        message=routed.message,
                    )
                )
                if not self._apply_repair(intent_state, routed):
                    intent_state.status = IntentStatus.FAILED
                    intent_state.phase = IntentPhase.FAILED
                    self._checkpoint(intent_state.intent_id, "intent_failed")
                    return self._result_from_state(intent_state, status=IntentStatus.FAILED)
                repairs += 1
        intent_state.status = IntentStatus.FAILED
        intent_state.phase = IntentPhase.FAILED
        self._checkpoint(intent_state.intent_id, "intent_failed")
        return self._result_from_state(intent_state, status=IntentStatus.FAILED)

    def _run_schema_build_dag(self, intent_state: IntentState, workflow_state: WorkflowState) -> ModuleResult[Schema]:
        schema_intent_text = str(intent_state.schema_intent_text or intent_state.intent_text or "").strip()
        if not schema_intent_text:
            return ModuleResult(
                status=ModuleStatus.FATAL_ERROR,
                error=ModuleError(
                    status="FATAL_ERROR",
                    owner_stage=StageName.SCHEMALINK,
                    current_stage=StageName.SCHEMALINK,
                    error_code="SCHEMA_INTENT_EMPTY",
                    message="schema intent is empty",
                    hint="schema intent is empty",
                    repair_action=RepairAction.STOP,
                ),
            )

        skip_cached = intent_state.skip_schema_intent_decompose_once
        if skip_cached:
            intent_state.skip_schema_intent_decompose_once = False

        if skip_cached and intent_state.schema_sub_intent_graph_cache is not None:
            graph = intent_state.schema_sub_intent_graph_cache
        else:
            decompose_run = self.runner.run(
                self.schema_intent_decomposer,
                {
                    "schema_intent": schema_intent_text,
                    "database_scope": workflow_state.database_scope,
                    "current_schema": (intent_state.initial_schema.model_dump(mode="json") if intent_state.initial_schema else {}),
                },
                steps=workflow_state.steps,
            )
            if not decompose_run.ok or decompose_run.output is None:
                return ModuleResult(
                    status=ModuleStatus.FATAL_ERROR,
                    error=ModuleError(
                        status="FATAL_ERROR",
                        owner_stage=StageName.SCHEMALINK,
                        current_stage=StageName.SCHEMALINK,
                        error_code="SCHEMA_INTENT_DECOMPOSE_FAILED",
                        message=decompose_run.error or "schema intent decompose failed",
                        hint=decompose_run.error or "schema intent decompose failed",
                        repair_action=RepairAction.STOP,
                    ),
                )
            sub_decompose = decompose_run.output
            try:
                graph = IntentTopologyBuilder().build(sub_decompose)
            except Exception as exc:
                return ModuleResult(
                    status=ModuleStatus.FATAL_ERROR,
                    error=ModuleError(
                        status="FATAL_ERROR",
                        owner_stage=StageName.SCHEMALINK,
                        current_stage=StageName.SCHEMALINK,
                        error_code="SCHEMA_INTENT_TOPOLOGY_FAILED",
                        message=str(exc),
                        hint="schema sub-intent topology failed",
                        repair_action=RepairAction.STOP,
                    ),
                )
            if not graph.topo_layers:
                return ModuleResult(
                    status=ModuleStatus.FATAL_ERROR,
                    error=ModuleError(
                        status="FATAL_ERROR",
                        owner_stage=StageName.SCHEMALINK,
                        current_stage=StageName.SCHEMALINK,
                        error_code="SCHEMA_INTENT_DAG_EMPTY",
                        message="schema sub-intent dag is empty",
                        hint="schema sub-intent dag is empty",
                        repair_action=RepairAction.STOP,
                    ),
                )

            source_ids = {edge.source for edge in graph.edges}
            sink_candidates = [node_id for node_id in graph.nodes.keys() if node_id not in source_ids]
            if len(sink_candidates) != 1:
                return ModuleResult(
                    status=ModuleStatus.FATAL_ERROR,
                    error=ModuleError(
                        status="FATAL_ERROR",
                        owner_stage=StageName.SCHEMALINK,
                        current_stage=StageName.SCHEMALINK,
                        error_code="SCHEMA_INTENT_DAG_NOT_CONVERGED",
                        message="schema sub-intent dag must converge to one sink node",
                        hint="schema sub-intent dag must converge to one sink node",
                        repair_action=RepairAction.STOP,
                    ),
                )

            intent_state.schema_sub_intent_decompose_cache = sub_decompose
            intent_state.schema_sub_intent_graph_cache = graph

        accumulated_schema = self._schema_build_accumulated_base(intent_state)
        for layer in graph.topo_layers:
            for node_id in layer:
                node = graph.nodes.get(node_id)
                if node is None:
                    continue
                sub_state = SchemaLinkState(
                    mode="ENRICH" if accumulated_schema.databases else "BUILD",
                    intent_id=f"{intent_state.intent_id}:{node_id}",
                    intent_text=node.intent,
                    known_information_text=intent_state.known_information_text,
                    current_schema=accumulated_schema.model_copy(deep=True),
                )
                intent_state.schemalink_state = sub_state
                out = self.schemalink.run(
                    sub_state,
                    workflow_state.database_scope,
                    workflow_state=workflow_state,
                    steps=workflow_state.steps,
                )
                if out.status == ModuleStatus.WAIT_USER:
                    return out
                if out.status != ModuleStatus.SUCCESS or out.payload is None:
                    self._set_schemalink_accumulated_checkpoint(intent_state, sub_state.current_schema)
                    return out
                accumulated_schema = merge_schema(
                    accumulated_schema,
                    out.payload,
                    description_merge=self._merge_description_llm,
                )
                self._set_schemalink_accumulated_checkpoint(intent_state, accumulated_schema)
        self._clear_schemalink_accumulated_checkpoint(intent_state)
        return ModuleResult(status=ModuleStatus.SUCCESS, payload=accumulated_schema)

    def _merge_description_llm(self, existing: str, incoming: str) -> str:
        existing_text = str(existing or "").strip()
        incoming_text = str(incoming or "").strip()
        if not existing_text:
            return incoming_text
        if not incoming_text:
            return existing_text
        if incoming_text in existing_text:
            return existing_text
        if existing_text in incoming_text:
            return incoming_text
        run = self.runner.run(
            self.schema_desc_merger,
            {
                "existing_description": existing_text,
                "incoming_description": incoming_text,
            },
            steps=None,
        )
        if not run.ok or run.output is None:
            return merge_description(existing_text, incoming_text)
        merged = str(run.output.merged_description or "").strip()
        return merged or merge_description(existing_text, incoming_text)

    def resume(self, intent_state: IntentState, workflow_state: WorkflowState, reply: str) -> IntentExecutionResult:
        if intent_state.schemalink_state is not None:
            original = intent_state.schemalink_state.known_information_text
            intent_state.schemalink_state.known_information_text = f"{original}\n\n【用户补充】\n{reply.strip()}"
            intent_state.known_information_text = intent_state.schemalink_state.known_information_text
        if intent_state.phase == IntentPhase.WAIT_USER:
            intent_state.phase = IntentPhase.SCHEMALINK
            intent_state.status = IntentStatus.READY
        return self.execute(intent_state, workflow_state)

    def _phase_to_stage(self, phase: IntentPhase) -> StageName:
        mapping = {
            IntentPhase.CONTEXT_BUILDING: StageName.CONTEXT_BUILD,
            IntentPhase.SCHEMALINK: StageName.SCHEMALINK,
            IntentPhase.PLAN_RA: StageName.PLAN_RA,
            IntentPhase.RENDER_SQL: StageName.RENDER_SQL,
            IntentPhase.VALIDATE_SQL: StageName.VALIDATE_SQL,
            IntentPhase.EXECUTE_SQL: StageName.EXECUTE_SQL,
            IntentPhase.INTERPRET_RESULT: StageName.INTERPRET_RESULT,
        }
        return mapping.get(phase, StageName.EXECUTE_SQL)

    def _current_input(self, intent_state: IntentState) -> dict:
        query_intent_text = intent_state.query_intent_text or intent_state.intent_text
        return {
            "intent": query_intent_text,
            "phase": intent_state.phase.value,
            "selected_sql": intent_state.selected_sql,
            "sql_validation_feedback": intent_state.sql_validation_feedback,
        }

    def _upstream(self, intent_state: IntentState) -> dict:
        schemalink_state = intent_state.schemalink_state
        return {
            "resolved_schema": intent_state.resolved_schema.model_dump(mode="json") if intent_state.resolved_schema else {},
            "ra_plan": intent_state.ra_plan.model_dump(mode="json") if intent_state.ra_plan else {},
            "sql_render_result": intent_state.sql_render_result.model_dump(mode="json") if intent_state.sql_render_result else {},
            "known_information_text": intent_state.known_information_text,
            "sql_validation_feedback": intent_state.sql_validation_feedback,
            "schemalink_last_tool_output": schemalink_state.last_tool_output if schemalink_state else {},
            "schemalink_last_write_result": schemalink_state.last_write_result if schemalink_state else {},
        }

    def _apply_repair(self, intent_state: IntentState, error: ModuleError) -> bool:
        action = error.repair_action
        if action == RepairAction.REBUILD_SCHEMA:
            mode = "BUILD"
            if intent_state.schemalink_state is None:
                return False
            intent_state.schema_sub_intent_decompose_cache = None
            intent_state.schema_sub_intent_graph_cache = None
            intent_state.skip_schema_intent_decompose_once = False
            self._clear_schemalink_accumulated_checkpoint(intent_state)
            intent_state.schemalink_state.mode = mode
            intent_state.schemalink_state.last_tool_output = {}
            intent_state.schemalink_state.last_write_result = {"error": error.message}
            intent_state.schemalink_state.current_schema = (
                intent_state.initial_schema.model_copy(deep=True)
                if intent_state.initial_schema is not None
                else intent_state.schemalink_state.current_schema.model_copy(deep=True)
            )
            intent_state.phase = IntentPhase.SCHEMALINK
            return True
        if action == RepairAction.ENRICH_SCHEMA:
            if intent_state.schemalink_state is None:
                return False
            intent_state.schemalink_state.mode = "ENRICH"
            intent_state.schemalink_state.last_tool_output = {}
            intent_state.schemalink_state.last_write_result = {"error": error.message}
            intent_state.phase = IntentPhase.SCHEMALINK
            intent_state.skip_schema_intent_decompose_once = True
            return True
        if action == RepairAction.REPLAN_RA:
            intent_state.ra_plan = None
            intent_state.sql_render_result = None
            intent_state.sql_validation_result = None
            intent_state.sql_render_feedback = ""
            intent_state.selected_sql = None
            intent_state.execution_result = None
            intent_state.interpretation_result = None
            intent_state.phase = IntentPhase.PLAN_RA
            return True
        if action == RepairAction.RERENDER_SQL:
            intent_state.sql_render_result = None
            intent_state.sql_validation_result = None
            intent_state.sql_validation_feedback = ""
            intent_state.selected_sql = None
            intent_state.execution_result = None
            intent_state.interpretation_result = None
            intent_state.phase = IntentPhase.RENDER_SQL
            return True
        if action == RepairAction.REVALIDATE_SQL:
            intent_state.phase = IntentPhase.VALIDATE_SQL
            return True
        if action == RepairAction.REEXECUTE_SQL:
            intent_state.execution_result = None
            intent_state.phase = IntentPhase.EXECUTE_SQL
            return True
        if action == RepairAction.REINTERPRET_RESULT:
            intent_state.phase = IntentPhase.INTERPRET_RESULT
            return True
        if action == RepairAction.RETRY_CURRENT:
            return True
        return False

    def _checkpoint(self, owner_id: str, label: str) -> None:
        if self.checkpoint_cb is not None:
            self.checkpoint_cb(scope="intent", owner_id=owner_id, label=label)

    def _schema_build_accumulated_base(self, intent_state: IntentState) -> Schema:
        checkpoint = intent_state.schemalink_accumulated_schema_checkpoint
        if checkpoint is not None:
            return checkpoint.model_copy(deep=True)
        if intent_state.initial_schema is not None:
            return intent_state.initial_schema.model_copy(deep=True)
        return Schema()

    def _set_schemalink_accumulated_checkpoint(self, intent_state: IntentState, schema: Schema) -> None:
        intent_state.schemalink_accumulated_schema_checkpoint = schema.model_copy(deep=True)

    def _clear_schemalink_accumulated_checkpoint(self, intent_state: IntentState) -> None:
        intent_state.schemalink_accumulated_schema_checkpoint = None

    def _append_step(
        self,
        workflow_state: WorkflowState,
        *,
        scope: str,
        owner_id: str,
        agent: str,
        phase: str,
        summary: str,
    ) -> None:
        self._step_limiter().ensure_can_append(len(workflow_state.steps))
        workflow_state.steps.append(
            AgentStep(
                step_id=f"{scope}_{owner_id}_{len(workflow_state.steps)+1}",
                scope=scope,
                owner_id=owner_id,
                agent=agent,
                phase=phase,
                summary=summary,
                created_at=time.time(),
            )
        )

    def _result_from_state(self, intent_state: IntentState, *, status: IntentStatus, ask_ticket=None) -> IntentExecutionResult:
        interpretation = intent_state.interpretation_result
        query_intent_text = intent_state.query_intent_text or intent_state.intent_text
        return IntentExecutionResult(
            intent_id=intent_state.intent_id,
            intent=query_intent_text,
            status=status,
            resolved_schema=intent_state.resolved_schema,
            ra_plan=intent_state.ra_plan,
            selected_sql=intent_state.selected_sql or "",
            execution_result=intent_state.execution_result,
            interpretation_result=interpretation,
            answer=interpretation.answer if interpretation else "",
            sql=intent_state.selected_sql or "",
            error=intent_state.error_state,
            ask_ticket=ask_ticket,
        )

    def _build_sql_render_feedback(self, validation: object) -> str:
        try:
            reports = getattr(validation, "reports", None) or []
            for report in reports:
                errors = getattr(report, "errors", None) or []
                if errors:
                    return str(errors[0])
        except Exception:
            pass
        return "sql validation failed"
