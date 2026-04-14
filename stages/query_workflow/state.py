from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from .contracts import (
    AgentStep,
    AskTicket,
    DependencyContext,
    IntentDecomposeResult,
    IntentGraph,
    InterpretationResult,
    ModuleError,
    RelationalPlan,
    RepairRecord,
    SQLExecutionResult,
    SQLRenderResult,
    SQLValidationResult,
    Schema,
    WorkflowCheckpoint,
    WorkflowResult,
)
from .enums import IntentPhase, IntentStatus, WorkflowStatus


class SchemaLinkState(BaseModel):
    mode: Literal["BUILD", "ENRICH"] = "BUILD"
    intent_id: str
    intent_text: str
    known_information_text: str = ""
    current_schema: Schema = Field(default_factory=Schema)
    round_index: int = 0
    pending_question_ticket_id: str | None = None
    # ToolAgent 的结构化产出（SchemaToolOutput.model_dump）。
    last_tool_output: dict = Field(default_factory=dict)
    # 写入阶段的执行结果（status/invalid_targets/write_no_effect/等），与工具输出分离，避免 orch 误读。
    last_write_result: dict = Field(default_factory=dict)
    last_tool_trace: list[dict] = Field(default_factory=list)
    invalid_write_count: int = 0
    last_write_fingerprint: str = ""


class AskQueueState(BaseModel):
    active_ticket_id: str | None = None
    queued_ticket_ids: list[str] = Field(default_factory=list)
    tickets: dict[str, AskTicket] = Field(default_factory=dict)


class IntentState(BaseModel):
    intent_id: str
    intent_text: str
    query_intent_text: str = ""
    schema_intent_text: str = ""
    dependent_intent_ids: list[str] = Field(default_factory=list)
    status: IntentStatus = IntentStatus.PENDING
    phase: IntentPhase = IntentPhase.CONTEXT_BUILDING
    dependency_context: DependencyContext | None = None
    known_information_text: str = ""
    initial_schema: Schema | None = None
    schemalink_accumulated_schema_checkpoint: Schema | None = None
    schema_sub_intent_decompose_cache: IntentDecomposeResult | None = None
    schema_sub_intent_graph_cache: IntentGraph | None = None
    skip_schema_intent_decompose_once: bool = False
    schemalink_state: SchemaLinkState | None = None
    resolved_schema: Schema | None = None
    ra_plan: RelationalPlan | None = None
    sql_render_result: SQLRenderResult | None = None
    sql_validation_result: SQLValidationResult | None = None
    sql_render_feedback: str = ""
    sql_validation_feedback: str = ""
    selected_sql: str | None = None
    execution_result: SQLExecutionResult | None = None
    interpretation_result: InterpretationResult | None = None
    repair_history: list[RepairRecord] = Field(default_factory=list)
    error_state: ModuleError | None = None


class WorkflowState(BaseModel):
    workflow_id: str
    original_query: str
    normalized_query: str
    database_scope: list[str] = Field(default_factory=list)
    status: WorkflowStatus = WorkflowStatus.NEW
    intent_graph: "IntentGraph | None" = None
    intents: dict[str, IntentState] = Field(default_factory=dict)
    ask_queue: AskQueueState = Field(default_factory=AskQueueState)
    final_result: WorkflowResult | None = None
    workflow_error: ModuleError | None = None
    sql_dialect: str = "mysql"
    model_name: str = ""
    checkpoints: list[WorkflowCheckpoint] = Field(default_factory=list)
    steps: list[AgentStep] = Field(default_factory=list)


from .contracts import IntentGraph


def state_to_dict(state: WorkflowState) -> dict:
    return state.model_dump(mode="json")


def state_from_dict(payload: dict) -> WorkflowState:
    return WorkflowState.model_validate(payload)
