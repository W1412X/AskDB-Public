from __future__ import annotations

from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from .enums import AskTicketStatus, IntentPhase, IntentStatus, ModuleStatus, RepairAction, StageName, WorkflowStatus

T = TypeVar("T")


class WorkflowCheckpoint(BaseModel):
    checkpoint_id: str
    scope: Literal["workflow", "intent", "schemalink"]
    owner_id: str = ""
    label: str = ""
    created_at: float = 0.0


class AgentStep(BaseModel):
    step_id: str
    scope: Literal["workflow", "intent", "schemalink"]
    owner_id: str = ""
    agent: str = ""
    phase: str = ""
    summary: str = ""
    round_index: int | None = None
    created_at: float = 0.0


class WorkflowRequest(BaseModel):
    workflow_id: str | None = None
    query: str
    database_scope: list[str] = Field(default_factory=list)
    sql_dialect: str = "mysql"
    model_name: str = ""


class UserReply(BaseModel):
    ticket_id: str
    reply: str


class ColumnSpec(BaseModel):
    type: str = ""
    description: str = ""
    indexes: list[str] = Field(default_factory=list)
    sample_values: list[str] = Field(default_factory=list)


class TableSchema(BaseModel):
    description: str = ""
    columns: dict[str, ColumnSpec] = Field(default_factory=dict)


class DatabaseSchema(BaseModel):
    description: str = ""
    tables: dict[str, TableSchema] = Field(default_factory=dict)


class JoinPath(BaseModel):
    left: str
    right: str
    cardinality: str = ""
    null_rate: float | None = None


class Schema(BaseModel):
    databases: dict[str, DatabaseSchema] = Field(default_factory=dict)
    join_paths: list[JoinPath] = Field(default_factory=list)


class ResumePoint(BaseModel):
    intent_id: str = ""
    phase: str = ""
    checkpoint: str = ""


class AskTicket(BaseModel):
    ticket_id: str
    scope: Literal["workflow", "intent", "schemalink"]
    owner_id: str
    question_id: str
    question: str
    why_needed: str = ""
    acceptance_criteria: list[str] = Field(default_factory=list)
    resume_point: ResumePoint = Field(default_factory=ResumePoint)
    priority: int = 1
    status: AskTicketStatus = AskTicketStatus.OPEN
    answer: str = ""
    fingerprint: str = ""


class ModuleError(BaseModel):
    status: Literal["RETRYABLE_ERROR", "FATAL_ERROR", "UPSTREAM_ERROR"]
    owner_stage: StageName
    current_stage: StageName
    error_code: str
    message: str
    hint: str = ""
    repair_action: RepairAction
    evidence: dict[str, Any] = Field(default_factory=dict)


class ModuleResult(BaseModel, Generic[T]):
    status: ModuleStatus
    payload: T | None = None
    ask_ticket: AskTicket | None = None
    error: ModuleError | None = None
    mark: str = ""


class IntentPairItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    query: str
    schema_intent: str = Field(alias="schema")


class IntentPairDecomposeResult(BaseModel):
    intents: list[IntentPairItem] = Field(default_factory=list)


class IntentDecomposeItem(BaseModel):
    intent_id: str
    intent: str
    dependent_intent_ids: list[str] = Field(default_factory=list)


class IntentDecomposeResult(BaseModel):
    intents: list[IntentDecomposeItem] = Field(default_factory=list)


class IntentDecomposeValidationResult(BaseModel):
    status: Literal["SUCCESS", "FAILED"]
    rationale: str = ""
    issues: list[str] = Field(default_factory=list)
    suggested_fix: str = ""


class IntentNode(BaseModel):
    intent_id: str
    intent: str
    dependent_intent_ids: list[str] = Field(default_factory=list)


class IntentEdge(BaseModel):
    source: str
    target: str


class IntentGraph(BaseModel):
    nodes: dict[str, IntentNode] = Field(default_factory=dict)
    edges: list[IntentEdge] = Field(default_factory=list)
    topo_layers: list[list[str]] = Field(default_factory=list)


class DependencyItem(BaseModel):
    intent_id: str
    intent: str
    resolved_schema: Schema = Field(default_factory=Schema)
    sql: str = ""
    result_summary: str = ""


class DependencyContext(BaseModel):
    known_information: list[DependencyItem] = Field(default_factory=list)
    current_intent: str = ""
    initial_schema: Schema = Field(default_factory=Schema)


class ToolTask(BaseModel):
    goal: str = ""


class AskRequest(BaseModel):
    question: str = ""
    why_needed: str = ""
    acceptance_criteria: list[str] = Field(default_factory=list)


class SchemaOrchestratorOutput(BaseModel):
    action: Literal["WRITE_SCHEMA", "CALL_TOOL", "ASK_USER", "SUCCESS"]
    description: str = ""
    tool_task: ToolTask = Field(default_factory=ToolTask)
    ask_request: AskRequest = Field(default_factory=AskRequest)


class SchemaWrite(BaseModel):
    type: Literal[
        "db_create",
        "table_create",
        "column_create",
        "column_description_merge",
        "join_path_create",
    ]
    database: str = ""
    table: str = ""
    column: str = ""
    spec: ColumnSpec | None = None
    left: str = ""
    right: str = ""
    cardinality: str = ""
    null_rate: float | None = None
    scope: str = ""
    text: str = ""


class SchemaDelta(BaseModel):
    writes: list[SchemaWrite] = Field(default_factory=list)
    summary: str = ""
    invalid_targets: list[str] = Field(default_factory=list)


class SchemaToolOutput(BaseModel):
    class ConfirmMoreInfo(BaseModel):
        column: str
        description: str = ""

    class ConfirmJoinPath(BaseModel):
        left: str
        right: str
        # user-provided contract: "cardinate"
        cardinate: str = ""
        null_rate: float | None = None

    class Confirm(BaseModel):
        tables: list[str] = Field(default_factory=list)  # ["db.table", ...]
        columns: list[str] = Field(default_factory=list)  # ["db.table.column", ...]
        more_info: list["SchemaToolOutput.ConfirmMoreInfo"] = Field(default_factory=list)
        join_paths: list["SchemaToolOutput.ConfirmJoinPath"] = Field(default_factory=list)

    confirm: Confirm = Field(default_factory=Confirm)
    suggestion: list[str] = Field(default_factory=list)


class RAEntity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    database: str
    table: str
    alias: str
    columns: list[str] = Field(default_factory=list)


class RAJoin(BaseModel):
    model_config = ConfigDict(extra="forbid")

    left_alias: str
    right_alias: str
    left_column: str
    right_column: str
    join_kind: Literal["inner", "left", "right", "full", "cross", "natural"] = "left"
    on_expr: str = ""
    lateral: bool = False
    reason: str = ""


class RAFilter(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expr: str
    clause: Literal["where", "having", "qualify"] = "where"
    predicate_kind: Literal["expr", "exists", "not_exists"] = "expr"
    reason: str = ""
    required: bool = True


class RAAggregation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expr: str
    alias: str
    reason: str = ""


class RASortItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expr: str
    direction: Literal["asc", "desc"] = "asc"
    nulls: Literal["first", "last", ""] = ""
    reason: str = ""


class RAWindowDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    partition_by: list[str] = Field(default_factory=list)
    order_by: list[RASortItem] = Field(default_factory=list)
    frame: str = ""


class RAWindowExpression(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expr: str
    alias: str
    window_name: str = ""
    reason: str = ""


class RACTENode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    body: "RelationalPlan"
    recursive: bool = False


class RAFromDerived(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alias: str
    body: "RelationalPlan"
    lateral: bool = False
    reason: str = ""


class RAOutputContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    row_semantics: str = ""
    required_columns: list[str] = Field(default_factory=list)


class RelationalPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str = ""
    plan_kind: Literal["select", "set"] = "select"
    ctes: list[RACTENode] = Field(default_factory=list)
    entities: list[RAEntity] = Field(default_factory=list)
    from_derived: list[RAFromDerived] = Field(default_factory=list)
    joins: list[RAJoin] = Field(default_factory=list)
    filters: list[RAFilter] = Field(default_factory=list)
    aggregations: list[RAAggregation] = Field(default_factory=list)
    window_definitions: list[RAWindowDefinition] = Field(default_factory=list)
    window_expressions: list[RAWindowExpression] = Field(default_factory=list)
    group_by_variant: Literal["simple", "rollup", "cube", "grouping_sets"] = "simple"
    group_by: list[str] = Field(default_factory=list)
    grouping_sets: list[list[str]] = Field(default_factory=list)
    distinct: bool = False
    order_by: list[RASortItem] = Field(default_factory=list)
    limit: int | None = None
    offset: int | None = None
    for_update: str = ""
    optimizer_hints: list[str] = Field(default_factory=list)
    output_contract: RAOutputContract = Field(default_factory=RAOutputContract)
    assumptions: list[str] = Field(default_factory=list)
    set_branches: list["RelationalPlan"] = Field(default_factory=list)
    set_combine_operator: Literal["UNION", "UNION ALL", "INTERSECT", "EXCEPT"] = "UNION"


class RAPlanOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["SUCCESS", "FAILED"]
    ra: RelationalPlan = Field(default_factory=RelationalPlan)
    mark: str = ""


class SQLCandidate(BaseModel):
    sql: str
    rationale: str = ""
    assumptions: list[str] = Field(default_factory=list)
    expected_columns: list[str] = Field(default_factory=list)


class SQLRenderResult(BaseModel):
    status: Literal["SUCCESS", "FAILED"]
    candidates: list[SQLCandidate] = Field(default_factory=list)
    mark: str = ""


class SQLValidationDecision(BaseModel):
    status: Literal["ok", "fail"]
    reason: str = ""


class ValidationErrorReport(BaseModel):
    candidate_index: int
    passed: bool
    errors: list[str] = Field(default_factory=list)


class SQLValidationResult(BaseModel):
    status: Literal["SUCCESS", "FAILED"]
    best_candidate_index: int = -1
    reports: list[ValidationErrorReport] = Field(default_factory=list)


class SQLExecutionResult(BaseModel):
    status: Literal["SUCCESS", "FAILED"]
    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    execution_message: str = ""


class InterpretationResult(BaseModel):
    status: Literal["SUCCESS", "FAILED"]
    answer: str = ""
    confidence: str = "LOW"
    assumptions: list[str] = Field(default_factory=list)
    missing_information: list[str] = Field(default_factory=list)
    mark: str = ""


class ErrorAttributionOutput(BaseModel):
    owner_stage: StageName
    current_stage: StageName
    error_code: str
    message: str
    repair_action: RepairAction
    error_type: Literal["ENVIRONMENT", "UPSTREAM", "LOCAL"] = "LOCAL"
    confidence: str = "MEDIUM"


class RepairRecord(BaseModel):
    owner_stage: StageName
    current_stage: StageName
    repair_action: RepairAction
    message: str


class IntentResultSummary(BaseModel):
    intent_id: str
    intent: str
    status: IntentStatus
    answer: str = ""
    sql: str = ""
    error: ModuleError | None = None


RACTENode.model_rebuild()
RAFromDerived.model_rebuild()
RAWindowDefinition.model_rebuild()
RAWindowExpression.model_rebuild()
RelationalPlan.model_rebuild()


class WorkflowResult(BaseModel):
    workflow_id: str
    status: WorkflowStatus
    final_answer: str = ""
    intent_results: list[IntentResultSummary] = Field(default_factory=list)
    ask_ticket: AskTicket | None = None
    error: ModuleError | None = None
    view: dict[str, Any] = Field(default_factory=dict)
