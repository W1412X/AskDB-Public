from __future__ import annotations

import uuid

import time

from config import get_settings_manager
from utils.log_console import LogCategory
from utils.logger import get_logger

from ..agents.agent_runner import AgentRunner
from ..agents.intent_decomposer_agent import IntentDecomposerAgent
from ..agents.intent_decompose_validator_agent import IntentDecomposeValidatorAgent
from ..contracts import (
    AgentStep,
    AskTicket,
    IntentDecomposeItem,
    IntentDecomposeResult,
    IntentPairDecomposeResult,
    ModuleError,
    UserReply,
    WorkflowRequest,
    WorkflowResult,
)
from ..enums import IntentPhase, IntentStatus, RepairAction, StageName, WorkflowStatus
from ..execution.intent_executor import IntentExecutor
from ..repositories.ask_queue_store import AskQueueStore
from ..repositories.workflow_store import WorkflowStore
from ..runtime.ask_queue_manager import AskQueueManager
from ..runtime.checkpointing import CheckpointRecorder
from ..runtime.intent_dispatcher import IntentDispatcher
from ..runtime.intent_topology_builder import IntentTopologyBuilder
from ..runtime.result_synthesizer import ResultSynthesizer
from ..runtime.step_limiter import WorkflowStepLimiter
from ..state import IntentState, WorkflowState


class QueryWorkflowPipeline:
    def __init__(self, store: WorkflowStore | None = None) -> None:
        self.store = store or WorkflowStore()
        self.ask_queue_manager = AskQueueManager(AskQueueStore())
        self.checkpoints = CheckpointRecorder(self.store)
        self.runner = AgentRunner()
        self.logger = get_logger("query_workflow")

    def _step_limiter(self) -> WorkflowStepLimiter:
        return WorkflowStepLimiter.from_settings()

    def run(self, request: WorkflowRequest) -> WorkflowResult:
        workflow_id = request.workflow_id or f"wf_{uuid.uuid4().hex[:12]}"
        state = self._build_state(request, workflow_id=workflow_id)
        try:
            return self._run_from_decompose(state)
        except RuntimeError as exc:
            if "max_steps" not in str(exc):
                raise
            state.workflow_error = ModuleError(
                status="FATAL_ERROR",
                owner_stage=StageName.SYNTHESIZE_RESULT,
                current_stage=StageName.SYNTHESIZE_RESULT,
                error_code="WORKFLOW_STEP_LIMIT_EXCEEDED",
                message=str(exc),
                hint="increase query_workflow.max_steps",
                repair_action=RepairAction.STOP,
            )
            state.status = WorkflowStatus.FAILED
            return self._finalize(state, None)

    def resume(self, workflow_id: str, user_reply: UserReply) -> WorkflowResult:
        state = self.store.load(workflow_id)
        if state is None:
            raise ValueError("workflow_id is invalid or expired")
        ticket = state.ask_queue.tickets.get(user_reply.ticket_id)
        if ticket is None:
            raise ValueError("ticket_id not found")
        ticket = self.ask_queue_manager.submit_reply(state, user_reply)
        self.checkpoints.record(state, scope=ticket.scope, owner_id=ticket.owner_id, label=f"reply_{ticket.ticket_id}")
        if ticket.scope == "workflow":
            state.original_query = f"{state.original_query}\n\n用户补充：{user_reply.reply.strip()}".strip()
            state.normalized_query = " ".join(state.original_query.split())
            state.intents = {}
            state.intent_graph = None
            state.workflow_error = None
            state.final_result = None
            try:
                return self._run_from_decompose(state)
            except RuntimeError as exc:
                if "max_steps" not in str(exc):
                    raise
                state.workflow_error = ModuleError(
                    status="FATAL_ERROR",
                    owner_stage=StageName.SYNTHESIZE_RESULT,
                    current_stage=StageName.SYNTHESIZE_RESULT,
                    error_code="WORKFLOW_STEP_LIMIT_EXCEEDED",
                    message=str(exc),
                    hint="increase query_workflow.max_steps",
                    repair_action=RepairAction.STOP,
                )
                state.status = WorkflowStatus.FAILED
                return self._finalize(state, None)
        owner = self._resolve_intent_owner(state, ticket.owner_id, ticket.resume_point.intent_id)
        if owner is None:
            raise ValueError("ticket owner not found")
        executor = self._build_executor(state)
        resumed = executor.resume(owner, state, user_reply.reply)
        resumed_ticket = resumed.ask_ticket
        if resumed.status == IntentStatus.WAIT_USER and resumed_ticket is not None:
            resumed_ticket = self.ask_queue_manager.create_ticket(state, resumed_ticket)
            state.status = WorkflowStatus.WAIT_USER
            self.store.save(state)
            return self._finalize(state, resumed_ticket)
        resumed_ticket = self._dispatch(state)
        return self._finalize(state, resumed_ticket)

    def _resolve_intent_owner(self, state: WorkflowState, owner_id: str, resume_intent_id: str = "") -> IntentState | None:
        direct = state.intents.get(owner_id)
        if direct is not None:
            return direct
        for candidate in [owner_id, resume_intent_id]:
            candidate = str(candidate or "").strip()
            if not candidate:
                continue
            root_id = candidate.split(":", 1)[0]
            if root_id in state.intents:
                return state.intents[root_id]
        return None

    def _build_state(self, request: WorkflowRequest, *, workflow_id: str) -> WorkflowState:
        return WorkflowState(
            workflow_id=workflow_id,
            original_query=request.query,
            normalized_query=" ".join(str(request.query or "").split()),
            database_scope=request.database_scope,
            status=WorkflowStatus.RUNNING,
            sql_dialect=request.sql_dialect,
            model_name=request.model_name or get_settings_manager().config.stages.query_workflow.model_name,
        )

    def _run_from_decompose(self, state: WorkflowState) -> WorkflowResult:
        state.status = WorkflowStatus.RUNNING
        self.logger.info("workflow decompose start", workflow_id=state.workflow_id, category=LogCategory.WORKFLOW)
        decompose_agent = IntentDecomposerAgent()
        decompose_agent.model_name = state.model_name
        validator_agent = IntentDecomposeValidatorAgent()
        validator_agent.model_name = state.model_name
        qw = get_settings_manager().config.stages.query_workflow
        max_self_repair = max(0, int(qw.max_decompose_self_repair))
        decompose = None
        normalized_items: list[IntentDecomposeItem] = []
        attempt_query = state.original_query
        last_reason = ""
        failure_kind = "INTENT_DECOMPOSE_FAILED"

        for attempt in range(max_self_repair + 1):
            payload = {"query": attempt_query, "database_scope": state.database_scope}
            run = self.runner.run(decompose_agent, payload, steps=state.steps)
            if not run.ok or run.output is None:
                failure_kind = "INTENT_DECOMPOSE_FAILED"
                last_reason = run.error or "IntentDecomposerAgent did not return valid output"
            else:
                candidate = run.output
                self._append_step(
                    state,
                    scope="workflow",
                    owner_id=state.workflow_id,
                    agent=decompose_agent.name,
                    phase="INTENT_DECOMPOSE",
                    summary=f"拆分得到 {len(candidate.intents)} 个意图",
                )
                validation = self.runner.run(
                    validator_agent,
                    {"query": state.original_query, "intents": [item.model_dump(mode='json', by_alias=True) for item in candidate.intents]},
                    steps=state.steps,
                )
                if not validation.ok or validation.output is None:
                    validation = None
                elif validation.output is not None:
                    self._append_step(
                        state,
                        scope="workflow",
                        owner_id=state.workflow_id,
                        agent=validator_agent.name,
                        phase="INTENT_DECOMPOSE_VALIDATE",
                        summary=validation.output.rationale or "意图拆分校验完成",
                    )
                reject = False
                if validation and validation.output and validation.output.status != "SUCCESS":
                    if (validation.output.issues or []) or (validation.output.suggested_fix or ""):
                        reject = True
                if reject:
                    failure_kind = "INTENT_DECOMPOSE_INVALID"
                    issues = "；".join(validation.output.issues or []) if validation and validation.output else ""
                    last_reason = (validation.output.rationale if validation and validation.output else "") or issues or "意图拆分不合理"
                else:
                    normalized_candidate = self._normalize_intent_items(candidate)
                    if normalized_candidate:
                        decompose = candidate
                        normalized_items = normalized_candidate
                        break
                    failure_kind = "INTENT_DECOMPOSE_EMPTY"
                    last_reason = "intent_divide 没有输出可执行的 query/schema 对"

            if attempt < max_self_repair:
                attempt_query = self._build_decompose_retry_query(state.original_query, last_reason, attempt + 1)
                self.logger.warning(
                    "workflow decompose self-retry",
                    workflow_id=state.workflow_id,
                    attempt=attempt + 1,
                    max_retry=max_self_repair,
                    reason=last_reason,
                    category=LogCategory.WORKFLOW,
                )
                continue

        if decompose is None or not normalized_items:
            if failure_kind == "INTENT_DECOMPOSE_INVALID":
                question = "当前问题拆分后无法保证查询链完整，请补充更完整的业务目标或明确要输出的结果。"
                acceptance = ["明确主查询链", "明确必要过滤条件或输出口径"]
            elif failure_kind == "INTENT_DECOMPOSE_EMPTY":
                question = "当前问题拆分后未形成有效的 query/schema 对，请补充更明确的查询要求。"
                acceptance = ["至少提供一个完整查询目标", "确保查询目标可独立执行"]
            else:
                question = "当前问题无法稳定拆分为可执行意图，请补充更明确的业务目标、统计口径或数据范围。"
                acceptance = ["明确要统计或查询的对象", "明确时间范围或过滤范围"]
            why_needed = (last_reason or "intent decompose failed").strip()
            if max_self_repair > 0:
                why_needed = f"{why_needed}（已自动重试/纠偏 {max_self_repair} 次）"
            ticket = AskTicket(
                ticket_id=f"ask_{state.workflow_id}_decompose",
                scope="workflow",
                owner_id=state.workflow_id,
                question_id="decompose_clarification",
                question=question,
                why_needed=why_needed,
                acceptance_criteria=acceptance,
                resume_point={"intent_id": "", "phase": "INTENT_DECOMPOSE", "checkpoint": "rerun_decompose"},
            )
            ticket = self.ask_queue_manager.create_ticket(state, ticket)
            state.workflow_error = ModuleError(
                status="RETRYABLE_ERROR",
                owner_stage=StageName.INTENT_DECOMPOSE,
                current_stage=StageName.INTENT_DECOMPOSE,
                error_code=failure_kind,
                message=last_reason or "Intent decomposer failed",
                hint="ask-user and rerun decomposer",
                repair_action=RepairAction.ASK_USER,
            )
            return self._finalize(state, ticket)

        self.logger.info(
            "workflow decompose completed",
            workflow_id=state.workflow_id,
            intent_count=len(decompose.intents),
            category=LogCategory.WORKFLOW,
        )
        self.checkpoints.record(state, scope="workflow", label="intent_decompose_completed")
        try:
            graph = IntentTopologyBuilder().build(IntentDecomposeResult(intents=normalized_items))
        except Exception as exc:
            ticket = AskTicket(
                ticket_id=f"ask_{state.workflow_id}_topology",
                scope="workflow",
                owner_id=state.workflow_id,
                question_id="topology_clarification",
                question="当前问题中的子任务依赖关系不够明确，请补充哪些结果需要先得到，或直接改写成更线性的查询需求。",
                why_needed=f"意图拓扑构建失败：{exc}",
                acceptance_criteria=["说明先后依赖关系", "或直接重述成一步一步的查询目标"],
                resume_point={"intent_id": "", "phase": "INTENT_TOPOLOGY", "checkpoint": "rerun_decompose"},
            )
            ticket = self.ask_queue_manager.create_ticket(state, ticket)
            state.workflow_error = ModuleError(
                status="RETRYABLE_ERROR",
                owner_stage=StageName.INTENT_TOPOLOGY,
                current_stage=StageName.INTENT_TOPOLOGY,
                error_code="INTENT_TOPOLOGY_FAILED",
                message=str(exc),
                hint="ask-user and rerun decomposer",
                repair_action=RepairAction.ASK_USER,
            )
            return self._finalize(state, ticket)
        state.intent_graph = graph
        self.logger.info(
            "workflow topology completed",
            workflow_id=state.workflow_id,
            layer_count=len(graph.topo_layers),
            category=LogCategory.WORKFLOW,
        )
        self.checkpoints.record(state, scope="workflow", label="intent_topology_completed")
        for idx, item in enumerate(decompose.intents, start=1):
            query_text = str(getattr(item, "query", "") or "").strip()
            schema_text = str(getattr(item, "schema_intent", "") or "").strip()
            if not query_text or not schema_text:
                continue
            intent_id = f"intent_{idx:03d}"
            state.intents[intent_id] = IntentState(
                intent_id=intent_id,
                intent_text=query_text,
                query_intent_text=query_text,
                schema_intent_text=schema_text,
                dependent_intent_ids=[],
            )
        self.store.save(state)
        ticket = self._dispatch(state)
        return self._finalize(state, ticket)

    def _dispatch(self, state: WorkflowState) -> AskTicket | None:
        self.logger.info(
            "workflow dispatch start",
            workflow_id=state.workflow_id,
            intent_count=len(state.intents),
            category=LogCategory.WORKFLOW,
        )
        qw = get_settings_manager().config.stages.query_workflow
        max_parallel = int(qw.max_parallel_intents)
        dispatcher = IntentDispatcher(self._build_executor(state), max_parallel_intents=max_parallel)
        ticket = dispatcher.dispatch(state)
        self.store.save(state)
        if ticket is not None:
            ticket = self.ask_queue_manager.create_ticket(state, ticket)
            self.checkpoints.record(state, scope=ticket.scope, owner_id=ticket.owner_id, label=f"ask_{ticket.ticket_id}")
            self.store.save(state)
        return ticket

    def _normalize_intent_items(self, decompose: IntentPairDecomposeResult) -> list[IntentDecomposeItem]:
        normalized_items: list[IntentDecomposeItem] = []
        for idx, item in enumerate(decompose.intents, start=1):
            query_text = str(getattr(item, "query", "") or "").strip()
            schema_text = str(getattr(item, "schema_intent", "") or "").strip()
            if not query_text or not schema_text:
                continue
            normalized_items.append(
                IntentDecomposeItem(
                    intent_id=f"intent_{idx:03d}",
                    intent=query_text,
                    dependent_intent_ids=[],
                )
            )
        return normalized_items

    def _build_decompose_retry_query(self, original_query: str, reason: str, attempt: int) -> str:
        guidance = (
            "系统自动纠偏要求：\n"
            "- 你必须保留完整查询链，尤其是统计后的筛选/排序/限制条件。\n"
            "- query 表达完整业务目标；schema 只表达能力构建，不复述具体值。\n"
            "- 不要丢失“先统计再筛选”的后半段逻辑。\n"
            f"- 上一轮失败原因：{reason or '拆分不完整'}。\n"
            f"- 当前是第 {attempt} 次自动纠偏重试。"
        )
        return f"{str(original_query or '').strip()}\n\n{guidance}".strip()

    def _build_executor(self, state: WorkflowState) -> IntentExecutor:
        qw = get_settings_manager().config.stages.query_workflow
        return IntentExecutor(
            model_name=state.model_name,
            max_schemalink_rounds=int(qw.max_schemalink_rounds),
            max_repair_attempts=int(qw.max_repair_attempts_per_intent),
            max_rows=int(qw.max_rows),
            sql_timeout_ms=int(qw.sql_timeout_ms),
            checkpoint_cb=lambda scope, owner_id="", label="": self.checkpoints.record(state, scope=scope, owner_id=owner_id, label=label),
        )

    def _finalize(self, state: WorkflowState, ticket: AskTicket | None) -> WorkflowResult:
        view = build_workflow_view(state)
        if ticket is not None:
            result = WorkflowResult(
                workflow_id=state.workflow_id,
                status=WorkflowStatus.WAIT_USER,
                final_answer="",
                intent_results=[],
                ask_ticket=ticket,
                error=None,
                view=view,
            )
            state.final_result = result
            self.checkpoints.record(state, scope=ticket.scope, owner_id=ticket.owner_id, label="wait_user")
            self.store.save(state)
            return result
        result = ResultSynthesizer(model_name=state.model_name).synthesize(state, view)
        state.final_result = result
        self.checkpoints.record(state, scope="workflow", label="synthesis_completed")
        self.store.save(state)
        return result

    def _append_step(
        self,
        state: WorkflowState,
        *,
        scope: str,
        owner_id: str,
        agent: str,
        phase: str,
        summary: str,
    ) -> None:
        self._step_limiter().ensure_can_append(len(state.steps))
        state.steps.append(
            AgentStep(
                step_id=f"{scope}_{owner_id}_{len(state.steps)+1}",
                scope=scope,
                owner_id=owner_id,
                agent=agent,
                phase=phase,
                summary=summary,
                created_at=time.time(),
            )
        )


def _intent_sub_dag_snapshot(intent_state: IntentState) -> dict | None:
    g = intent_state.schema_sub_intent_graph_cache
    if g is None:
        return None
    node_rows = []
    for nid, node in g.nodes.items():
        node_rows.append(
            {
                "id": nid,
                "intent": node.intent,
                "dependent_intent_ids": list(node.dependent_intent_ids or []),
            }
        )
    return {
        "nodes": node_rows,
        "edges": [e.model_dump(mode="json") for e in g.edges],
        "topo_layers": g.topo_layers,
    }


def _schemalink_public_slice(intent_state: IntentState) -> dict | None:
    sl = intent_state.schemalink_state
    if sl is None:
        return None
    return {
        "mode": sl.mode,
        "round_index": sl.round_index,
        "intent_id": sl.intent_id,
        "last_tool_output_keys": list((sl.last_tool_output or {}).keys()) if isinstance(sl.last_tool_output, dict) else [],
        "last_write_result": sl.last_write_result if sl.last_write_result else {},
    }


def build_workflow_view(state: WorkflowState) -> dict:
    nodes = []
    edges = []
    for intent_id, intent_state in state.intents.items():
        nodes.append({"id": intent_id, "label": intent_state.query_intent_text or intent_state.intent_text, "status": intent_state.status.value})
    if state.intent_graph is not None:
        for edge in state.intent_graph.edges:
            edges.append({"source": edge.source, "target": edge.target})
    intents = []
    for intent_state in state.intents.values():
        schema = intent_state.resolved_schema.model_dump(mode="json") if intent_state.resolved_schema else {"databases": {}, "join_paths": []}
        execution_rows = []
        if intent_state.execution_result:
            for row in intent_state.execution_result.rows:
                execution_rows.append({col: row[idx] for idx, col in enumerate(intent_state.execution_result.columns)})
        ctx_done = intent_state.phase != IntentPhase.CONTEXT_BUILDING
        intents.append(
            {
                "intent_id": intent_state.intent_id,
                "description": intent_state.query_intent_text or intent_state.intent_text,
                "schema_intent": intent_state.schema_intent_text or "",
                "status": intent_state.status.value,
                "phase": intent_state.phase.value,
                "schema_sub_dag": _intent_sub_dag_snapshot(intent_state),
                "ra_plan": intent_state.ra_plan.model_dump(mode="json") if intent_state.ra_plan else None,
                "sql_render": intent_state.sql_render_result.model_dump(mode="json") if intent_state.sql_render_result else None,
                "sql_validation": intent_state.sql_validation_result.model_dump(mode="json") if intent_state.sql_validation_result else None,
                "schemalink": _schemalink_public_slice(intent_state),
                "task_flow": [
                    {"task_id": "context_build", "phase": "CONTEXT_BUILDING", "status": "completed" if ctx_done else "running"},
                    {"task_id": "schemalink", "phase": "SCHEMALINK", "status": _phase_status(intent_state, "SCHEMALINK")},
                    {"task_id": "plan_ra", "phase": "PLAN_RA", "status": _phase_status(intent_state, "PLAN_RA")},
                    {"task_id": "render_sql", "phase": "RENDER_SQL", "status": _phase_status(intent_state, "RENDER_SQL")},
                    {"task_id": "validate_sql", "phase": "VALIDATE_SQL", "status": _phase_status(intent_state, "VALIDATE_SQL")},
                    {"task_id": "execute_sql", "phase": "EXECUTE_SQL", "status": _phase_status(intent_state, "EXECUTE_SQL")},
                    {"task_id": "interpret_result", "phase": "INTERPRET_RESULT", "status": _phase_status(intent_state, "INTERPRET_RESULT")},
                ],
                "dependencies": [
                    {
                        "intent_id": dep.intent_id,
                        "intent_request": dep.intent,
                        "result_summary": dep.result_summary,
                        "sql": dep.sql,
                        "sql_preview": dep.sql[:1200],
                        "resolved_schema_summary": _schema_summary(dep.resolved_schema.model_dump(mode="json")),
                    }
                    for dep in (intent_state.dependency_context.known_information if intent_state.dependency_context else [])
                ],
                "schema": schema,
                "sql": intent_state.selected_sql or "",
                "exec_result": {
                    "row_count": intent_state.execution_result.row_count if intent_state.execution_result else 0,
                    "displayed_row_count": len(execution_rows),
                    "truncated": intent_state.execution_result.truncated if intent_state.execution_result else False,
                    "rows": execution_rows,
                },
                "interpretation": {
                    "answer": intent_state.interpretation_result.answer if intent_state.interpretation_result else "",
                    "assumptions": intent_state.interpretation_result.assumptions if intent_state.interpretation_result else [],
                },
                "error": intent_state.error_state.model_dump(mode="json") if intent_state.error_state else None,
            }
        )
    all_steps = state.steps or []
    _STEPS_VIEW_LIMIT = 500
    steps_recent = [s.model_dump(mode="json") for s in all_steps[-_STEPS_VIEW_LIMIT:]]
    return {
        "workflow_id": state.workflow_id,
        "status": state.status.value,
        "original_query": state.original_query,
        "steps_total": len(all_steps),
        "steps_shown": len(steps_recent),
        "ask_queue": {
            "active_ticket_id": state.ask_queue.active_ticket_id,
            "queued_ticket_ids": list(state.ask_queue.queued_ticket_ids),
            "tickets": {ticket_id: ticket.model_dump(mode="json") for ticket_id, ticket in state.ask_queue.tickets.items()},
        },
        "checkpoints": [item.model_dump(mode="json") for item in state.checkpoints],
        "steps_recent": steps_recent,
        "steps_truncated": len(all_steps) > _STEPS_VIEW_LIMIT,
        "topology": {"nodes": nodes, "edges": edges},
        "intents": intents,
    }


def _schema_summary(schema: dict) -> dict:
    tables = []
    table_count = 0
    column_count = 0
    for db_name, db_obj in (schema.get("databases") or {}).items():
        for table_name, table_obj in ((db_obj or {}).get("tables") or {}).items():
            columns = list(((table_obj or {}).get("columns") or {}).keys())
            table_count += 1
            column_count += len(columns)
            if len(tables) < 8:
                tables.append({"db": db_name, "table": table_name, "columns": columns[:12]})
    return {"table_count": table_count, "column_count": column_count, "tables": tables}


def _phase_status(intent_state: IntentState, phase: str) -> str:
    if intent_state.status == IntentStatus.COMPLETED:
        return "completed"
    if intent_state.status in {IntentStatus.FAILED, IntentStatus.BLOCKED_BY_UPSTREAM} and intent_state.phase.value == phase:
        return "failed"
    if intent_state.phase.value == phase:
        return "running" if intent_state.status != IntentStatus.WAIT_USER else "wait_user"
    order = ["CONTEXT_BUILDING", "SCHEMALINK", "PLAN_RA", "RENDER_SQL", "VALIDATE_SQL", "EXECUTE_SQL", "INTERPRET_RESULT"]
    if phase in order and intent_state.phase.value in order and order.index(intent_state.phase.value) > order.index(phase):
        return "completed"
    return "pending"
