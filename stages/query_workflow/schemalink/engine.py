from __future__ import annotations

import time

from config import get_settings_manager
from utils.log_console import LogCategory
from utils.logger import get_logger

from ..agents.agent_runner import AgentRunner
from ..agents.schema_tool_agent import SchemaToolAgent
from ..agents.schemalink_orchestrator_agent import SchemaLinkOrchestratorAgent
from ..contracts import AgentStep, AskTicket, ModuleError, ModuleResult, ModuleStatus, Schema, SchemaDelta, SchemaToolOutput, SchemaWrite
from ..enums import RepairAction, StageName
from ..schemalink.schema_delta_applier import SchemaDeltaApplier
from ..schemalink.join_semantic_guard import JoinSemanticGuard
from ..schemalink.ref_parse import parse_column_ref, parse_table_ref
from ..schemalink.schema_deterministic_sufficiency import deterministic_sufficiency
from ..schemalink.schema_gate import SchemaGate
from ..schemalink.schema_init_resolver import SchemaInitResolver
from ..schemalink.schema_sufficiency_validator import SchemaSufficiencyValidator
from ..schemalink.schema_validator import SchemaValidator
from ..schemalink.schema_write_planner import SchemaWritePlanner
from ..runtime.workflow_logging import SchemaLinkRuntimeLogger
from ..runtime.step_limiter import WorkflowStepLimiter
from ..state import SchemaLinkState


class SchemaLinkEngine:
    def __init__(self, model_name: str, max_rounds: int = 8, checkpoint_cb=None) -> None:
        stage = get_settings_manager().config.stages
        self.runner = AgentRunner()
        self.logger = SchemaLinkRuntimeLogger(get_logger("schemalink"))
        self.orchestrator = SchemaLinkOrchestratorAgent()
        self.tool_agent = SchemaToolAgent()
        sta = stage.schema_tool_agent
        self.tool_agent.max_tool_calls_per_round = int(sta.max_tool_calls_per_round)
        self.tool_agent.max_tool_rounds = int(sta.max_tool_rounds)
        for agent in [self.orchestrator, self.tool_agent]:
            agent.model_name = model_name
        self.init_resolver = SchemaInitResolver()
        self.write_planner = SchemaWritePlanner(self.init_resolver)
        self.schema_validator = SchemaValidator(self.init_resolver)
        self.sufficiency_validator = SchemaSufficiencyValidator(model_name=model_name)
        self.schema_gate = SchemaGate(
            self.schema_validator,
            self.sufficiency_validator,
            llm_cache_max_entries=int(stage.schema_gate.llm_cache_max_entries),
        )
        self.applier = SchemaDeltaApplier()
        self.join_semantic_guard = JoinSemanticGuard(self.init_resolver)
        self.max_rounds = max_rounds
        self.checkpoint_cb = checkpoint_cb

    def run(
        self,
        state: SchemaLinkState,
        database_scope: list[str],
        *,
        workflow_state=None,
        steps: list[AgentStep] | None = None,
    ) -> ModuleResult[Schema]:
        self._ensure_databases(state, database_scope, steps)
        for _ in range(self.max_rounds):
            if workflow_state is not None:
                WorkflowStepLimiter.from_settings().ensure_can_append(len(workflow_state.steps))
            ss = self._schema_summary(state.current_schema)
            self._record_step(
                workflow_state=workflow_state,
                steps=steps,
                scope="schemalink",
                owner_id=state.intent_id,
                agent="schemalink_engine",
                phase="SCHEMALINK",
                summary=(
                    f"round {state.round_index + 1} 开始 · mode={state.mode} · "
                    f"tables={ss.get('table_count', 0)} cols={ss.get('column_count', 0)} "
                    f"joins={ss.get('join_path_count', 0)}"
                ),
                round_index=state.round_index + 1,
            )
            self.logger.info(
                "schemalink round start",
                intent_id=state.intent_id,
                round_index=state.round_index + 1,
                mode=state.mode,
                schema_summary=self._schema_summary(state.current_schema),
                last_tool_output=state.last_tool_output,
                last_write_result=state.last_write_result,
            )
            orch_payload = {
                "intent": state.intent_text,
                "known_information_text": state.known_information_text,
                "current_schema": state.current_schema.model_dump(mode="json"),
                "database_scope": database_scope,
                "last_tool_output": state.last_tool_output,
                "last_write_result": state.last_write_result,
            }
            orch = self.runner.run(self.orchestrator, orch_payload, steps=steps)
            if not orch.ok or orch.output is None:
                self.logger.error(
                    "schemalink orchestrator failed",
                    intent_id=state.intent_id,
                    round_index=state.round_index + 1,
                    error=orch.error or "SchemaLinkOrchestratorAgent failed",
                )
                return ModuleResult(
                    status=ModuleStatus.FATAL_ERROR,
                    error=ModuleError(
                        status="FATAL_ERROR",
                        owner_stage=StageName.SCHEMALINK,
                        current_stage=StageName.SCHEMALINK,
                        error_code="SCHEMALINK_ORCHESTRATOR_FAILED",
                        message=orch.error or "SchemaLinkOrchestratorAgent failed",
                        hint=orch.error or "schemalink orchestrator failed",
                        repair_action=RepairAction.STOP,
                    ),
            )
            decision = orch.output
            state.round_index += 1
            self._record_step(
                workflow_state=workflow_state,
                steps=steps,
                scope="schemalink",
                owner_id=state.intent_id,
                agent=self.orchestrator.name,
                phase="SCHEMALINK",
                summary=decision.description or "schemalink decision",
                round_index=state.round_index,
            )
            self.logger.info(
                "schemalink decision",
                intent_id=state.intent_id,
                round_index=state.round_index,
                action=decision.action,
                description=decision.description,
                tool_task=decision.tool_task.model_dump(mode="json"),
                ask_request=decision.ask_request.model_dump(mode="json"),
            )
            if decision.action == "SUCCESS":
                gate_out = self.schema_gate.validate_for_success(
                    intent_text=state.intent_text,
                    schema=state.current_schema,
                    schema_fingerprint=self._schema_fingerprint(state.current_schema),
                    database_scope=database_scope,
                    known_information_text=state.known_information_text,
                    last_tool_output=state.last_tool_output,
                    last_write_result=state.last_write_result,
                    steps=steps,
                )
                if gate_out.rejected_before_sufficiency:
                    state.last_write_result = {
                        "status": "FAILED",
                        "summary": gate_out.rejection_reason_parts[0]
                        if gate_out.rejection_reason_parts
                        else "schema insufficient",
                        "sufficiency": gate_out.sufficiency.model_dump(mode="json"),
                    }
                    self.logger.warning(
                        "schemalink success rejected",
                        intent_id=state.intent_id,
                        round_index=state.round_index,
                        validation_errors=gate_out.structural.errors,
                        sufficiency=gate_out.sufficiency.model_dump(mode="json"),
                    )
                    self._checkpoint(state.intent_id, f"schemalink_round_{state.round_index}_success_rejected")
                    continue
                if gate_out.llm_sufficiency_invoked or gate_out.sufficiency_from_cache:
                    summary = gate_out.sufficiency.reason or (
                        "schema sufficient" if gate_out.sufficiency.sufficient else "schema insufficient"
                    )
                    if gate_out.sufficiency_from_cache:
                        summary = f"[cache] {summary}"
                    self._record_step(
                        workflow_state=None,
                        steps=steps,
                        scope="schemalink",
                        owner_id=state.intent_id,
                        agent=self.sufficiency_validator.agent.name,
                        phase="SCHEMALINK",
                        summary=summary,
                        round_index=state.round_index,
                    )
                if not gate_out.ok:
                    state.last_write_result = {
                        "status": "FAILED",
                        "summary": "；".join(gate_out.rejection_reason_parts),
                    }
                    self.logger.warning(
                        "schemalink success rejected",
                        intent_id=state.intent_id,
                        round_index=state.round_index,
                        validation_errors=gate_out.structural.errors,
                        sufficiency=gate_out.sufficiency.model_dump(mode="json"),
                    )
                    self._checkpoint(state.intent_id, f"schemalink_round_{state.round_index}_success_rejected")
                    continue
                self.logger.info(
                    "schemalink success",
                    intent_id=state.intent_id,
                    round_index=state.round_index,
                    schema_summary=self._schema_summary(state.current_schema),
                    category=LogCategory.SUCCESS,
                )
                self._checkpoint(state.intent_id, f"schemalink_round_{state.round_index}_completed")
                return ModuleResult(status=ModuleStatus.SUCCESS, payload=state.current_schema, mark=decision.description)
            if decision.action == "ASK_USER":
                ticket = AskTicket(
                    ticket_id=f"ask_{state.intent_id}_{state.round_index}",
                    scope="schemalink",
                    owner_id=state.intent_id,
                    question_id=f"schemalink_round_{state.round_index}",
                    question=decision.ask_request.question,
                    why_needed=decision.ask_request.why_needed,
                    acceptance_criteria=decision.ask_request.acceptance_criteria,
                    resume_point={"intent_id": state.intent_id, "phase": "SCHEMALINK", "checkpoint": f"round_{state.round_index}"},
                )
                state.pending_question_ticket_id = ticket.ticket_id
                self.logger.warning(
                    "schemalink ask user",
                    intent_id=state.intent_id,
                    round_index=state.round_index,
                    ticket_id=ticket.ticket_id,
                    question=ticket.question,
                    why_needed=ticket.why_needed,
                )
                self._checkpoint(state.intent_id, f"schemalink_round_{state.round_index}_wait_user")
                return ModuleResult(status=ModuleStatus.WAIT_USER, ask_ticket=ticket)
            if decision.action == "CALL_TOOL":
                self.logger.info(
                    "schemalink call tool",
                    intent_id=state.intent_id,
                    round_index=state.round_index,
                    tool_task=decision.tool_task.model_dump(mode="json"),
                )
                tool_run = self.runner.run(
                    self.tool_agent,
                    {
                        "tool_task": decision.tool_task.model_dump(mode="json"),
                        "current_schema": state.current_schema.model_dump(mode="json"),
                        "known_information_text": state.known_information_text,
                        "database_scope": database_scope,
                    },
                    steps=steps,
                )
                if not tool_run.ok or tool_run.output is None:
                    self.logger.error(
                        "schemalink tool agent failed",
                        intent_id=state.intent_id,
                        round_index=state.round_index,
                        error=tool_run.error or "SchemaToolAgent failed",
                    )
                    return ModuleResult(
                        status=ModuleStatus.FATAL_ERROR,
                        error=ModuleError(
                            status="FATAL_ERROR",
                            owner_stage=StageName.SCHEMALINK,
                            current_stage=StageName.SCHEMALINK,
                            error_code="SCHEMATOOL_AGENT_FAILED",
                            message=tool_run.error or "SchemaToolAgent failed",
                            hint=tool_run.error or "schema tool agent failed",
                            repair_action=RepairAction.STOP,
                        ),
                    )
                tool_output = tool_run.output
                state.last_tool_output = SchemaToolOutput.model_validate(tool_output).model_dump(mode="json")
                state.last_tool_trace = list(tool_run.tool_trace or [])
                state.last_write_result = {}
                self._record_step(
                    workflow_state=None,
                    steps=steps,
                    scope="schemalink",
                    owner_id=state.intent_id,
                    agent=self.tool_agent.name,
                    phase="SCHEMALINK",
                    summary=self._tool_summary(state.last_tool_output) or "tool output",
                    round_index=state.round_index,
                )
                self.logger.info(
                    "schemalink tool output",
                    intent_id=state.intent_id,
                    round_index=state.round_index,
                    summary=self._tool_summary(state.last_tool_output) or "",
                )
                self._checkpoint(state.intent_id, f"schemalink_round_{state.round_index}")
                continue
            if decision.action == "WRITE_SCHEMA":
                self.logger.info(
                    "schemalink write schema",
                    intent_id=state.intent_id,
                    round_index=state.round_index,
                    evidence_summary=self._tool_summary(state.last_tool_output) or "",
                )
                raw_confirm = (state.last_tool_output or {}).get("confirm") if isinstance(state.last_tool_output, dict) else None
                raw_has_any = False
                if isinstance(raw_confirm, dict):
                    for k in ["tables", "columns", "more_info", "join_paths"]:
                        v = raw_confirm.get(k) or []
                        if isinstance(v, list) and len(v) > 0:
                            raw_has_any = True
                            break
                # Deterministic diff: only write what is not already present in current_schema.
                last_tool_output = self._diff_tool_confirm(state.current_schema, state.last_tool_output)
                write_plan: list[dict] = []
                write_plan, invalid_joins = self._write_plan_from_confirm(last_tool_output, state.last_tool_trace)
                write_invalid_targets: list[str] = list(invalid_joins or [])
                if write_invalid_targets:
                    self.logger.warning(
                        "schemalink write dropping invalid joins",
                        intent_id=state.intent_id,
                        round_index=state.round_index,
                        invalid_targets=write_invalid_targets,
                    )
                if not write_plan:
                    if raw_has_any:
                        # Everything was already present after diff; treat as NOOP.
                        status = "NOOP"
                        summary = "WRITE_SCHEMA 无需写入，目标已存在或描述已覆盖"
                        if write_invalid_targets:
                            status = "PARTIAL"
                            summary = "WRITE_SCHEMA 无有效写入目标；已忽略无效 join"
                        state.last_write_result = {
                            "status": status,
                            "summary": summary,
                            "write_plan": [],
                        }
                        if write_invalid_targets:
                            state.last_write_result["invalid_targets"] = write_invalid_targets
                        state.last_tool_output = {}
                        state.last_tool_trace = []
                        self.logger.info(
                            "schemalink write noop (empty after diff)",
                            intent_id=state.intent_id,
                            round_index=state.round_index,
                        )
                        self._checkpoint(state.intent_id, f"schemalink_round_{state.round_index}_write_noop_empty")
                        continue
                    state.last_write_result = {
                        "status": "FAILED",
                        "summary": "WRITE_SCHEMA 缺少明确的写入目标（confirm 为空或无法解析）",
                        "write_no_effect": True,
                        "write_plan": [],
                    }
                    self.logger.warning(
                        "schemalink write skipped due to missing findings",
                        intent_id=state.intent_id,
                        round_index=state.round_index,
                    )
                    self._checkpoint(state.intent_id, f"schemalink_round_{state.round_index}_write_skipped")
                    continue
                before_fp = self._schema_fingerprint(state.current_schema)
                delta = self.write_planner.plan(
                    intent=state.intent_text,
                    write_plan=write_plan,
                    current_schema=state.current_schema,
                    tool_output=last_tool_output,
                    database_scope=database_scope,
                )
                self.logger.info(
                    "schemalink delta built",
                    intent_id=state.intent_id,
                    round_index=state.round_index,
                    delta_summary=delta.summary,
                    write_count=len(delta.writes),
                    writes=[item.model_dump(mode="json") for item in delta.writes],
                )
                state.current_schema = self.applier.apply(state.current_schema, delta)
                after_fp = self._schema_fingerprint(state.current_schema)
                if delta.invalid_targets:
                    for target in delta.invalid_targets:
                        if target not in write_invalid_targets:
                            write_invalid_targets.append(target)
                    status = "PARTIAL" if delta.writes else "FAILED"
                    state.last_write_result = {
                        "status": status,
                        "summary": "写入目标非法或依赖不存在",
                        "invalid_targets": write_invalid_targets,
                    }
                    self.logger.warning(
                        "schemalink write invalid targets",
                        intent_id=state.intent_id,
                        round_index=state.round_index,
                        invalid_targets=delta.invalid_targets,
                    )
                    self._record_step(
                        workflow_state=None,
                        steps=steps,
                        scope="schemalink",
                        owner_id=state.intent_id,
                        agent="schema_write_planner",
                        phase="SCHEMALINK",
                        summary=f"invalid write targets: {', '.join(delta.invalid_targets)}",
                        round_index=state.round_index,
                    )
                    self._checkpoint(state.intent_id, f"schemalink_round_{state.round_index}_write_invalid")
                    if status == "FAILED":
                        continue
                if before_fp == after_fp:
                    if self._write_targets_satisfied(state.current_schema, write_plan):
                        state.invalid_write_count = 0
                        sufficiency = self.sufficiency_validator.validate(
                            state.intent_text,
                            state.current_schema,
                            known_information_text=state.known_information_text,
                            last_tool_output=state.last_tool_output,
                            last_write_result=state.last_write_result,
                            steps=steps,
                        )
                        self._record_step(
                            workflow_state=None,
                            steps=steps,
                            scope="schemalink",
                            owner_id=state.intent_id,
                            agent=self.sufficiency_validator.agent.name,
                            phase="SCHEMALINK",
                            summary=sufficiency.reason
                            or ("schema sufficient" if sufficiency.sufficient else "schema insufficient"),
                            round_index=state.round_index,
                        )
                        state.last_write_result = {
                            "status": "PARTIAL" if write_invalid_targets else "NOOP",
                            "summary": "WRITE_SCHEMA 无需写入，目标已存在或描述已覆盖",
                            "write_plan": write_plan,
                            "sufficiency": sufficiency.model_dump(mode="json"),
                        }
                        if write_invalid_targets:
                            state.last_write_result["invalid_targets"] = write_invalid_targets
                        self.logger.info(
                            "schemalink write noop",
                            intent_id=state.intent_id,
                            round_index=state.round_index,
                        )
                        self._checkpoint(state.intent_id, f"schemalink_round_{state.round_index}_write_noop")
                        continue
                    state.invalid_write_count += 1
                    state.last_write_result = {
                        "status": "FAILED",
                        "summary": "WRITE_SCHEMA 无效，schema 未发生变化",
                        "write_no_effect": True,
                        "write_plan": write_plan,
                    }
                    self.logger.warning(
                        "schemalink write no effect",
                        intent_id=state.intent_id,
                        round_index=state.round_index,
                        invalid_write_count=state.invalid_write_count,
                    )
                    self._checkpoint(state.intent_id, f"schemalink_round_{state.round_index}_write_no_effect")
                    if state.invalid_write_count >= 3:
                        ticket = AskTicket(
                            ticket_id=f"ask_{state.intent_id}_{state.round_index}",
                            scope="schemalink",
                            owner_id=state.intent_id,
                            question_id=f"schemalink_write_no_effect_{state.round_index}",
                            question="无法确认缺失字段的真实列名或位置，请补充该字段可能所在的表或实际列名。",
                            why_needed=state.last_write_result.get("summary") or "schema write no effect",
                            acceptance_criteria=["提供字段真实列名", "或提供字段所在表名"],
                            resume_point={
                                "intent_id": state.intent_id,
                                "phase": "SCHEMALINK",
                                "checkpoint": f"round_{state.round_index}",
                            },
                        )
                        state.pending_question_ticket_id = ticket.ticket_id
                        return ModuleResult(status=ModuleStatus.WAIT_USER, ask_ticket=ticket)
                    continue
                state.invalid_write_count = 0
                state.last_write_fingerprint = after_fp
                validation = self.schema_validator.validate_schema(state.current_schema, database_scope)
                if not validation.valid:
                    self.logger.error(
                        "schemalink schema invalid",
                        intent_id=state.intent_id,
                        round_index=state.round_index,
                        errors=validation.errors,
                    )
                    return ModuleResult(
                        status=ModuleStatus.FATAL_ERROR,
                        error=ModuleError(
                            status="FATAL_ERROR",
                            owner_stage=StageName.SCHEMALINK,
                            current_stage=StageName.SCHEMALINK,
                            error_code="SCHEMA_INVALID",
                            message="; ".join(validation.errors),
                            hint="schema write produced invalid schema",
                            repair_action=RepairAction.STOP,
                        ),
                    )
                sufficiency = self.sufficiency_validator.validate(
                    state.intent_text,
                    state.current_schema,
                    known_information_text=state.known_information_text,
                    last_tool_output=state.last_tool_output,
                    last_write_result=state.last_write_result,
                    steps=steps,
                )
                deterministic = deterministic_sufficiency(state.intent_text, state.current_schema)
                if deterministic is not None and not deterministic.sufficient:
                    sufficiency = deterministic
                self._record_step(
                    workflow_state=None,
                    steps=steps,
                    scope="schemalink",
                    owner_id=state.intent_id,
                    agent=self.sufficiency_validator.agent.name,
                    phase="SCHEMALINK",
                    summary=sufficiency.reason or ("schema sufficient" if sufficiency.sufficient else "schema insufficient"),
                    round_index=state.round_index,
                )
                state.last_write_result = {
                    "status": "PARTIAL" if write_invalid_targets else "SUCCESS",
                    "summary": delta.summary,
                    "sufficiency": sufficiency.model_dump(mode="json"),
                }
                if write_invalid_targets:
                    state.last_write_result["invalid_targets"] = write_invalid_targets
                # Consume tool evidence to avoid repeated writes on the same confirm.
                state.last_tool_output = {}
                state.last_tool_trace = []
                self.logger.info(
                    "schemalink schema updated",
                    intent_id=state.intent_id,
                    round_index=state.round_index,
                    schema_summary=self._schema_summary(state.current_schema),
                )
                self._checkpoint(state.intent_id, f"schemalink_round_{state.round_index}")
                continue
            self.logger.warning(
                "schemalink unknown action",
                intent_id=state.intent_id,
                round_index=state.round_index,
                action=getattr(decision, "action", ""),
            )
        self.logger.error(
            "schemalink max rounds reached",
            intent_id=state.intent_id,
            round_index=state.round_index,
            schema_summary=self._schema_summary(state.current_schema),
        )
        return ModuleResult(
            status=ModuleStatus.FATAL_ERROR,
            error=ModuleError(
                status="FATAL_ERROR",
                owner_stage=StageName.SCHEMALINK,
                current_stage=StageName.SCHEMALINK,
                error_code="SCHEMALINK_MAX_ROUNDS",
                message="schemalink exceeded max rounds",
                hint="schemalink exceeded max rounds",
                repair_action=RepairAction.STOP,
            ),
        )

    def _ensure_databases(self, state: SchemaLinkState, database_scope: list[str], steps: list[AgentStep] | None) -> None:
        created = []
        for db in database_scope or []:
            if not db:
                continue
            if db in (state.current_schema.databases or {}):
                continue
            if not self.init_resolver.database_exists(db):
                continue
            state.current_schema = self.applier.apply(
                state.current_schema,
                SchemaDelta(writes=[SchemaWrite(type="db_create", database=db)], summary="bootstrap db"),
            )
            created.append(db)
        if created:
            self._record_step(
                workflow_state=None,
                steps=steps,
                scope="schemalink",
                owner_id=state.intent_id,
                agent="schema_bootstrap",
                phase="SCHEMALINK",
                summary=f"bootstrap databases: {', '.join(created)}",
                round_index=state.round_index,
            )

    def _checkpoint(self, owner_id: str, label: str) -> None:
        if self.checkpoint_cb is not None:
            self.checkpoint_cb(scope="schemalink", owner_id=owner_id, label=label)

    def _tool_summary(self, tool_output: dict) -> str:
        confirm = (tool_output or {}).get("confirm") or {}
        if not isinstance(confirm, dict):
            return ""
        tables = [str(x).strip() for x in (confirm.get("tables") or []) if str(x).strip()]
        columns = [str(x).strip() for x in (confirm.get("columns") or []) if str(x).strip()]
        more = confirm.get("more_info") or []
        joins = confirm.get("join_paths") or []
        sug = (tool_output or {}).get("suggestion") or []

        def _take(items: list[str], n: int = 6) -> str:
            return ", ".join(items[:n]) + (" ..." if len(items) > n else "")

        parts: list[str] = []
        if tables:
            parts.append(f"tables: {_take(tables)}")
        if columns:
            parts.append(f"columns: {_take(columns)}")
        if joins:
            join_texts = []
            for j in joins[:4]:
                if isinstance(j, dict):
                    left = str(j.get("left") or "").strip()
                    right = str(j.get("right") or "").strip()
                    if left and right:
                        join_texts.append(f"{left}={right}")
            if join_texts:
                parts.append(f"join_paths: {_take(join_texts, n=4)}")
        if more:
            cols = []
            for item in more[:4]:
                if isinstance(item, dict):
                    col = str(item.get("column") or "").strip()
                    if col:
                        cols.append(col)
            if cols:
                parts.append(f"more_info: {_take(cols, n=4)}")
        if sug and isinstance(sug, list):
            first = str(sug[0] or "").strip() if sug else ""
            if first:
                parts.append(f"suggestion: {first}")
        return " | ".join(parts) if parts else "confirm: empty"

    def _write_plan_from_confirm(self, tool_output: dict, tool_trace: list[dict]) -> tuple[list[dict], list[str]]:
        confirm = (tool_output or {}).get("confirm")
        if not isinstance(confirm, dict):
            return [], []
        verified: dict[frozenset[str], dict] = {}
        for item in tool_trace or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("tool") or "") != "relation_validator":
                continue
            args = item.get("arguments") or {}
            res = item.get("result") or {}
            if not isinstance(args, dict) or not isinstance(res, dict):
                continue
            if not res.get("is_joinable"):
                continue
            left = str(args.get("left_column") or "").strip()
            right = str(args.get("right_column") or "").strip()
            if left and right:
                verified[frozenset([left, right])] = res

        write_plan: list[dict] = []
        invalid_joins: list[str] = []
        for t in confirm.get("tables") or []:
            if isinstance(t, str) and t.strip():
                write_plan.append({"type": "table_create", "target": t.strip()})
        for c in confirm.get("columns") or []:
            if isinstance(c, str) and c.strip():
                write_plan.append({"type": "column_create", "target": c.strip()})
        for item in confirm.get("more_info") or []:
            if not isinstance(item, dict):
                continue
            col = str(item.get("column") or "").strip()
            desc = str(item.get("description") or "").strip()
            if col and desc:
                write_plan.append({"type": "column_description_merge", "target": col, "description": desc})
        for j in confirm.get("join_paths") or []:
            if not isinstance(j, dict):
                continue
            left = str(j.get("left") or "").strip()
            right = str(j.get("right") or "").strip()
            if not left or not right:
                continue
            if left == right:
                invalid_joins.append(f"semantic_rejected:{left}={right}:self_join_same_column")
                continue
            res = verified.get(frozenset([left, right]))
            if not res:
                invalid_joins.append(f"unverified_join:{left}={right}")
                continue
            semantic = self.join_semantic_guard.validate(left, right, res)
            if not semantic.accepted:
                invalid_joins.append(f"semantic_rejected:{left}={right}:{semantic.reason}")
                continue
            join_type_hint = str(res.get("join_type_hint") or "").strip()
            left_null = res.get("left_null_rate")
            right_null = res.get("right_null_rate")
            null_rate = None
            try:
                vals = [v for v in [left_null, right_null] if v is not None]
                if vals:
                    null_rate = float(max(vals))
            except Exception:
                null_rate = None
            write_plan.append(
                {
                    "type": "join_path_create",
                    "left": left,
                    "right": right,
                    "cardinality": join_type_hint or str(j.get("cardinate") or "").strip(),
                    "null_rate": null_rate,
                }
            )
        return write_plan, invalid_joins

    def _diff_tool_confirm(self, schema: Schema, tool_output: dict) -> dict:
        """Filter toolagent confirm by removing objects already present in current_schema."""
        if not isinstance(tool_output, dict):
            return tool_output
        confirm = tool_output.get("confirm")
        if not isinstance(confirm, dict):
            return tool_output

        existing_tables: set[str] = set()
        existing_columns: set[str] = set()
        for db_name, db_obj in (schema.databases or {}).items():
            for table_name, table_obj in (db_obj.tables or {}).items():
                existing_tables.add(f"{db_name}.{table_name}")
                for col_name in (table_obj.columns or {}).keys():
                    existing_columns.add(f"{db_name}.{table_name}.{col_name}")
        existing_joins: set[frozenset[str]] = set()
        for jp in schema.join_paths or []:
            existing_joins.add(frozenset([str(jp.left or ""), str(jp.right or "")]))

        new_confirm = dict(confirm)
        tables = [str(x).strip() for x in (confirm.get("tables") or []) if str(x).strip()]
        columns = [str(x).strip() for x in (confirm.get("columns") or []) if str(x).strip()]
        more_info = confirm.get("more_info") or []
        joins = confirm.get("join_paths") or []

        new_confirm["tables"] = [t for t in tables if t not in existing_tables]
        new_confirm["columns"] = [c for c in columns if c not in existing_columns]

        filtered_more = []
        for item in more_info:
            if not isinstance(item, dict):
                continue
            col = str(item.get("column") or "").strip()
            desc = str(item.get("description") or "").strip()
            if not col or not desc:
                continue
            # Keep only if column exists (or is being created) AND description actually adds content.
            if col not in existing_columns and col not in new_confirm["columns"]:
                continue
            if col in existing_columns:
                parsed = parse_column_ref(col)
                cur_desc = ""
                if parsed:
                    db, table, column = parsed
                    db_obj = (schema.databases or {}).get(db)
                    table_obj = (db_obj.tables or {}).get(table) if db_obj else None
                    col_obj = (table_obj.columns or {}).get(column) if table_obj else None
                    if col_obj is not None:
                        cur_desc = str(getattr(col_obj, "description", "") or "")
                if desc and cur_desc and desc in cur_desc:
                    continue
            filtered_more.append({"column": col, "description": desc})
        new_confirm["more_info"] = filtered_more

        filtered_joins = []
        for j in joins:
            if not isinstance(j, dict):
                continue
            left = str(j.get("left") or "").strip()
            right = str(j.get("right") or "").strip()
            if not left or not right:
                continue
            if left == right:
                continue
            if frozenset([left, right]) in existing_joins:
                continue
            filtered_joins.append(j)
        new_confirm["join_paths"] = filtered_joins

        out = dict(tool_output)
        out["confirm"] = new_confirm
        return out

    def _schema_fingerprint(self, schema: Schema) -> str:
        parts = []
        for db_name, db_obj in (schema.databases or {}).items():
            db_desc = str(getattr(db_obj, "description", "") or "")
            if db_desc:
                parts.append(f"{db_name}:desc={db_desc}")
            for table_name, table_obj in (db_obj.tables or {}).items():
                cols = sorted((table_obj.columns or {}).keys())
                col_desc = []
                for col_name in cols:
                    spec = (table_obj.columns or {}).get(col_name)
                    desc = ""
                    if spec and getattr(spec, "description", ""):
                        desc = str(spec.description)
                    col_desc.append(f"{col_name}={desc}")
                table_desc = str(getattr(table_obj, "description", "") or "")
                if table_desc:
                    parts.append(f"{db_name}.{table_name}:desc={table_desc}")
                parts.append(f"{db_name}.{table_name}:" + ",".join(col_desc))
        joins = []
        for path in schema.join_paths or []:
            joins.append(f"{path.left}->{path.right}")
        return "|".join(sorted(parts) + sorted(joins))

    def _schema_summary(self, schema: Schema) -> dict:
        databases = schema.databases or {}
        table_count = 0
        column_count = 0
        tables = []
        for db_name, db_obj in databases.items():
            for table_name, table_obj in (db_obj.tables or {}).items():
                table_count += 1
                cols = list((table_obj.columns or {}).keys())
                column_count += len(cols)
                tables.append({"database": db_name, "table": table_name, "columns": cols[:10]})
        return {
            "database_count": len(databases),
            "table_count": table_count,
            "column_count": column_count,
            "join_path_count": len(schema.join_paths or []),
            "tables_preview": tables[:10],
        }

    def _write_targets_satisfied(self, schema: Schema, write_plan: list[dict]) -> bool:
        if not write_plan:
            return False

        def has_database(db: str) -> bool:
            return db in (schema.databases or {})

        def has_table(db: str, table: str) -> bool:
            db_obj = (schema.databases or {}).get(db)
            return bool(db_obj and table in (db_obj.tables or {}))

        def has_column(db: str, table: str, column: str) -> bool:
            db_obj = (schema.databases or {}).get(db)
            table_obj = (db_obj.tables or {}).get(table) if db_obj else None
            return bool(table_obj and column in (table_obj.columns or {}))

        def has_join(left: str, right: str) -> bool:
            for path in schema.join_paths or []:
                if {path.left, path.right} == {left, right}:
                    return True
            return False

        for item in write_plan:
            item_type = str(item.get("type") or "").strip()
            if item_type == "db_create":
                target = str(item.get("target") or "").strip()
                if not target or not has_database(target):
                    return False
                continue
            if item_type == "table_create":
                target = str(item.get("target") or "").strip()
                pt = parse_table_ref(target)
                if not pt or not has_table(pt[0], pt[1]):
                    return False
                continue
            if item_type == "column_create":
                target = str(item.get("target") or "").strip()
                col = parse_column_ref(target)
                if not col or not has_column(col[0], col[1], col[2]):
                    return False
                continue
            if item_type == "column_description_merge":
                target = str(item.get("target") or "").strip()
                col = parse_column_ref(target)
                if not col or not has_column(col[0], col[1], col[2]):
                    return False
                continue
            if item_type == "join_path_create":
                left = str(item.get("left") or "").strip()
                right = str(item.get("right") or "").strip()
                if not left or not right or not has_join(left, right):
                    return False
        return True

    def _record_step(
        self,
        workflow_state=None,
        steps: list[AgentStep] | None = None,
        *,
        scope: str,
        owner_id: str,
        agent: str,
        phase: str,
        summary: str,
        round_index: int | None = None,
    ) -> None:
        if steps is None:
            return
        if workflow_state is not None:
            WorkflowStepLimiter.from_settings().ensure_can_append(len(steps))
        step = AgentStep(
            step_id=f"{scope}_{owner_id}_{len(steps)+1}",
            scope=scope,
            owner_id=owner_id,
            agent=agent,
            phase=phase,
            summary=summary,
            round_index=round_index,
            created_at=time.time(),
        )
        steps.append(step)
