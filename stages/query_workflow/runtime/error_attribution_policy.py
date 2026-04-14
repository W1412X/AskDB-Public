from __future__ import annotations

from ..contracts import ErrorAttributionOutput, ModuleError
from ..enums import RepairAction, StageName


class ErrorRouterSafetyNet:
    """
    Minimal pre-LLM checks only (no semantic routing). Everything else goes to the attribution agent.
    """

    def try_route(
        self,
        *,
        current_stage: StageName,
        error_message: str,
    ) -> ModuleError | None:
        text = str(error_message or "").strip()
        if not text:
            return ModuleError(
                status="FATAL_ERROR",
                owner_stage=current_stage,
                current_stage=current_stage,
                error_code="EMPTY_ERROR_MESSAGE",
                message="empty error message",
                hint="empty error message",
                repair_action=RepairAction.STOP,
                evidence={"router": "safety_net", "reason": "empty_message"},
            )
        return None


class DefaultRepairPolicy:
    """
    Deterministic fallback when LLM fails or output cannot be applied in the intent repair loop.
    """

    def resolve(self, current_stage: StageName, error_message: str) -> ModuleError:
        upper = str(error_message or "").upper()
        if current_stage == StageName.SCHEMALINK and any(
            token in upper for token in ["INSUFFICIENT", "NOT ENOUGH", "MISSING EVIDENCE"]
        ):
            owner_stage = StageName.SCHEMALINK
            action = RepairAction.ENRICH_SCHEMA
            error_type = "UPSTREAM"
        elif current_stage == StageName.SCHEMALINK:
            owner_stage = StageName.SCHEMALINK
            action = RepairAction.REBUILD_SCHEMA
            error_type = "LOCAL"
        elif current_stage == StageName.PLAN_RA:
            owner_stage = StageName.PLAN_RA
            action = RepairAction.REPLAN_RA
            error_type = "LOCAL"
        elif current_stage == StageName.VALIDATE_SQL and any(
            token in upper for token in ["UNKNOWN COLUMN", "UNKNOWN TABLE", "MISSING COLUMN", "MISSING TABLE"]
        ):
            owner_stage = StageName.PLAN_RA
            action = RepairAction.REPLAN_RA
            error_type = "UPSTREAM"
        elif current_stage == StageName.EXECUTE_SQL and any(
            token in upper for token in ["UNKNOWN COLUMN", "UNKNOWN TABLE", "SYNTAX", "AMBIGUOUS"]
        ):
            owner_stage = StageName.RENDER_SQL
            action = RepairAction.RERENDER_SQL
            error_type = "LOCAL"
        elif current_stage == StageName.VALIDATE_SQL:
            owner_stage = StageName.RENDER_SQL
            action = RepairAction.RERENDER_SQL
            error_type = "LOCAL"
        elif current_stage == StageName.INTERPRET_RESULT:
            owner_stage = StageName.INTERPRET_RESULT
            action = RepairAction.REINTERPRET_RESULT
            error_type = "LOCAL"
        elif current_stage == StageName.EXECUTE_SQL:
            owner_stage = StageName.EXECUTE_SQL
            action = RepairAction.REEXECUTE_SQL
            error_type = "ENVIRONMENT"
        else:
            owner_stage = current_stage
            action = RepairAction.STOP
            error_type = "ENVIRONMENT"
        return ModuleError(
            status="FATAL_ERROR" if error_type == "ENVIRONMENT" or action == RepairAction.STOP else "RETRYABLE_ERROR",
            owner_stage=owner_stage,
            current_stage=current_stage,
            error_code="UNROUTED_ERROR",
            message=error_message,
            hint=error_message,
            repair_action=action,
            evidence={"error_type": error_type, "router": "default_policy"},
        )


class ErrorAttributionValidator:
    """
    Turns agent output into ModuleError. Repair actions that IntentExecutor cannot step through
    (e.g. ASK_USER) are remapped via DefaultRepairPolicy; unknown actions likewise.
    """

    _APPLICABLE: frozenset[RepairAction] = frozenset(
        {
            RepairAction.REBUILD_SCHEMA,
            RepairAction.ENRICH_SCHEMA,
            RepairAction.REPLAN_RA,
            RepairAction.RERENDER_SQL,
            RepairAction.REVALIDATE_SQL,
            RepairAction.REEXECUTE_SQL,
            RepairAction.REINTERPRET_RESULT,
            RepairAction.RETRY_CURRENT,
        }
    )

    def __init__(self, default_policy: DefaultRepairPolicy | None = None) -> None:
        self._default = default_policy or DefaultRepairPolicy()

    def to_module_error(
        self,
        out: ErrorAttributionOutput,
        *,
        current_stage: StageName,
        error_message: str,
    ) -> ModuleError:
        base_evidence: dict = {
            "router": "llm",
            "error_type": out.error_type,
            "confidence": out.confidence,
        }
        repair = out.repair_action
        msg = out.message or error_message

        if repair == RepairAction.STOP:
            return ModuleError(
                status="FATAL_ERROR",
                owner_stage=out.owner_stage,
                current_stage=current_stage,
                error_code=out.error_code or "ATTRIBUTION_STOP",
                message=msg,
                hint=msg,
                repair_action=RepairAction.STOP,
                evidence=base_evidence,
            )

        if repair == RepairAction.ASK_USER:
            fb = self._default.resolve(current_stage, error_message)
            evidence = {
                **base_evidence,
                "sanitized_from": "ASK_USER",
                "fallback_router": fb.evidence.get("router", ""),
            }
            return ModuleError(
                status=fb.status,
                owner_stage=fb.owner_stage,
                current_stage=current_stage,
                error_code=out.error_code or fb.error_code,
                message=msg,
                hint=msg,
                repair_action=fb.repair_action,
                evidence=evidence,
            )

        if repair not in self._APPLICABLE:
            fb = self._default.resolve(current_stage, error_message)
            evidence = {
                **base_evidence,
                "sanitized_from": repair.value,
                "fallback_router": fb.evidence.get("router", ""),
            }
            return ModuleError(
                status=fb.status,
                owner_stage=fb.owner_stage,
                current_stage=current_stage,
                error_code="ATTRIBUTION_SANITIZED",
                message=msg,
                hint=msg,
                repair_action=fb.repair_action,
                evidence=evidence,
            )

        owner = _align_owner_for_repair(out.owner_stage, repair)
        status = "FATAL_ERROR" if out.error_type == "ENVIRONMENT" else "RETRYABLE_ERROR"
        return ModuleError(
            status=status,
            owner_stage=owner,
            current_stage=current_stage,
            error_code=out.error_code or "ATTRIBUTION_ERROR",
            message=msg,
            hint=msg,
            repair_action=repair,
            evidence=base_evidence,
        )


def _align_owner_for_repair(owner: StageName, repair: RepairAction) -> StageName:
    preferred = _REPAIR_OWNER.get(repair)
    if preferred is not None:
        return preferred
    return owner


_REPAIR_OWNER: dict[RepairAction, StageName] = {
    RepairAction.REBUILD_SCHEMA: StageName.SCHEMALINK,
    RepairAction.ENRICH_SCHEMA: StageName.SCHEMALINK,
    RepairAction.REPLAN_RA: StageName.PLAN_RA,
    RepairAction.RERENDER_SQL: StageName.RENDER_SQL,
    RepairAction.REVALIDATE_SQL: StageName.VALIDATE_SQL,
    RepairAction.REEXECUTE_SQL: StageName.EXECUTE_SQL,
    RepairAction.REINTERPRET_RESULT: StageName.INTERPRET_RESULT,
}
