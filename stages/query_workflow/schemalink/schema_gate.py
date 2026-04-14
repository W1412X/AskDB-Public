from __future__ import annotations

import hashlib
import json
from collections import OrderedDict

from pydantic import BaseModel, Field

from ..agents.schema_sufficiency_validator_agent import SchemaSufficiencyResult
from ..contracts import AgentStep, Schema
from .schema_deterministic_sufficiency import deterministic_sufficiency
from .schema_sufficiency_validator import SchemaSufficiencyValidator
from .schema_validator import SchemaValidationResult, SchemaValidator


class SufficiencyLLMCache:
    """Small LRU of LLM sufficiency results keyed by intent + schema fingerprint + context digests."""

    def __init__(self, max_entries: int = 16) -> None:
        self._max = max(1, int(max_entries))
        self._data: OrderedDict[str, SchemaSufficiencyResult] = OrderedDict()

    @staticmethod
    def make_key(
        *,
        intent_text: str,
        schema_fingerprint: str,
        known_information_text: str,
        last_tool_output: dict | None,
        last_write_result: dict | None,
    ) -> str:
        ctx = json.dumps(
            {"t": last_tool_output or {}, "w": last_write_result or {}},
            sort_keys=True,
            ensure_ascii=False,
        )
        raw = "\n".join(
            [
                intent_text,
                schema_fingerprint,
                hashlib.sha256((known_information_text or "").encode()).hexdigest()[:24],
                hashlib.sha256(ctx.encode()).hexdigest()[:24],
            ]
        )
        return hashlib.sha256(raw.encode()).hexdigest()

    def get(self, key: str) -> SchemaSufficiencyResult | None:
        if key not in self._data:
            return None
        self._data.move_to_end(key)
        return self._data[key]

    def set(self, key: str, value: SchemaSufficiencyResult) -> None:
        self._data[key] = value
        self._data.move_to_end(key)
        while len(self._data) > self._max:
            self._data.popitem(last=False)


class SchemaGateResult(BaseModel):
    """Outcome of structural → deterministic → LLM sufficiency gate (SchemaLink SUCCESS path)."""

    ok: bool
    structural: SchemaValidationResult
    sufficiency: SchemaSufficiencyResult
    rejected_before_sufficiency: bool
    llm_sufficiency_invoked: bool
    sufficiency_from_cache: bool = False
    rejection_reason_parts: list[str] = Field(default_factory=list)


class SchemaGate:
    """
    SUCCESS exit: structural validity, deterministic sufficiency, then LLM sufficiency
    (skipped when structure invalid; LLM result optionally reused from SufficiencyLLMCache).
    """

    def __init__(
        self,
        structural: SchemaValidator,
        sufficiency_validator: SchemaSufficiencyValidator,
        *,
        llm_cache_max_entries: int = 16,
    ) -> None:
        self._structural = structural
        self._sufficiency = sufficiency_validator
        self._llm_cache = SufficiencyLLMCache(max_entries=llm_cache_max_entries)

    def validate_for_success(
        self,
        *,
        intent_text: str,
        schema: Schema,
        schema_fingerprint: str,
        database_scope: list[str],
        known_information_text: str,
        last_tool_output: dict | None,
        last_write_result: dict | None,
        steps: list[AgentStep] | None,
    ) -> SchemaGateResult:
        structural = self._structural.validate_schema(schema, database_scope)
        if not structural.valid:
            return SchemaGateResult(
                ok=False,
                structural=structural,
                sufficiency=SchemaSufficiencyResult(sufficient=False, gap_category="unknown", reason=""),
                rejected_before_sufficiency=True,
                llm_sufficiency_invoked=False,
                rejection_reason_parts=[f"schema invalid: {'; '.join(structural.errors)}"],
            )

        deterministic = deterministic_sufficiency(intent_text, schema)
        if deterministic is not None and not deterministic.sufficient:
            return SchemaGateResult(
                ok=False,
                structural=structural,
                sufficiency=deterministic,
                rejected_before_sufficiency=True,
                llm_sufficiency_invoked=False,
                rejection_reason_parts=[f"schema insufficient: {deterministic.gap_category}"],
            )

        cache_key = SufficiencyLLMCache.make_key(
            intent_text=intent_text,
            schema_fingerprint=schema_fingerprint,
            known_information_text=known_information_text,
            last_tool_output=last_tool_output,
            last_write_result=last_write_result,
        )
        cached = self._llm_cache.get(cache_key)
        if cached is not None:
            parts: list[str] = []
            if not cached.sufficient:
                parts.append(f"schema insufficient: {cached.reason}")
            return SchemaGateResult(
                ok=cached.sufficient,
                structural=structural,
                sufficiency=cached,
                rejected_before_sufficiency=False,
                llm_sufficiency_invoked=False,
                sufficiency_from_cache=True,
                rejection_reason_parts=parts,
            )

        llm_result = self._sufficiency.validate(
            intent_text,
            schema,
            known_information_text=known_information_text,
            last_tool_output=last_tool_output,
            last_write_result=last_write_result,
            steps=steps,
        )
        self._llm_cache.set(cache_key, llm_result)

        parts = []
        if not llm_result.sufficient:
            parts.append(f"schema insufficient: {llm_result.reason}")
        ok = llm_result.sufficient
        return SchemaGateResult(
            ok=ok,
            structural=structural,
            sufficiency=llm_result,
            rejected_before_sufficiency=False,
            llm_sufficiency_invoked=True,
            rejection_reason_parts=parts,
        )
