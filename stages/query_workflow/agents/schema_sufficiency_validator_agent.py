from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from .base_agent import BaseAgent
from .prompt_builder import build_json_system_prompt, build_json_user_prompt
from ..contracts import AgentStep


class SchemaSufficiencyResult(BaseModel):
    sufficient: bool
    gap_category: Literal[
        "ok",
        "missing_connection",
        "missing_fields",
        "coverage_insufficient",
        "unknown",
    ] = "unknown"
    reason: str = ""


SYSTEM_RULES = [
    "你的职责是判断当前 schema 是否已经满足意图所需的最小完备性。",
    "你只评估 schema 结构覆盖，不评估数据值是否存在。",
    "你必须基于意图与 current_schema 判断，不要假设不存在的表或列。",
    "你必须只围绕当前 schema intent（payload.intent）评估，不要把业务查询里的具体值条件当作 schema 缺口。",
    "如果 schema 结构不足以支持后续 RA/SQL 规划，应返回 sufficient=false，并说明缺口。",
    "只输出缺口类别（gap_category），可选输出简短 reason（语义化即可）。",
    "不要包含任何真实物理对象引用（不要出现 db.table 或 db.table.column）。",
    "不要基于样本值/数据分布/某个取值是否存在来判断 schema 是否完备；这些属于数据结果问题，不属于 schema 缺口。",
    "当相关列已存在时，即使目标值当前不存在，也不能判定 missing_fields。",
    "不要输出 SQL、不要输出表外猜测。",
    "只返回严格 JSON，不要输出 markdown，不要输出代码块，不要输出解释性自然语言。",
]

USER_REQUIREMENTS = [
    "gap_category 只能取值：ok / missing_connection / missing_fields / coverage_insufficient / unknown。",
    "若 sufficient=true，则 gap_category 必须为 ok。",
    "若 sufficient=false，则 gap_category 不能为 ok。",
    "reason 可为空；若填写必须语义化，不得包含任何 db.table.column。",
    "reason 禁止写“某个具体值不存在/仅包含某些值”这类数据事实；应只描述结构缺口。",
    "示例：若状态列已存在但值“maintenance”不存在，仍应判 sufficient=true, gap_category=ok。",
]


class _Payload(BaseModel):
    intent: str
    known_information_text: str = ""
    current_schema: dict = {}
    last_tool_output: dict = {}
    last_write_result: dict = {}


class SchemaSufficiencyValidatorAgent(BaseAgent[_Payload, SchemaSufficiencyResult]):
    name = "schema_sufficiency_validator"
    description = "schema 最小完备性评估"
    output_model = SchemaSufficiencyResult

    def build_system_prompt(self) -> str:
        return build_json_system_prompt(
            role_line="你是 AskDB 系统中的 schema 完备性评估代理。",
            mission_line="你的唯一职责是：判断当前 schema 是否足够支撑后续规划，并指出缺口。",
            rules=SYSTEM_RULES,
        )

    def build_user_prompt(self, payload: _Payload | dict, steps: list[AgentStep] | None) -> str:
        p = _Payload.model_validate(payload)
        return build_json_user_prompt(
            steps_block=self.format_steps_block(steps),
            task_input_json=(
                f'{{"intent": {p.intent!r}, "known_information_text": {p.known_information_text!r}, '
                f'"current_schema": {p.current_schema}, "last_tool_output": {p.last_tool_output}, "last_write_result": {p.last_write_result}}}'
            ),
            requirement_prefix=["输出格式：", "约束："],
            requirements=USER_REQUIREMENTS,
            output_schema=self.format_output_schema(),
        )

    def post_validate(self, _payload: _Payload, output: SchemaSufficiencyResult) -> None:
        # No regex hard-fail here: the validator should never stall the pipeline.
        if output.sufficient and output.gap_category != "ok":
            output.gap_category = "ok"
        if (not output.sufficient) and output.gap_category == "ok":
            output.gap_category = "unknown"
