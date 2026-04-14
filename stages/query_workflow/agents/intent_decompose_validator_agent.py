from __future__ import annotations

from pydantic import BaseModel, Field

from .base_agent import BaseAgent
from .prompt_builder import build_json_system_prompt, build_json_user_prompt
from ..contracts import AgentStep, IntentDecomposeValidationResult


SYSTEM_RULES = [
    "你的职责是评估意图拆分是否合理，不负责生成拆分。",
    "你必须基于原始用户问题与拆分结果做对比判断。",
    "每个拆分元素都必须包含 query 与 schema，且两者语义一一对应。",
    "schema 必须是方法/能力导向的构建目标，不应复述具体值条件。",
    "如果拆分导致某个元素不是完整独立查询，必须判定 FAILED。",
    "如果拆分后的多个元素彼此强耦合、不能独立求解，必须判定 FAILED。",
    "如果拆分后的元素都是完整独立查询，可判定 SUCCESS。",
    "除非能指出具体缺失条件或逻辑断裂点，否则不要判定 FAILED。",
    "只返回严格 JSON，不要输出 markdown，不要输出代码块，不要输出解释性自然语言。",
]

USER_REQUIREMENTS = [
    "输出必须包含 status 和 rationale。",
    "若 FAILED，issues 必须给出具体缺失条件或逻辑断裂点，suggested_fix 给出可执行修正建议。",
    "重点检查 query/schema 是否一一对应且语义一致。",
]


class _Payload(BaseModel):
    query: str
    intents: list[dict] = Field(default_factory=list)


class IntentDecomposeValidatorAgent(BaseAgent[_Payload, IntentDecomposeValidationResult]):
    name = "intent_decompose_validator"
    description = "意图拆分合理性评估"
    output_model = IntentDecomposeValidationResult

    def build_system_prompt(self) -> str:
        return build_json_system_prompt(
            role_line="你是 AskDB 系统中的意图拆分评估代理。",
            mission_line="你的唯一职责是：判断意图拆分是否合理，确保不会破坏原始查询链。",
            rules=SYSTEM_RULES,
        )

    def build_user_prompt(self, payload: _Payload | dict, steps: list[AgentStep] | None) -> str:
        p = _Payload.model_validate(payload)
        return build_json_user_prompt(
            steps_block=self.format_steps_block(steps),
            task_input_json=f'{{"query": {p.query!r}, "intents": {p.intents}}}',
            requirement_prefix=["输出格式：", "约束："],
            requirements=USER_REQUIREMENTS,
            output_schema=self.format_output_schema(),
        )
